package block

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	_ "github.com/mattn/go-sqlite3"
)

const (
	backupTypeDifferential = "differential"
	backupTypeFull         = "full"
)

type Backup struct {
	Config         *BackupConfig
	Record         *BackupRecord
	lastFullRecord BackupRecord
	store          *Store
	vol            *Volume
}

func NewBackup(cfg *BackupConfig) (*Backup, error) {
	// Calculate target size in bytes.
	sizeInBytes, err := GetTargetSizeInBytes(cfg.DevicePath)
	if err != nil {
		return nil, err
	}

	if cfg.BlockSize > sizeInBytes {
		fmt.Fprintf(os.Stderr, "WARNING: block size %d exceeds the size of the backup target %d. This will result in wasted space!", cfg.BlockSize, sizeInBytes)
	}

	// Calculate the total number of blocks for the device.
	totalBlocks := calculateTotalBlocks(cfg.BlockSize, sizeInBytes)

	// Find the volume for the device path.
	vol, err := resolveVolume(cfg.Store, cfg.DevicePath)
	if err != nil {
		return nil, err
	}

	// Find the last full backup record.
	lastFullRecord, err := cfg.Store.findLastFullBackupRecord(vol.ID)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	// Determine the backup type.
	backupType, err := determineBackupType(lastFullRecord)
	if err != nil {
		return nil, err
	}

	// Trim the last slash from the output directory.
	if cfg.OutputDirectory != "" {
		cfg.OutputDirectory = strings.TrimRight(cfg.OutputDirectory, "/")
	}

	if cfg.OutputFileName == "" {
		cfg.OutputFileName = generateBackupName(vol, backupType)
	}

	fullPath := fmt.Sprintf("%s/%s", cfg.OutputDirectory, cfg.OutputFileName)

	// TODO - Consider storing a checksum of the target volume, so we can verify at restore time.
	br, err := cfg.Store.insertBackupRecord(vol.ID, cfg.OutputFileName, fullPath, string(cfg.OutputFormat), backupType, totalBlocks, cfg.BlockSize, sizeInBytes)
	if err != nil {
		return nil, err
	}

	return &Backup{
		Record:         &br,
		Config:         cfg,
		vol:            vol,
		store:          cfg.Store,
		lastFullRecord: lastFullRecord,
	}, nil
}

func (b *Backup) TotalBlocks() int {
	return b.Record.TotalBlocks
}

func (b *Backup) BackupType() string {
	return b.Record.BackupType
}

func (b *Backup) FileName() string {
	return b.Config.OutputFileName
}

func (b *Backup) FullPath() string {
	return b.Record.FullPath
}

func (b *Backup) OutputDirectory() string {
	return b.Config.OutputDirectory
}

func (b *Backup) SizeInBytes() int {
	return b.Record.SizeInBytes
}

func (b *Backup) Run() error {
	// Open the device for reading.
	sourceFile, err := os.Open(b.vol.DevicePath)
	if err != nil {
		return err
	}
	defer func() { _ = sourceFile.Close() }()

	// Open the backup file for writing.
	var targetFile *os.File
	switch b.Config.OutputFormat {
	case BackupOutputFormatFile:
		targetFile, err = os.OpenFile(b.FullPath(), os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("error opening restore file: %v", err)
		}
	case BackupOutputFormatSTDOUT:
		targetFile = os.Stdout
	}

	defer func() { _ = targetFile.Close() }()

	// Create a buffer to store the block hashes.
	// The number of hashes we buffer before writing to the database.
	bufSize := b.Config.BlockBufferSize * b.Config.BlockSize

	// The number of individual blocks we can store in the buffer.
	bufCapacity := bufSize / b.Config.BlockSize

	// The current iteration we are on.
	iteration := 0

	// Seek to the beginning of the file.
	_, err = sourceFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	endOfFile := int64(b.SizeInBytes())

	// Create a buffered reader to read the source file.
	reader := bufio.NewReaderSize(sourceFile, bufSize)

	// Read chunks until we have enough to fill the buffer.
	for iteration*bufCapacity < b.TotalBlocks() {
		blockBuf := make([]byte, bufSize)

		offset := int64(iteration * bufCapacity * b.Config.BlockSize)
		endRange := offset + int64(bufSize)

		if endRange > endOfFile {
			endRange = endOfFile
			trimmedBufSize := endRange - offset
			if trimmedBufSize <= 0 {
				break
			}
			blockBuf = make([]byte, trimmedBufSize)
		}

		n, err := reader.Read(blockBuf)
		switch {
		case err == io.EOF || err == io.ErrUnexpectedEOF:
			// If we hit EOF before filling the buffer, that's expected behavior; we just trim the buffer.
			blockBuf = blockBuf[:n]
			if len(blockBuf) == 0 {
				break
			}
		case err != nil:
			return fmt.Errorf("error reading block data: %w", err)
		}

		// If the buffer is not full, we need to trim it.
		if len(blockBuf) < bufSize {
			tmpBuf := make([]byte, len(blockBuf))
			copy(tmpBuf, blockBuf)
			blockBuf = tmpBuf
		}

		// The number of individual blocks in the buffer.
		bufEntries := len(blockBuf) / b.Config.BlockSize

		// Insert the block positions into the database and write the blocks to the backup file.
		hashMap, err := b.writeBlocks(targetFile, iteration, bufEntries, bufCapacity, blockBuf)
		if err != nil {
			return err
		}

		// Insert the block positions into the database.
		if err := b.insertBlockPositionsTransaction(iteration, bufEntries, bufCapacity, hashMap); err != nil {
			return err
		}

		iteration++
	}

	s, err := GetTargetSizeInBytes(b.FullPath())
	if err != nil {
		return fmt.Errorf("error getting backup size: %v", err)
	}

	b.Record.SizeInBytes = s

	return nil
}

func (b *Backup) insertBlockPositionsTransaction(iteration int, bufEntries int, bufCapacity int, hashMap map[int]string) error {
	if len(hashMap) == 0 {
		return nil
	}

	posStartRange := iteration * bufCapacity
	posEndRange := posStartRange + bufCapacity

	placeholders := strings.Trim(strings.Repeat("?,", bufEntries), ",")
	blockQueryStr := "SELECT id, hash FROM blocks WHERE hash IN (" + placeholders + ")"
	blockQueryValues := []interface{}{}

	for i := 0; i < bufEntries; i++ {
		blockQueryValues = append(blockQueryValues, hashMap[posStartRange+i])
	}

	rows, err := b.store.Query(blockQueryStr, blockQueryValues...)
	if err != nil {
		return err
	}

	// Create a map of the block hashes to their IDs.
	blockIDMap := make(map[string]int)

	for rows.Next() {
		var id int
		var hash string
		if err := rows.Scan(&id, &hash); err != nil {
			switch {
			case err == sql.ErrNoRows:
				break
			default:
				return err
			}
		}

		blockIDMap[hash] = id
	}

	dupMap := make(map[int]string, bufEntries)

	// Query the positions range against the last full backup.
	if b.BackupType() == backupTypeDifferential {
		// Query hashes associated with the position range.
		rows, err := b.store.Query("SELECT b.id, bp.position, hash FROM blocks b JOIN block_positions bp ON bp.block_id = b.id WHERE bp.backup_id = ? AND bp.position >= ? AND bp.position < ?", b.lastFullRecord.ID, posStartRange, posEndRange)
		if err != nil {
			return err
		}

		for rows.Next() {
			var id int
			var hash string
			var position int
			if err := rows.Scan(&id, &position, &hash); err != nil {
				switch {
				case err == sql.ErrNoRows:
					break
				default:
					return err
				}
			}
			dupMap[position] = hash
		}
		rows.Close()
	}

	// Prepare for bulk insert.
	baseStmt := "INSERT INTO block_positions (backup_id, block_id, position) VALUES "
	var valueStrings []string
	var valueArgs []interface{}

	for i := 0; i < bufEntries; i++ {
		pos := posStartRange + i

		if b.BackupType() == backupTypeDifferential {
			// Skip if the hash is the same as the last full backup.
			if _, ok := dupMap[pos]; ok && dupMap[pos] == hashMap[pos] {
				continue
			}
		}
		blockID := blockIDMap[hashMap[pos]]
		valueStrings = append(valueStrings, "(?, ?, ?)")
		valueArgs = append(valueArgs, b.Record.ID, blockID, pos)
	}

	// If there are no inserts, we can abort the transaction.
	if len(valueStrings) == 0 {
		return nil
	}

	// Start a transaction to insert the block positions into the database
	tx, err := b.store.Begin()
	if err != nil {
		return err
	}

	// Append the value strings to the base statement.
	stmt := baseStmt + strings.Join(valueStrings, ",")

	// Prepare the query.
	query, err := tx.Prepare(stmt)
	if err != nil {
		handleRollback(tx)
		return err
	}
	defer query.Close()

	// Execute the query.
	_, err = query.Exec(valueArgs...)
	if err != nil {
		handleRollback(tx)
		return err
	}

	return tx.Commit()
}

func (b *Backup) writeBlocks(target *os.File, iteration int, bufEntries int, bufCapacity int, blockBuf []byte) (map[int]string, error) {
	// Calculate the hash for each block in the buffer.
	hashMap := b.hashBufferedData(iteration, bufEntries, bufCapacity, blockBuf)

	reverseMap := make(map[string]int)
	for k, v := range hashMap {
		reverseMap[v] = k
	}

	duplicateHashes, err := b.identifyDuplicateBlocks(reverseMap)
	if err != nil {
		return nil, fmt.Errorf("error identifying duplicate blocks: %v", err)
	}

	querySlice := []string{}
	queryValues := []interface{}{}

	// Use a map to force unique hashes.
	insertablePositions := map[string]int{}

	// Exclude hashes that already exist in the database from the insert.
	for hash, pos := range reverseMap {
		found := false
		for _, dup := range duplicateHashes {
			if hash == dup {
				found = true
				break
			}
		}

		if found {
			continue
		}

		insertablePositions[hash] = pos
		querySlice = append(querySlice, "(?)")
	}

	// If there are no insertable positions, we can return early.
	if len(insertablePositions) == 0 {
		return hashMap, nil
	}

	// Convert the insertable positions to a slice.
	insertableSlice := []int{}
	for _, pos := range insertablePositions {
		insertableSlice = append(insertableSlice, pos)
		queryValues = append(queryValues, hashMap[pos])
	}

	tx, err := b.store.Begin()
	if err != nil {
		return nil, err
	}

	// TODO - There may be a limit to the number of placeholders we can use in a query.
	q := "INSERT INTO blocks (hash) VALUES " + strings.Join(querySlice, ",")
	insertBlockQuery, err := tx.Prepare(q)
	if err != nil {
		handleRollback(tx)
		return nil, err
	}

	_, err = insertBlockQuery.Exec(queryValues...)
	if err != nil {
		return nil, fmt.Errorf("error inserting block hash into database: %v", err)
	}

	if err := tx.Commit(); err != nil {
		handleRollback(tx)
		return nil, err
	}

	sort.Ints(insertableSlice)

	buf := make([]byte, b.Config.BlockSize*len(insertableSlice))
	var idx int

	for _, pos := range insertableSlice {
		startingPos := (pos - (iteration * bufCapacity)) * b.Config.BlockSize
		copy(buf[idx:], blockBuf[startingPos:startingPos+b.Config.BlockSize])
		// Move the index for the next block
		idx += b.Config.BlockSize
	}

	_, err = target.Write(buf)
	if err != nil {
		return nil, fmt.Errorf("error writing block to backup file: %v", err)
	}

	return hashMap, nil
}

func (b *Backup) identifyDuplicateBlocks(reverseMap map[string]int) ([]string, error) {
	qValues := []interface{}{}
	for hash := range reverseMap {
		qValues = append(qValues, hash)
	}

	placeholders := strings.Trim(strings.Repeat("?,", len(qValues)), ",")
	query := "SELECT DISTINCT hash FROM blocks WHERE hash IN (" + placeholders + ")"
	rows, err := b.store.Query(query, qValues...)
	if err != nil {
		return nil, err
	}

	duplicateHashes := []string{}

	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			switch {
			case err == sql.ErrNoRows:
				break
			case err != nil:
				return nil, err
			}
		}
		duplicateHashes = append(duplicateHashes, hash)
	}
	defer rows.Close()
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return duplicateHashes, nil
}

func (b *Backup) hashBufferedData(iteration int, bufEntries int, bufCapacity int, buf []byte) map[int]string {
	var wg sync.WaitGroup
	var mu sync.Mutex

	hashMap := make(map[int]string)

	// Calculate the hash for each block in the buffer.
	for i := 0; i < bufEntries; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			startingPos := b.Config.BlockSize * i
			endingPos := (startingPos + b.Config.BlockSize)

			// Read byte range for the block.
			blockData := buf[startingPos:endingPos]

			// Calculate the hash for the block.
			hash := calculateBlockHash(blockData)

			// Determine the position of the chunk.
			pos := iteration*bufCapacity + i

			mu.Lock()
			hashMap[pos] = hash
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	return hashMap
}

func resolveVolume(store *Store, devicePath string) (*Volume, error) {
	pathSlice := strings.Split(devicePath, "/")
	volName := pathSlice[len(pathSlice)-1]
	vol, err := store.FindVolume(volName)
	switch {
	case err == sql.ErrNoRows:
		// Create a new volume record.
		vol, err = store.InsertVolume(volName, devicePath)
		if err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	}

	return &vol, nil
}

func determineBackupType(lastFull BackupRecord) (string, error) {
	if lastFull == (BackupRecord{}) {
		return backupTypeFull, nil
	}
	return backupTypeDifferential, nil
}

func generateBackupName(vol *Volume, backupType string) string {
	timestamp := time.Now().UnixMilli()
	return fmt.Sprintf("%s_%s_%d", vol.Name, backupType, timestamp)
}

func calculateBlockHash(blockData []byte) string {
	hash := xxhash.Sum64(blockData)
	return fmt.Sprint(hash)
}

func calculateTotalBlocks(blockSize int, sizeInBytes int) int {
	totalBlocks := float64(sizeInBytes) / float64(blockSize)
	return int(math.Ceil(totalBlocks))
}

func handleRollback(tx *sql.Tx) {
	if err := tx.Rollback(); err != nil {
		fmt.Printf("error rolling back transaction: %v", err)
	}
}
