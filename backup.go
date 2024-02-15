package block

import (
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
	"github.com/mattn/go-sqlite3"
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

	// Read chunks until we have enough to fill the buffer.
	for iteration*bufCapacity < b.TotalBlocks() {
		// Create a buffer to store the block hashes.
		blockBuf := make([]byte, 0, bufSize)

		// Read blocks until we have enough to fill the buffer.
		for blockNum := 0; blockNum < bufCapacity; blockNum++ {
			// Determine the position of the chunk.
			blockPos := iteration*bufCapacity + blockNum

			// Read block data from the source file.
			blockData, err := readBlock(sourceFile, b.TotalBlocks(), b.Config.BlockSize, blockPos)
			switch {
			case err == io.EOF:
				continue
			case err != nil:
				return fmt.Errorf("error reading block data at position %d: %v", blockPos, err)
			}

			blockBuf = append(blockBuf, blockData...)
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
	// Start a transaction to insert the block positions into the database
	tx, err := b.store.Begin()
	if err != nil {
		return err
	}

	posStartRange := iteration * bufCapacity
	posEndRange := posStartRange + bufCapacity

	dupMap := make(map[int]string, bufEntries)

	// Query for the block positions associated with the last full backup and
	// store the hash in a map for comparison.
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

	var valueStrings []string
	var valueArgs []interface{}
	baseStmt := "INSERT INTO block_positions (backup_id, block_id, position) VALUES "

	for i := 0; i < bufEntries; i++ {
		pos := posStartRange + i

		if b.BackupType() == backupTypeDifferential {
			// Skip if the hash was already registered by the last full backup.
			if _, ok := dupMap[pos]; ok && dupMap[pos] == hashMap[pos] {
				continue
			}
		}
		valueStrings = append(valueStrings, "(?, (SELECT id FROM blocks WHERE hash = ?), ?)")
		valueArgs = append(valueArgs, b.Record.ID, hashMap[pos], pos)
	}

	// If there are no inserts, we can abort the transaction.
	if len(valueStrings) == 0 {
		return tx.Rollback()
	}

	// Append the value strings to the base statement.
	stmt := baseStmt + strings.Join(valueStrings, ",")

	// Prepare the query.
	query, err := tx.Prepare(stmt)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer query.Close()

	// Execute the query.
	_, err = query.Exec(valueArgs...)
	if err != nil {
		tx.Rollback()
		return err
	}

	// Commit the transaction
	return tx.Commit()
}

func (b *Backup) writeBlocks(target *os.File, iteration int, bufEntries int, bufCapacity int, blockBuf []byte) (map[int]string, error) {
	// Calculate the hash for each block in the buffer.
	hashMap := b.hashBufferedData(iteration, bufEntries, bufCapacity, blockBuf)

	// Start a transaction to insert the block hashes into the database
	tx, err := b.store.Begin()
	if err != nil {
		return nil, err
	}

	// Sort the hash map by position.
	// Note: With deduplication, there's really no reason we need to care about order.
	// The backups are deterministic, so the order of the hashes will always be the same, which makes it easier to test and verify.
	// TODO - Work to remove this sort.
	var hashMapSlice []int
	for key := range hashMap {
		hashMapSlice = append(hashMapSlice, key)
	}
	sort.Ints(hashMapSlice)

	insertBlockQuery, err := tx.Prepare("INSERT INTO blocks (hash) VALUES (?)")
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer insertBlockQuery.Close()

	for _, pos := range hashMapSlice {
		// Insert the block hash into the database.
		_, err = insertBlockQuery.Exec(hashMap[pos])
		if err != nil {
			if sqliteErr, ok := err.(sqlite3.Error); ok {
				// If there's a constraint error, we know the hash is already in the database.
				// This also means there's no need to write the block to the backup file.
				if sqliteErr.Code == sqlite3.ErrConstraint {
					continue
				}
				return nil, fmt.Errorf("error inserting block hash into database: %v", err)
			}
		}

		// Calculate original starting position.
		startingPos := (pos - (iteration * bufCapacity)) * b.Config.BlockSize
		endingPos := (startingPos + b.Config.BlockSize)

		// TODO - We should be able to parallelize this.
		_, err = target.Write(blockBuf[startingPos:endingPos])
		if err != nil {
			fmt.Printf("Error writing block to backup file: %v\n", err)
		}
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return nil, err
	}

	return hashMap, nil
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

func readBlock(disk *os.File, totalBlocks, blockSize, blockNum int) ([]byte, error) {
	offset := int64(blockSize * blockNum)
	buffer := make([]byte, blockSize)

	endRange := blockSize*blockNum + blockSize
	endOfFile := blockSize * totalBlocks
	if endRange > endOfFile {
		endRange = endOfFile
		trimmedBlockSize := endRange - blockSize*blockNum
		if trimmedBlockSize <= 0 {
			return nil, io.EOF
		}
		buffer = make([]byte, trimmedBlockSize)
	}

	_, err := disk.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	_, err = disk.Read(buffer)
	if err != nil {
		return nil, err
	}

	return buffer, nil
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
