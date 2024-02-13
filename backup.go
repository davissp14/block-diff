package block

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	restoreDirectory = "restores"
	backupDirectory  = "backups"

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

	// Calculate the total number of blocks for the device.
	totalBlocks := calculateTotalBlocks(cfg.BlockSize, sizeInBytes)

	// Find the volume for the device path.
	vol, err := resolveVolume(cfg.Store, cfg.DevicePath)
	if err != nil {
		return nil, err
	}

	// Find the last full backup record.
	lastFullRecord, err := cfg.Store.findLastFullBackupRecord(vol.Id)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	// Determine the backup type.
	backupType, err := determineBackupType(cfg.Store, vol, lastFullRecord)
	if err != nil {
		return nil, err
	}

	if cfg.OutputFileName == "" {
		cfg.OutputFileName = generateBackupName(vol, backupType)
	}

	fullPath := fmt.Sprintf("%s/%s", cfg.OutputDirectory, cfg.OutputFileName)

	// TODO - Consider storing a checksum of the target volume, so we can verify at restore time.
	br, err := cfg.Store.insertBackupRecord(vol.Id, cfg.OutputFileName, fullPath, backupType, totalBlocks, cfg.BlockSize)
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
	return b.Record.totalBlocks
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
	dev, err := os.Open(b.vol.DevicePath)
	if err != nil {
		return err
	}
	defer dev.Close()

	// Create a buffer to store the block hashes.
	// The number of hashes we buffer before writing to the database.
	bufSize := b.Config.BlockBufferSize * b.Config.BlockSize

	// The number of individual blocks we can store in the buffer.
	bufBlocks := bufSize / b.Config.BlockSize

	// The current iteration we are on.
	iteration := 0

	// Read chunks until we have enough to fill the buffer.
	for iteration*bufBlocks < b.TotalBlocks() {
		// Create a buffer to store the block hashes.
		blockBuf := make([]byte, 0, bufSize)

		// Read chunks until we have enough to fill the buffer.
		for blockNum := 0; blockNum < bufBlocks; blockNum++ {
			// Determine the position of the chunk.
			blockPos := iteration*bufBlocks + blockNum

			// Read the block data from the device.
			blockData, err := readBlock(dev, b.TotalBlocks(), b.Config.BlockSize, blockPos)
			switch {
			case err == io.EOF:
				continue
			case err != nil:
				return fmt.Errorf("error reading block data at position %d: %v", blockPos, err)
			}

			blockBuf = append(blockBuf, blockData...)
		}

		// Ensure the buffer size is the expected size.
		if len(blockBuf) < bufSize {
			tmpBuf := make([]byte, len(blockBuf))
			copy(tmpBuf, blockBuf)
			blockBuf = tmpBuf
		}

		// Insert the block hashes into the database.
		hashMap, err := b.insertBlocksTransaction(iteration, bufBlocks, blockBuf)
		if err != nil {
			return err
		}

		entries := len(blockBuf) / b.Config.BlockSize

		// Insert the block positions into the database.
		if err := b.insertBlockPositionsTransaction(iteration, entries, bufBlocks, hashMap); err != nil {
			return err
		}
		iteration++
	}

	var backupTarget *os.File

	// Open up the destination file for writing.
	switch b.Config.OutputFormat {
	case BackupOutputFormatSTDOUT:
		backupTarget = os.Stdout
		defer backupTarget.Close()
	case BackupOutputFormatFile:
		backupTarget, err = os.OpenFile(b.FullPath(), os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("error opening restore file: %v", err)
		}
		defer backupTarget.Close()
	default:
		return fmt.Errorf("unsupported output format: %s", b.Config.OutputFormat)
	}

	// Query sqlite for only the blocks that need to be backed up.
	// TODO - Flag zero block hashes, so we can exclude it.
	rows, err := b.store.Query("SELECT block_id, MIN(position) AS position FROM block_positions where backup_id = ? GROUP BY block_id ORDER BY position;", b.Record.Id)
	if err != nil {
		return err
	}

	totalBytesWritten := 0
	// Iterate over each resolved block position and write the block data for that position to the backup file.
	for rows.Next() {
		var blockID int
		var position int
		if err := rows.Scan(&blockID, &position); err != nil {
			return err
		}

		// Read the block data from the device.
		blockData, err := readBlock(dev, b.TotalBlocks(), b.Config.BlockSize, position)
		if err != nil {
			return err
		}

		// Write blockdata to the backup file
		_, err = backupTarget.Write(blockData)
		if err != nil {
			return fmt.Errorf("error writing to backup file: %v", err)
		}

		totalBytesWritten += len(blockData)
	}

	if err := b.store.updateBackupSize(b.Record.Id, totalBytesWritten); err != nil {
		return err
	}

	b.Record.SizeInBytes = totalBytesWritten

	return nil
}

func (b *Backup) insertBlockPositionsTransaction(iteration int, entries int, bufBlocks int, hashMap map[int]string) error {
	if len(hashMap) != 0 {
		// Start a transaction to insert the block positions into the database
		tx, err := b.store.Begin()
		if err != nil {
			return err
		}

		for i := 0; i < entries; i++ {
			// Determine the position of the chunk.
			pos := iteration*bufBlocks + i

			if b.BackupType() == backupTypeDifferential {
				refBlock, err := b.store.findBlockAtPosition(b.lastFullRecord.Id, pos)
				if err != nil && err != sql.ErrNoRows {
					return err
				}

				// Skip if the hash was already registered by the last full backup.
				if refBlock != nil && refBlock.hash == hashMap[pos] {
					continue
				}
			}

			query, err := tx.Prepare("INSERT INTO block_positions (backup_id, block_id, position) VALUES (?, (SELECT id FROM blocks WHERE hash = ?), ?)")
			if err != nil {
				tx.Rollback()
				return err
			}
			defer query.Close()

			_, err = query.Exec(b.Record.Id, hashMap[pos], pos)
			if err != nil {
				tx.Rollback()
				return err
			}
		}

		// Commit the transaction
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func (b *Backup) insertBlocksTransaction(iteration int, bufBlocks int, buf []byte) (map[int]string, error) {
	hashMap := make(map[int]string)

	// Start a transaction to insert the block hashes into the database
	tx, err := b.store.Begin()
	if err != nil {
		return nil, err
	}

	insertBlockQuery, err := tx.Prepare("INSERT INTO blocks (hash) VALUES (?) ON CONFLICT DO NOTHING")
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer insertBlockQuery.Close()

	var mu sync.Mutex
	var wg sync.WaitGroup

	// Calculate the hash for each block in the buffer.
	for i := 0; i < len(buf)/b.Config.BlockSize; i++ {
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
			pos := iteration*bufBlocks + i

			mu.Lock()
			hashMap[pos] = hash
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// Insert the block hashes into the database.
	for _, value := range hashMap {
		_, err = insertBlockQuery.Exec(value)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return nil, err
	}

	return hashMap, nil
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

func determineBackupType(store *Store, vol *Volume, lastFull BackupRecord) (string, error) {
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
	hash := sha256.Sum256(blockData)
	return hex.EncodeToString(hash[:])
}

func calculateTotalBlocks(blockSize int, sizeInBytes int) int {
	totalBlocks := float64(sizeInBytes) / float64(blockSize)
	return int(math.Ceil(totalBlocks))
}
