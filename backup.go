package block

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	restoreDirectory = "restores"
	backupDirectory  = "backups"
	blockSize        = 4096
	// hashSizeInBlocks is the number of blocks we evaluate for a given hash.
	hashSizeInBlocks = 256
	// hashBufferSize is the number of chunks we buffer before writing to the database.
	hashBufferBlockSize = 10

	backupTypeDifferential = "differential"
	backupTypeFull         = "full"
)

type Backup struct {
	Record         *BackupRecord
	outputPath     string
	store          *Store
	vol            *Volume
	lastFullRecord BackupRecord
}

func NewBackup(store *Store, vol *Volume, outputPath string) (*Backup, error) {
	// Calculate the total number of chunks and the chunk size for the device.
	chunkSize, totalChunks, err := calculateBlocks(vol.DevicePath)
	if err != nil {
		return nil, err
	}

	var backupType string
	lastFullRecord, err := store.findLastFullBackupRecord(vol.Id)
	switch {
	case err == sql.ErrNoRows:
		backupType = backupTypeFull
	case err != nil:
		return nil, err
	default:
		backupType = backupTypeDifferential
	}

	fileName := generateBackupName(vol, backupType)

	// TODO - Consider storing a checksum of the target volume, so we can verify at restore time.
	br, err := store.insertBackupRecord(vol.Id, fileName, backupType, totalChunks, chunkSize)
	if err != nil {
		return nil, err
	}

	return &Backup{
		store:          store,
		vol:            vol,
		outputPath:     outputPath,
		Record:         &br,
		lastFullRecord: lastFullRecord,
	}, nil
}

func (b *Backup) TotalChunks() int {
	return b.Record.TotalChunks
}

func (b *Backup) ChunkSize() int {
	return b.Record.ChunkSize
}

func (b *Backup) BackupType() string {
	return b.Record.BackupType
}

func (b *Backup) FileName() string {
	return b.Record.FileName
}

func (b *Backup) OutputPath() string {
	return b.outputPath
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
	bufSize := hashBufferBlockSize * hashSizeInBlocks * blockSize

	// The number of individual chunks we can store in the buffer.
	bufferChunks := bufSize / (hashSizeInBlocks * blockSize)

	if b.TotalChunks()%bufferChunks != 0 {
		return fmt.Errorf("bufferChunks: %d is not a multiple of totalChunks: %d", bufferChunks, b.TotalChunks())
	}

	// The current iteration we are on.
	iteration := 0

	// Read chunks until we have enough to fill the buffer.
	for iteration*bufferChunks < b.TotalChunks() {
		// Create a buffer to store the block hashes.
		hashBuffer := make([]byte, 0, bufSize)

		// Read chunks until we have enough to fill the buffer.
		for chunkNum := 0; chunkNum < bufferChunks; chunkNum++ {
			// Determine the position of the chunk.
			chunkPos := iteration*bufferChunks + chunkNum

			// Read the block data from the device.
			blockData, err := readBlock(dev, b.ChunkSize(), chunkPos)
			if err != nil {
				return fmt.Errorf("error reading block data at position %d: %v", chunkPos, err)
			}

			hashBuffer = append(hashBuffer, blockData...)
		}

		// Insert the block hashes into the database.
		hashMap, err := b.insertBlocksTransaction(iteration, bufferChunks, hashBuffer)
		if err != nil {
			return err
		}

		// Insert the block positions into the database.
		if err := b.insertBlockPositionsTransaction(iteration, bufferChunks, hashMap); err != nil {
			return err
		}
		iteration++
	}

	// Create and open up the backup file for writing.
	backupPath := fmt.Sprintf("%s/%s", b.outputPath, b.FileName())
	backupTarget, err := os.OpenFile(backupPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening restore file: %v", err)
	}
	defer backupTarget.Close()

	// Query sqlite for only the blocks that need to be backed up.
	// TODO - Flag zero block hashes, so we can exclude it.
	rows, err := b.store.Query("SELECT block_id, MIN(position) AS position FROM block_positions where backup_id = ? GROUP BY block_id ORDER BY block_id;", b.Record.Id)
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

		blockData, err := readBlock(dev, b.ChunkSize(), position)
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

func (b *Backup) insertBlockPositionsTransaction(iteration int, bufferChunks int, hashMap map[int]string) error {
	if len(hashMap) != 0 {
		// Start a transaction to insert the block positions into the database
		tx, err := b.store.Begin()
		if err != nil {
			return err
		}

		for i := 0; i < bufferChunks; i++ {
			// Determine the position of the chunk.
			pos := iteration*bufferChunks + i

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

func (b *Backup) insertBlocksTransaction(iteration int, bufChunks int, buf []byte) (map[int]string, error) {
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
	for i := 0; i < bufChunks; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			startingPos := hashSizeInBlocks * blockSize * i
			endingPos := startingPos + hashSizeInBlocks*blockSize

			// Read byte range for the block.
			blockData := buf[startingPos:endingPos]

			// Calculate the hash for the block.
			hash := calculateBlockHash(blockData)

			// Determine the position of the chunk.
			pos := iteration*bufChunks + i

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

func readBlock(disk *os.File, chunkSize, chunkNum int) ([]byte, error) {
	buffer := make([]byte, chunkSize)
	_, err := disk.Seek(int64(chunkSize*chunkNum), 0)
	if err != nil {
		return nil, err
	}
	_, err = disk.Read(buffer)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func generateBackupName(vol *Volume, backupType string) string {
	timestamp := time.Now().UnixMilli()
	return fmt.Sprintf("%s_%s_%d", vol.Name, backupType, timestamp)
}

func calculateBlockHash(blockData []byte) string {
	hash := sha256.Sum256(blockData)
	return hex.EncodeToString(hash[:])
}

// calculateBlocks will calculate the total number of blocks and the chunk size for a given file path and store it in the backup record.
func calculateBlocks(devicePath string) (chunkSize int, totalChunks int, err error) {
	fileInfo, err := os.Stat(devicePath)
	if err != nil {
		return
	}
	mode := fileInfo.Mode()

	totalSizeInBytes := fileInfo.Size()

	// Check to see if the file is a block device.
	if mode&os.ModeDevice != 0 && mode&os.ModeCharDevice == 0 {
		totalSizeInBytes, err = GetBlockDeviceSize(devicePath)
		if err != nil {
			return
		}
	}

	totalBlocks := totalSizeInBytes / blockSize
	chunkSize = hashSizeInBlocks * blockSize
	totalChunksFloat := float64(totalBlocks) / (float64(chunkSize) / float64(blockSize))
	totalChunks = int(math.Ceil(totalChunksFloat))

	return
}
