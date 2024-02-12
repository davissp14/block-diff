package block

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
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

func Backup(store *Store, vol *Volume, outputPath string) (BackupRecord, error) {
	chunkSize, totalChunks, err := calculateBlocks(vol.DevicePath)
	if err != nil {
		return BackupRecord{}, err
	}

	var backupType string

	fullBackup, err := store.findLastFullBackupRecord(vol.Id)
	switch {
	case err == sql.ErrNoRows:
		backupType = backupTypeFull
	case err != nil:
		return BackupRecord{}, err
	default:
		backupType = backupTypeDifferential
	}

	// Create the backup record
	// TODO - Consider storing a checksum of the target volume, so we can verify at restore time.
	backup, err := store.insertBackupRecord(vol.Id, generateBackupName(vol, backupType), backupType, totalChunks, chunkSize)
	if err != nil {
		return BackupRecord{}, err
	}

	dev, err := os.Open(vol.DevicePath)
	if err != nil {
		return BackupRecord{}, err
	}
	defer dev.Close()

	// Create a buffer to store the block hashes.
	// The number of hashes we buffer before writing to the database.
	bufSize := hashBufferBlockSize * hashSizeInBlocks * blockSize

	// The number of individual chunks we can store in the buffer.
	bufferChunks := bufSize / (hashSizeInBlocks * blockSize)

	if totalChunks%bufferChunks != 0 {
		return BackupRecord{}, fmt.Errorf("bufferChunks: %d is not a multiple of totalChunks: %d", bufferChunks, totalChunks)
	}

	// The current iteration we are on.
	iteration := 0

	// Read chunks until we have enough to fill the buffer.
	for iteration*bufferChunks < totalChunks {

		// Create a buffer to store the block hashes.
		hashBuffer := make([]byte, 0, bufSize)

		// Read chunks until we have enough to fill the buffer.
		for chunkNum := 0; chunkNum < bufferChunks; chunkNum++ {
			// Determine the position of the chunk.
			chunkPos := iteration*bufferChunks + chunkNum

			// Read the block data from the device.
			blockData, err := readBlock(dev, chunkSize, chunkPos)
			if err != nil {
				return BackupRecord{}, fmt.Errorf("error reading block data at position %d: %v", chunkPos, err)
			}

			hashBuffer = append(hashBuffer, blockData...)
		}

		// Start a transaction to insert the block hashes into the database
		tx, err := store.Begin()
		if err != nil {
			return BackupRecord{}, err
		}

		hashMap := make(map[int]string)

		insertBlockQuery, err := tx.Prepare("INSERT INTO blocks (hash) VALUES (?) ON CONFLICT DO NOTHING")
		if err != nil {
			tx.Rollback()
			return BackupRecord{}, err
		}
		defer insertBlockQuery.Close()

		var mu sync.Mutex

		var wg sync.WaitGroup
		// Calculate the hash for each block in the buffer.
		for i := 0; i < bufferChunks; i++ {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()

				startingPos := hashSizeInBlocks * blockSize * i
				endingPos := startingPos + hashSizeInBlocks*blockSize

				// Read byte range for the block.
				blockData := hashBuffer[startingPos:endingPos]

				// Calculate the hash for the block.
				hash := calculateBlockHash(blockData)

				// Determine the position of the chunk.
				pos := iteration*bufferChunks + i

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
				return BackupRecord{}, err
			}
		}

		if err := tx.Commit(); err != nil {
			return BackupRecord{}, err
		}

		if len(hashMap) != 0 {
			// Start a transaction to insert the block positions into the database
			tx, err = store.Begin()
			if err != nil {
				return BackupRecord{}, err
			}

			for i := 0; i < bufferChunks; i++ {
				// Determine the position of the chunk.
				pos := iteration*bufferChunks + i

				if backup.BackupType == backupTypeDifferential {
					refBlock, err := store.findBlockAtPosition(fullBackup.Id, pos)
					if err != nil && err != sql.ErrNoRows {
						return BackupRecord{}, err
					}

					// Skip if the hash was already registered by the last full backup.
					if refBlock != nil && refBlock.hash == hashMap[pos] {
						continue
					}
				}

				query, err := tx.Prepare("INSERT INTO block_positions (backup_id, block_id, position) VALUES (?, (SELECT id FROM blocks WHERE hash = ?), ?)")
				if err != nil {
					tx.Rollback()
					return BackupRecord{}, err
				}
				defer query.Close()

				_, err = query.Exec(backup.Id, hashMap[pos], pos)
				if err != nil {
					tx.Rollback()
					return BackupRecord{}, err
				}
			}

			// Commit the transaction
			if err := tx.Commit(); err != nil {
				return BackupRecord{}, err
			}
		}
		iteration++
	}

	// Create and open up the backup file for writing.
	backupPath := fmt.Sprintf("%s/%s", outputPath, backup.FileName)
	backupTarget, err := os.OpenFile(backupPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return backup, fmt.Errorf("error opening restore file: %v", err)
	}
	defer backupTarget.Close()

	// Query sqlite for only the blocks that need to be backed up.
	// TODO - Flag zero block hashes, so we can exclude it.
	rows, err := store.Query("SELECT block_id, MIN(position) AS position FROM block_positions where backup_id = ? GROUP BY block_id ORDER BY block_id;", backup.Id)
	if err != nil {
		return backup, err
	}

	totalBytesWritten := 0
	// Iterate over each resolved block position and write the block data for that position to the backup file.
	for rows.Next() {
		var blockID int
		var position int
		if err := rows.Scan(&blockID, &position); err != nil {
			return backup, err
		}

		blockData, err := readBlock(dev, chunkSize, position)
		if err != nil {
			return backup, err
		}

		// Write blockdata to the backup file
		_, err = backupTarget.Write(blockData)
		if err != nil {
			return backup, fmt.Errorf("error writing to backup file: %v", err)
		}

		totalBytesWritten += len(blockData)
	}

	if err := store.updateBackupSize(backup.Id, totalBytesWritten); err != nil {
		return backup, err
	}

	backup.SizeInBytes = totalBytesWritten

	return backup, nil
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

func GetBlockDeviceSize(devicePath string) (int64, error) {
	cmd := exec.Command("blockdev", "--getsize64", devicePath)
	result, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(strings.TrimSpace(string(result)), 10, 64)
}
