package block

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	// assetsDirectory  = "assets"
	restoreDirectory = "restores"
	backupDirectory  = "backups"
	blockSize        = 4096 // Assumes 4k block size

	// hashSizeInBlocks is the number of blocks we evaluate for a given hash.
	// A higher number will result in lower differential backup granularity and lower storage overhead.
	hashSizeInBlocks = 256

	backupTypeDifferential = "differential"
	backupTypeFull         = "full"
)

func Backup(store *sql.DB, vol *Volume) (BackupRecord, error) {
	chunkSize, totalChunks, err := calculateBlocks(vol.devicePath)
	if err != nil {
		return BackupRecord{}, err
	}

	var backupType string

	fullBackup, err := fetchLastFullBackupRecord(store, vol.id)
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
	backupFileName := generateBackupName(vol, backupType)
	backup, err := insertBackupRecord(store, vol.id, backupFileName, backupType, totalChunks, chunkSize)
	if err != nil {
		return BackupRecord{}, err
	}

	dev, err := os.Open(vol.devicePath)
	if err != nil {
		return BackupRecord{}, err
	}
	defer dev.Close()

	// TODO - Figure out a good way to batch these inserts.

	// Create a block digest for the device.
	for chunkNum := 0; chunkNum < totalChunks; chunkNum++ {
		blockData, err := readBlock(dev, chunkSize, chunkNum)
		if err != nil {
			return BackupRecord{}, err
		}

		hash := calculateBlockHash(blockData)

		block, err := insertBlock(store, hash)
		if err != nil {
			return BackupRecord{}, err
		}

		if backup.backupType == backupTypeDifferential {
			refBlock, err := findBlockAtPosition(store, fullBackup.id, chunkNum)
			if err != nil && err != sql.ErrNoRows {
				return BackupRecord{}, err
			}

			// Skip if the hash was already registered by the last full backup.
			if refBlock != nil && refBlock.hash == hash {
				continue
			}

			if refBlock == nil {
				if _, err := insertBlockPosition(store, backup.id, block.id, chunkNum); err != nil {
					return BackupRecord{}, err
				}
				continue
			}
		}

		_, err = insertBlockPosition(store, backup.id, block.id, chunkNum)
		if err != nil {
			return BackupRecord{}, err
		}
	}

	// Create and open up the backup file for writing.
	backupPath := fmt.Sprintf("%s/%s", backupDirectory, backup.fileName)
	backupTarget, err := os.OpenFile(backupPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return backup, fmt.Errorf("error opening restore file: %v", err)
	}
	defer backupTarget.Close()

	// Query sqlite for only the blocks that need to be backed up.
	// TODO - Flag zero block hashes, so we can exclude it.
	rows, err := store.Query("SELECT block_id, MIN(position) AS position FROM block_positions where backup_id = ? GROUP BY block_id ORDER BY block_id;", backup.id)
	if err != nil {
		return backup, err
	}

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
	}

	return backup, nil
}

// calculateBlocks will calculate the total number of blocks and the chunk size for a given file path and store it in the backup record.
func calculateBlocks(devicePath string) (chunkSize int, totalChunks int, err error) {
	fileInfo, err := os.Stat(devicePath)
	if err != nil {
		return
	}

	totalSizeInBytes := fileInfo.Size()
	totalBlocks := totalSizeInBytes / blockSize
	// Calculate the chunk size, or the number of blocks we evaluate for a given hash.
	chunkSize = hashSizeInBlocks * blockSize
	// Calculate the total chunks
	totalChunks = int(totalBlocks) / (chunkSize / blockSize)

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
	return fmt.Sprintf("%s_%s_%d", vol.name, backupType, timestamp)
}

func calculateBlockHash(blockData []byte) string {
	hash := sha256.Sum256(blockData)
	return hex.EncodeToString(hash[:])
}
