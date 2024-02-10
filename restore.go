package block

import (
	"database/sql"
	"fmt"
	"os"
)

func Restore(store *sql.DB, backup BackupRecord) error {
	restorePath := fmt.Sprintf("%s/%s.restore", restoreDirectory, backup.fileName)
	restoreTarget, err := os.OpenFile(restorePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening restore file: %v", err)
	}
	defer restoreTarget.Close()

	switch backup.backupType {
	case backupTypeFull:
		return restoreFromBackup(store, restoreTarget, backup)
	case backupTypeDifferential:
		fullBackup, err := findLastFullBackupRecord(store, backup.volumeID)
		if err != nil {
			return fmt.Errorf("error fetching last full backup: %w", err)
		}

		// Restore from the full backup first
		if err := restoreFromBackup(store, restoreTarget, fullBackup); err != nil {
			return fmt.Errorf("error restoring from full backup: %w", err)
		}

		// Layer the differential backup on top
		return restoreFromBackup(store, restoreTarget, backup)

	default:
		return fmt.Errorf("backup type %s is not supported", backup.backupType)
	}
}

func restoreFromBackup(store *sql.DB, target *os.File, backup BackupRecord) error {
	sourcePath := fmt.Sprintf("%s/%s", backupDirectory, backup.fileName)
	source, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("error opening restore source file: %v", err)
	}
	defer source.Close()

	// Count the total number of unique blocks in the backup
	var totalUniqueBlocks int
	row := store.QueryRow("SELECT COUNT(DISTINCT block_id) FROM block_positions WHERE backup_id = ?", backup.id)
	if err := row.Scan(&totalUniqueBlocks); err != nil {
		return fmt.Errorf("error counting unique blocks: %w", err)
	}

	for chunkNum := 0; chunkNum < totalUniqueBlocks; chunkNum++ {
		// Read block data from the source file
		blockData, err := readBlock(source, backup.chunkSize, chunkNum)
		if err != nil {
			return fmt.Errorf("error reading block at position %d: %w", chunkNum, err)
		}

		// Calculate the hash
		hash := calculateBlockHash(blockData)

		// Query the database for the block positions tied to the hash
		rows, err := store.Query("SELECT position from block_positions bp JOIN blocks b ON bp.block_id = b.id where bp.backup_id = ? AND b.hash = ?", backup.id, hash)
		if err != nil {
			return fmt.Errorf("error quering block positions for hash %s: %w", hash, err)
		}

		// Iterate over each block position and write the block data to the restore file
		for rows.Next() {
			var position int
			if err := rows.Scan(&position); err != nil {
				return fmt.Errorf("failed to scan position: %w", err)
			}

			_, err = target.WriteAt(blockData, int64(position*backup.chunkSize))
			if err != nil {
				return fmt.Errorf("error writing to restore file: %v", err)
			}
		}
	}

	return nil
}
