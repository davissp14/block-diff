package block

import (
	"fmt"
	"os"
)

type Restore struct {
	store          *Store
	backup         BackupRecord
	lastFullBackup BackupRecord
	config         RestoreConfig
}

func NewRestore(cfg RestoreConfig) (*Restore, error) {
	if cfg.OutputDirectory != "" {
		// Ensure the restore directory exists
		if _, err := os.Stat(cfg.OutputDirectory); err != nil {
			return nil, fmt.Errorf("restore directory does not exist: %v", err)
		}
	}

	// Resolve the backup record
	backup, err := cfg.Store.findBackup(cfg.SourceBackupID)
	if err != nil {
		return nil, fmt.Errorf("error resolving backup record with id %d: %v", cfg.SourceBackupID, err)
	}

	restore := &Restore{
		store:  cfg.Store,
		backup: backup,
		config: cfg,
	}

	if backup.BackupType == backupTypeDifferential {
		// Ensure the full backup exists
		lfb, err := cfg.Store.findLastFullBackupRecord(backup.VolumeID)
		if err != nil {
			return nil, fmt.Errorf("error resolving last full backup record: %v", err)
		}

		restore.lastFullBackup = lfb
	}

	return restore, nil
}

func (r *Restore) FullRestorePath() string {
	return fmt.Sprintf("%s/%s", r.config.OutputDirectory, r.config.OutputFileName)
}

func (r *Restore) Run() error {
	fmt.Printf("Restoring backup %s to %s\n", r.backup.FileName, r.FullRestorePath())

	restoreTarget, err := os.OpenFile(r.FullRestorePath(), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening restore file: %v", err)
	}
	defer restoreTarget.Close()

	switch r.backup.BackupType {
	case backupTypeFull:
		return r.restoreFromBackup(restoreTarget, r.backup)
	case backupTypeDifferential:
		// Restore from the full backup first
		if err := r.restoreFromBackup(restoreTarget, r.lastFullBackup); err != nil {
			return fmt.Errorf("error restoring from full backup: %w", err)
		}

		// Layer the differential backup on top
		return r.restoreFromBackup(restoreTarget, r.backup)

	default:
		return fmt.Errorf("backup type %s is not supported", r.backup.BackupType)
	}
}

func (r *Restore) restoreFromBackup(target *os.File, backup BackupRecord) error {
	// TODO - FIX THIS
	sourcePath := fmt.Sprintf("%s/%s", backupDirectory, backup.FileName)
	source, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("error opening restore source file: %v", err)
	}
	defer source.Close()

	// Count the total number of unique blocks in the backup
	var totalUniqueBlocks int
	row := r.store.QueryRow("SELECT COUNT(DISTINCT block_id) FROM block_positions WHERE backup_id = ?", backup.Id)
	if err := row.Scan(&totalUniqueBlocks); err != nil {
		return fmt.Errorf("error counting unique blocks: %w", err)
	}

	for blockNum := 0; blockNum < totalUniqueBlocks; blockNum++ {
		// Read block data from the source file
		blockData, err := readBlock(source, totalUniqueBlocks, backup.blockSize, blockNum)
		if err != nil {
			return fmt.Errorf("error reading block at position %d: %w", blockNum, err)
		}

		// Calculate the hash
		hash := calculateBlockHash(blockData)

		// Query the database for the block positions tied to the hash
		rows, err := r.store.Query("SELECT position from block_positions bp JOIN blocks b ON bp.block_id = b.id where bp.backup_id = ? AND b.hash = ?", backup.Id, hash)
		if err != nil {
			return fmt.Errorf("error quering block positions for hash %s: %w", hash, err)
		}

		// Iterate over each block position and write the block data to the restore file
		for rows.Next() {
			if rows.Err() != nil {
				return fmt.Errorf("error reading block positions: %w", rows.Err())
			}
			var pos int
			if err := rows.Scan(&pos); err != nil {
				return fmt.Errorf("failed to scan position: %w", err)
			}

			_, err = target.WriteAt(blockData, int64(pos*backup.blockSize))
			if err != nil {
				return fmt.Errorf("error writing to restore file: %v", err)
			}
		}
		rows.Close()
	}

	return nil
}
