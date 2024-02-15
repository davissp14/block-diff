package block

import (
	"database/sql"
	"time"
)

type Volume struct {
	ID         int
	Name       string
	DevicePath string
}

type BackupRecord struct {
	ID           int
	FileName     string
	FullPath     string
	OutputFormat string
	VolumeID     int
	BackupType   string
	SizeInBytes  int
	TotalBlocks  int
	BlockSize    int
	CreatedAt    time.Time
}

type Block struct {
	hash string
}

type BlockPosition struct {
	id       int
	backupID int
	blockID  int
	position int
}

type Store struct {
	*sql.DB
}

func (s Store) SetupDB() error {
	createVolumesTableSQL := `CREATE TABLE IF NOT EXISTS volumes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		devicePath TEXT NOT NULL,
		UNIQUE(name)
	);`
	_, err := s.Exec(createVolumesTableSQL)
	if err != nil {
		return err
	}

	createBackupsTableSQL := `CREATE TABLE IF NOT EXISTS backups (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		volume_id INTEGER NOT NULL,
		file_name TEXT NOT NULL,
		full_path TEXT NOT NULL,
		output_format TEXT CHECK(output_format IN ('file', 'stdout')) NOT NULL DEFAULT 'file',
		backup_type TEXT CHECK(backup_type IN ('full', 'differential')) NOT NULL,
		size_in_bytes INTEGER NOT NULL DEFAULT 0,
		total_blocks INTEGER NOT NULL,
		block_size INTEGER NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(volume_id) REFERENCES volumes(id)
	);`
	_, err = s.Exec(createBackupsTableSQL)
	if err != nil {
		return err
	}

	createBlocksTableSQL := `CREATE TABLE IF NOT EXISTS blocks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		hash TEXT NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(hash)
	);`
	_, err = s.Exec(createBlocksTableSQL)
	if err != nil {
		return err
	}

	createBlockPositionsSQL := `CREATE TABLE IF NOT EXISTS block_positions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		backup_id INTEGER NOT NULL,
		block_id INTEGER NOT NULL,
		position INTEGER NOT NULL,
		FOREIGN KEY(backup_id) REFERENCES backups(id),
		FOREIGN KEY(block_id) REFERENCES blocks(id)
		UNIQUE(backup_id, block_id, position)
	);`
	_, err = s.Exec(createBlockPositionsSQL)
	if err != nil {
		return err
	}

	return nil
}

func NewStore() (*Store, error) {
	s, err := sql.Open("sqlite3", "backups.db?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		return nil, err
	}

	return &Store{s}, nil
}

func (s Store) FindVolume(name string) (Volume, error) {
	var id int
	var devicePath string
	row := s.QueryRow("SELECT id, devicePath FROM volumes WHERE name = ?", name)
	if err := row.Scan(&id, &devicePath); err != nil {
		return Volume{}, err
	}

	return Volume{ID: id, Name: name, DevicePath: devicePath}, nil
}

func (s Store) InsertVolume(name, devicePath string) (Volume, error) {
	// Write the volume to the database
	insertSQL := `INSERT INTO volumes (name, devicePath) VALUES (?,?) ON CONFLICT DO NOTHING;`
	res, err := s.Exec(insertSQL, name, devicePath)
	if err != nil {
		return Volume{}, err
	}

	volumeID, err := res.LastInsertId()
	if err != nil {
		return Volume{}, err
	}

	if volumeID == 0 {
		return s.FindVolume(name)
	}

	return Volume{ID: int(volumeID), Name: name, DevicePath: devicePath}, nil
}

func (s Store) insertBackupRecord(volumeID int, fileName string, fullPath string, outputFormat string, backupType string, totalBlocks, blockSize, sizeInBytes int) (BackupRecord, error) {
	// Write the backup record to the database
	insertSQL := `INSERT INTO backups (volume_id, file_name, full_path, output_format, backup_type, total_blocks, block_size, size_in_bytes) VALUES (?,?,?,?,?,?,?,?);`
	res, err := s.Exec(insertSQL, volumeID, fileName, fullPath, outputFormat, backupType, totalBlocks, blockSize, sizeInBytes)
	if err != nil {
		return BackupRecord{}, err
	}

	backupID, err := res.LastInsertId()
	if err != nil {
		return BackupRecord{}, err
	}

	return BackupRecord{
		ID:           int(backupID),
		FileName:     fileName,
		FullPath:     fullPath,
		OutputFormat: outputFormat,
		VolumeID:     volumeID,
		BackupType:   backupType,
		TotalBlocks:  totalBlocks,
		BlockSize:    blockSize,
		SizeInBytes:  sizeInBytes,
		CreatedAt:    time.Now(),
	}, nil
}

func (s Store) ListBackups() ([]BackupRecord, error) {
	var backups []BackupRecord
	rows, err := s.Query("SELECT id, volume_id, file_name, full_path, output_format, backup_type, total_blocks, block_size, size_in_bytes, created_at FROM backups ORDER BY id ASC")
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var id int
		var volumeID int
		var fileName string
		var fullPath string
		var outputFormat string
		var backupType string
		var totalBlocks int
		var blockSize int
		var sizeInBytes int
		var createdAt time.Time
		if err := rows.Scan(&id, &volumeID, &fileName, &fullPath, &outputFormat, &backupType, &totalBlocks, &blockSize, &sizeInBytes, &createdAt); err != nil {
			return backups, err
		}

		backups = append(backups, BackupRecord{
			ID:           id,
			FileName:     fileName,
			FullPath:     fullPath,
			OutputFormat: outputFormat,
			VolumeID:     volumeID,
			BackupType:   backupType,
			TotalBlocks:  totalBlocks,
			BlockSize:    blockSize,
			SizeInBytes:  sizeInBytes,
			CreatedAt:    createdAt,
		})
	}

	return backups, nil
}

func (s Store) updateBackupSize(backupID int, sizeInBytes int) error {
	_, err := s.Exec("UPDATE backups SET size_in_bytes = ? WHERE id = ?", sizeInBytes, backupID)
	return err
}

func (s Store) TotalBlocks() (int, error) {
	var count int
	row := s.QueryRow("SELECT count(*) FROM blocks;")
	if err := row.Scan(&count); err != nil {
		return -1, err
	}

	return count, nil
}

func (s Store) findLastFullBackupRecord(volumeID int) (BackupRecord, error) {
	var id int
	var totalBlocks int
	var blockSize int
	var fileName string
	var fullPath string
	var outputFormat string
	var backupType string
	var createdAt time.Time
	row := s.QueryRow("SELECT id, file_name, full_path, output_format, backup_type, total_blocks, block_size, created_at FROM backups WHERE volume_id = ? AND backup_type = 'full' ORDER BY id DESC LIMIT 1", volumeID)
	if err := row.Scan(&id, &fileName, &fullPath, &outputFormat, &backupType, &totalBlocks, &blockSize, &createdAt); err != nil {
		return BackupRecord{}, err
	}

	return BackupRecord{
		ID:           id,
		FileName:     fileName,
		FullPath:     fullPath,
		OutputFormat: outputFormat,
		VolumeID:     volumeID,
		BackupType:   backupType,
		TotalBlocks:  totalBlocks,
		BlockSize:    blockSize,
		CreatedAt:    createdAt,
	}, nil
}

func (s Store) findBackup(id int) (BackupRecord, error) {
	var totalBlocks int
	var fileName string
	var fullPath string
	var outputFormat string
	var volumeID int
	var blockSize int
	var backupType string
	var createdAt time.Time
	row := s.QueryRow("SELECT file_name, full_path, output_format, volume_id, backup_type, total_blocks, block_size, created_at FROM backups WHERE id = ? ORDER BY id DESC LIMIT 1", id)
	if err := row.Scan(&fileName, &fullPath, &outputFormat, &volumeID, &backupType, &totalBlocks, &blockSize, &createdAt); err != nil {
		return BackupRecord{}, err
	}

	return BackupRecord{
		ID:           id,
		FileName:     fileName,
		FullPath:     fullPath,
		OutputFormat: outputFormat,
		VolumeID:     volumeID,
		BackupType:   backupType,
		TotalBlocks:  totalBlocks,
		BlockSize:    blockSize,
		CreatedAt:    createdAt,
	}, nil
}

func (s Store) findBlockPositionsByBackup(backupID int) ([]BlockPosition, error) {
	var positions []BlockPosition
	rows, err := s.Query("SELECT id, position, block_id FROM block_positions WHERE backup_id = ? ORDER BY position ASC;", backupID)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var id int
		var position int
		var blockID int
		if err := rows.Scan(&id, &position, &blockID); err != nil {
			return positions, err
		}

		positions = append(positions, BlockPosition{
			id:       id,
			backupID: backupID,
			blockID:  blockID,
			position: position,
		})
	}

	return positions, nil
}

func (s Store) UniqueBlocksInBackup(backupID int) (int, error) {
	var count int
	row := s.QueryRow("SELECT COUNT(DISTINCT block_id) FROM block_positions WHERE backup_id = ?", backupID)
	if err := row.Scan(&count); err != nil {
		return -1, err
	}

	return count, nil
}

func (s Store) findBlockAtPosition(backupID int, pos int) (*Block, error) {
	var hash string
	row := s.QueryRow("SELECT hash FROM blocks b JOIN block_positions bp ON bp.block_id = b.id WHERE bp.backup_id = ? AND bp.position = ?", backupID, pos)
	err := row.Scan(&hash)
	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	}

	return &Block{hash: hash}, nil
}
