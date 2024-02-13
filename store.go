package block

import (
	"database/sql"
	"time"
)

type Volume struct {
	Id         int
	Name       string
	DevicePath string
}

type BackupRecord struct {
	Id          int
	FileName    string
	VolumeID    int
	BackupType  string
	SizeInBytes int
	totalBlocks int
	blockSize   int
	createdAt   time.Time
}

type Block struct {
	id        int
	hash      string
	createdAt time.Time
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
		backup_type TEXT CHECK(backup_type IN ('full', 'differential')) NOT NULL,
		size_in_bytes INTEGER DEFAULT 0,
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

	return Volume{Id: id, Name: name, DevicePath: devicePath}, nil
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

	return Volume{Id: int(volumeID), Name: name, DevicePath: devicePath}, nil
}

func (s Store) insertBackupRecord(volumeID int, fileName string, backupType string, totalBlocks int, blockSize int) (BackupRecord, error) {
	// Write the backup record to the database
	insertSQL := `INSERT INTO backups (volume_id, file_name, backup_type, total_blocks, block_size) VALUES (?,?,?,?,?);`
	res, err := s.Exec(insertSQL, volumeID, fileName, backupType, totalBlocks, blockSize)
	if err != nil {
		return BackupRecord{}, err
	}

	backupID, err := res.LastInsertId()
	if err != nil {
		return BackupRecord{}, err
	}

	return BackupRecord{
		Id:          int(backupID),
		FileName:    fileName,
		VolumeID:    volumeID,
		BackupType:  backupType,
		totalBlocks: totalBlocks,
		blockSize:   blockSize,
		createdAt:   time.Now(),
	}, nil
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
	var backupType string
	var createdAt time.Time
	row := s.QueryRow("SELECT id, file_name, backup_type, total_blocks, block_size, created_at FROM backups WHERE volume_id = ? AND backup_type = 'full' ORDER BY id DESC LIMIT 1", volumeID)
	if err := row.Scan(&id, &fileName, &backupType, &totalBlocks, &blockSize, &createdAt); err != nil {
		return BackupRecord{}, err
	}

	return BackupRecord{
		Id:          id,
		FileName:    fileName,
		VolumeID:    volumeID,
		BackupType:  backupType,
		totalBlocks: totalBlocks,
		blockSize:   blockSize,
		createdAt:   createdAt,
	}, nil
}

func (s Store) findBackup(id int) (BackupRecord, error) {
	var totalBlocks int
	var fileName string
	var volumeID int
	var blockSize int
	var backupType string
	var createdAt time.Time
	row := s.QueryRow("SELECT file_name, volume_id, backup_type, total_blocks, block_size, created_at FROM backups WHERE id = ? ORDER BY id DESC LIMIT 1", id)
	if err := row.Scan(&fileName, &volumeID, &backupType, &totalBlocks, &blockSize, &createdAt); err != nil {
		return BackupRecord{}, err
	}

	return BackupRecord{
		Id:          id,
		FileName:    fileName,
		VolumeID:    volumeID,
		BackupType:  backupType,
		totalBlocks: totalBlocks,
		blockSize:   blockSize,
		createdAt:   createdAt,
	}, nil
}

func (s Store) insertBlockPosition(backupID int, blockID int, position int) (BlockPosition, error) {
	// Upsert the block position into the database
	insertSQL := `INSERT INTO block_positions (backup_id, block_id, position) VALUES (?,?,?);`
	res, err := s.Exec(insertSQL, backupID, blockID, position)
	if err != nil {
		return BlockPosition{}, err
	}

	posID, err := res.LastInsertId()
	if err != nil {
		return BlockPosition{}, err
	}

	return BlockPosition{
		id:       int(posID),
		backupID: backupID,
		blockID:  blockID,
		position: position,
	}, nil
}

func (s Store) findBlockPositionsByBackup(backupID int) ([]BlockPosition, error) {
	var positions []BlockPosition
	rows, err := s.Query("SELECT id, position, block_id FROM block_positions WHERE backup_id = ?;", backupID)
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

func (s Store) insertBlock(hash string) (*Block, error) {
	block, err := s.findBlock(hash)
	if err != nil {
		return nil, err
	}

	if block != nil {
		return block, nil
	}
	// Write the block to the database
	insertSQL := `INSERT INTO blocks (hash) VALUES (?) ON CONFLICT DO NOTHING;`
	res, err := s.Exec(insertSQL, hash)
	if err != nil {
		return nil, err
	}

	blockID, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}

	return &Block{
		id:        int(blockID),
		hash:      hash,
		createdAt: time.Now(),
	}, nil

}

func (s Store) findBlock(hash string) (*Block, error) {
	var id int
	var createdAt time.Time
	row := s.QueryRow("SELECT id, created_at FROM blocks WHERE hash = ?", hash)
	err := row.Scan(&id, &createdAt)
	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	}

	return &Block{id: id, hash: hash, createdAt: createdAt}, nil
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

// func writeDigest(store *sql.DB, fileName string, totalBlocks int, blockSize int, digest Digest) error {
// 	// Write the digest to the database
// 	insertSQL := `INSERT INTO digests (file_name, total_chunks, chunk_size, entries, full_digest, zero_byte_positions) VALUES (?,?,?,?,?,?);`
// 	entriesJSON, err := json.Marshal(digest.entries)
// 	if err != nil {
// 		return err
// 	}

// 	positions, err := json.Marshal(digest.emptyPositions)
// 	if err != nil {
// 		return err
// 	}

// 	_, err = store.Exec(insertSQL, fileName, totalBlocks, blockSize, entriesJSON, digest.fullDigest, positions)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// func lastFullDigest(store *sql.DB) (Digest, error) {
// 	var id int
// 	var fileName string
// 	var entriesJSON string
// 	var zeroBytePositions string
// 	var totalBlocks int
// 	var blockSize int
// 	row := store.QueryRow("SELECT id, file_name, total_chunks, chunk_size, entries, zero_byte_positions FROM digests where full_digest = true ORDER BY id DESC LIMIT 1")
// 	if err := row.Scan(&id, &fileName, &totalBlocks, &blockSize, &entriesJSON, &zeroBytePositions); err != nil {
// 		return Digest{}, err
// 	}

// 	var digest Digest
// 	if err := json.Unmarshal([]byte(entriesJSON), &digest.entries); err != nil {
// 		return Digest{}, err
// 	}
// 	if err := json.Unmarshal([]byte(zeroBytePositions), &digest.emptyPositions); err != nil {
// 		return Digest{}, err
// 	}

// 	digest.id = id
// 	digest.fileName = fileName
// 	digest.blockSize = blockSize
// 	digest.totalBlocks = totalBlocks

// 	return digest, nil
// }

// func findDigestByID(store *sql.DB, id int) (Digest, error) {
// 	var fileName string
// 	var totalBlocks int
// 	var blockSize int
// 	var fullDigest bool
// 	var entriesJSON string
// 	var zeroBytePositions string
// 	row := store.QueryRow("SELECT file_name, total_chunks, chunk_size, full_digest, entries, zero_byte_positions FROM digests WHERE id = ?", id)
// 	if err := row.Scan(&fileName, &totalBlocks, &blockSize, &fullDigest, &entriesJSON, &zeroBytePositions); err != nil {
// 		return Digest{}, err
// 	}

// 	var digest Digest
// 	if err := json.Unmarshal([]byte(entriesJSON), &digest.entries); err != nil {
// 		return Digest{}, err
// 	}
// 	if err := json.Unmarshal([]byte(zeroBytePositions), &digest.emptyPositions); err != nil {
// 		return Digest{}, err
// 	}

// 	digest.id = id
// 	digest.fullDigest = fullDigest
// 	digest.blockSize = blockSize
// 	digest.totalBlocks = totalBlocks
// 	digest.fileName = fileName

// 	return digest, nil
// }
