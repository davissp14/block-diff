package block

import (
	"database/sql"
	"time"
)

type Volume struct {
	id         int
	name       string
	devicePath string
}

type BackupRecord struct {
	id          int
	fileName    string
	volumeID    int
	backupType  string
	totalChunks int
	chunkSize   int
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

func setupDB(store *sql.DB) error {
	createVolumesTableSQL := `CREATE TABLE IF NOT EXISTS volumes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		devicePath TEXT NOT NULL,
		UNIQUE(name)
	);`
	_, err := store.Exec(createVolumesTableSQL)
	if err != nil {
		return err
	}

	createBackupsTableSQL := `CREATE TABLE IF NOT EXISTS backups (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		volume_id INTEGER NOT NULL,
		file_name TEXT NOT NULL,
		backup_type TEXT CHECK(backup_type IN ('full', 'differential')) NOT NULL,
		total_chunks INTEGER NOT NULL,
		chunk_size INTEGER NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(volume_id) REFERENCES volumes(id)
	);`
	_, err = store.Exec(createBackupsTableSQL)
	if err != nil {
		return err
	}

	createBlocksTableSQL := `CREATE TABLE IF NOT EXISTS blocks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		hash TEXT NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(hash)
	);`
	_, err = store.Exec(createBlocksTableSQL)
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
	_, err = store.Exec(createBlockPositionsSQL)
	if err != nil {
		return err
	}

	return nil
}

func findVolume(store *sql.DB, name string) (Volume, error) {
	var id int
	var devicePath string
	row := store.QueryRow("SELECT id, devicePath FROM volumes WHERE name = ?", name)
	if err := row.Scan(&id, &devicePath); err != nil {
		return Volume{}, err
	}

	return Volume{id: id, name: name, devicePath: devicePath}, nil
}

func insertVolume(store *sql.DB, name, devicePath string) (Volume, error) {
	// Write the volume to the database
	insertSQL := `INSERT INTO volumes (name, devicePath) VALUES (?,?) ON CONFLICT DO NOTHING;`
	res, err := store.Exec(insertSQL, name, devicePath)
	if err != nil {
		return Volume{}, err
	}

	volumeID, err := res.LastInsertId()
	if err != nil {
		return Volume{}, err
	}

	if volumeID == 0 {
		return findVolume(store, name)
	}

	return Volume{id: int(volumeID), name: name, devicePath: devicePath}, nil
}

func insertBackupRecord(store *sql.DB, volumeID int, fileName string, backupType string, totalChunks int, chunkSize int) (BackupRecord, error) {
	// Write the backup record to the database
	insertSQL := `INSERT INTO backups (volume_id, file_name, backup_type, total_chunks, chunk_size) VALUES (?,?,?,?,?);`
	res, err := store.Exec(insertSQL, volumeID, fileName, backupType, totalChunks, chunkSize)
	if err != nil {
		return BackupRecord{}, err
	}

	backupID, err := res.LastInsertId()
	if err != nil {
		return BackupRecord{}, err
	}

	return BackupRecord{
		id:          int(backupID),
		fileName:    fileName,
		volumeID:    volumeID,
		backupType:  backupType,
		totalChunks: totalChunks,
		chunkSize:   chunkSize,
		createdAt:   time.Now(),
	}, nil
}

func queryTotalBlocks(store *sql.DB) (int, error) {
	var count int
	row := store.QueryRow("SELECT count(*) FROM blocks;")
	if err := row.Scan(&count); err != nil {
		return -1, err
	}

	return count, nil
}

func findLastFullBackupRecord(store *sql.DB, volumeID int) (BackupRecord, error) {
	var id int
	var totalChunks int
	var chunkSize int
	var fileName string
	var backupType string
	var createdAt time.Time
	row := store.QueryRow("SELECT id, file_name, backup_type, total_chunks, chunk_size, created_at FROM backups WHERE volume_id = ? AND backup_type = 'full' ORDER BY id DESC LIMIT 1", volumeID)
	if err := row.Scan(&id, &fileName, &backupType, &totalChunks, &chunkSize, &createdAt); err != nil {
		return BackupRecord{}, err
	}

	return BackupRecord{
		id:          id,
		fileName:    fileName,
		volumeID:    volumeID,
		backupType:  backupType,
		totalChunks: totalChunks,
		chunkSize:   chunkSize,
		createdAt:   createdAt,
	}, nil
}

func insertBlockPosition(store *sql.DB, backupID int, blockID int, position int) (BlockPosition, error) {
	// Upsert the block position into the database
	insertSQL := `INSERT INTO block_positions (backup_id, block_id, position) VALUES (?,?,?);`
	res, err := store.Exec(insertSQL, backupID, blockID, position)
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

func findBlockPositionsByBackup(store *sql.DB, backupID int) ([]BlockPosition, error) {
	var positions []BlockPosition
	rows, err := store.Query("SELECT id, position, block_id FROM block_positions WHERE backup_id = ?;", backupID)
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

func insertBlock(store *sql.DB, hash string) (*Block, error) {
	block, err := findBlock(store, hash)
	if err != nil {
		return nil, err
	}

	if block != nil {
		return block, nil
	}
	// Write the block to the database
	insertSQL := `INSERT INTO blocks (hash) VALUES (?) ON CONFLICT DO NOTHING;`
	res, err := store.Exec(insertSQL, hash)
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

func findBlock(store *sql.DB, hash string) (*Block, error) {
	var id int
	var createdAt time.Time
	row := store.QueryRow("SELECT id, created_at FROM blocks WHERE hash = ?", hash)
	err := row.Scan(&id, &createdAt)
	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	}

	return &Block{id: id, hash: hash, createdAt: createdAt}, nil
}

func findBlockAtPosition(store *sql.DB, backupID int, pos int) (*Block, error) {
	var hash string
	row := store.QueryRow("SELECT hash FROM blocks b JOIN block_positions bp ON bp.block_id = b.id WHERE bp.backup_id = ? AND bp.position = ?", backupID, pos)
	err := row.Scan(&hash)
	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	}

	return &Block{hash: hash}, nil
}

// func writeDigest(store *sql.DB, fileName string, totalChunks int, chunkSize int, digest Digest) error {
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

// 	_, err = store.Exec(insertSQL, fileName, totalChunks, chunkSize, entriesJSON, digest.fullDigest, positions)
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
// 	var totalChunks int
// 	var chunkSize int
// 	row := store.QueryRow("SELECT id, file_name, total_chunks, chunk_size, entries, zero_byte_positions FROM digests where full_digest = true ORDER BY id DESC LIMIT 1")
// 	if err := row.Scan(&id, &fileName, &totalChunks, &chunkSize, &entriesJSON, &zeroBytePositions); err != nil {
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
// 	digest.chunkSize = chunkSize
// 	digest.totalChunks = totalChunks

// 	return digest, nil
// }

// func findDigestByID(store *sql.DB, id int) (Digest, error) {
// 	var fileName string
// 	var totalChunks int
// 	var chunkSize int
// 	var fullDigest bool
// 	var entriesJSON string
// 	var zeroBytePositions string
// 	row := store.QueryRow("SELECT file_name, total_chunks, chunk_size, full_digest, entries, zero_byte_positions FROM digests WHERE id = ?", id)
// 	if err := row.Scan(&fileName, &totalChunks, &chunkSize, &fullDigest, &entriesJSON, &zeroBytePositions); err != nil {
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
// 	digest.chunkSize = chunkSize
// 	digest.totalChunks = totalChunks
// 	digest.fileName = fileName

// 	return digest, nil
// }
