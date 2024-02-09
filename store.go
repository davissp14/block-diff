package block

import (
	"database/sql"
	"encoding/json"
)

type Digest struct {
	id             int            `json: "id"`
	fileName       string         `json: "file_name"`
	totalChunks    int            `json: "total_chunks"`
	chunkSize      int            `json: "chunk_size"`
	fullDigest     bool           `json: "full_digest"`
	emptyPositions []int          `json: "empty"`
	entries        map[int]string `json: "entries"`
}

func setupDigestDB(store *sql.DB) error {
	// Create the 'digests' table
	createTableSQL := `CREATE TABLE IF NOT EXISTS digests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
		total_chunks INTEGER NOT NULL,
		chunk_size INTEGER NOT NULL,
		file_name TEXT NOT NULL,
		full_digest BOOLEAN NOT NULL DEFAULT 0,
        entries JSON NOT NULL,
        zero_byte_positions TEXT NOT NULL
    );`
	_, err := store.Exec(createTableSQL)
	if err != nil {
		return err
	}

	return nil
}

func writeDigest(store *sql.DB, fileName string, totalChunks int, chunkSize int, digest Digest) error {
	// Write the digest to the database
	insertSQL := `INSERT INTO digests (file_name, total_chunks, chunk_size, entries, full_digest, zero_byte_positions) VALUES (?,?,?,?,?,?);`
	entriesJSON, err := json.Marshal(digest.entries)
	if err != nil {
		return err
	}

	positions, err := json.Marshal(digest.emptyPositions)
	if err != nil {
		return err
	}

	_, err = store.Exec(insertSQL, fileName, totalChunks, chunkSize, entriesJSON, digest.fullDigest, positions)
	if err != nil {
		return err
	}

	return nil
}

func lastFullDigest(store *sql.DB) (Digest, error) {
	var id int
	var fileName string
	var entriesJSON string
	var zeroBytePositions string
	var totalChunks int
	var chunkSize int
	row := store.QueryRow("SELECT id, file_name, total_chunks, chunk_size, entries, zero_byte_positions FROM digests where full_digest = true ORDER BY id DESC LIMIT 1")
	if err := row.Scan(&id, &fileName, &totalChunks, &chunkSize, &entriesJSON, &zeroBytePositions); err != nil {
		return Digest{}, err
	}

	var digest Digest
	if err := json.Unmarshal([]byte(entriesJSON), &digest.entries); err != nil {
		return Digest{}, err
	}
	if err := json.Unmarshal([]byte(zeroBytePositions), &digest.emptyPositions); err != nil {
		return Digest{}, err
	}

	digest.id = id
	digest.fileName = fileName
	digest.chunkSize = chunkSize
	digest.totalChunks = totalChunks

	return digest, nil
}

func findDigestByID(store *sql.DB, id int) (Digest, error) {
	var fileName string
	var totalChunks int
	var chunkSize int
	var fullDigest bool
	var entriesJSON string
	var zeroBytePositions string
	row := store.QueryRow("SELECT file_name, total_chunks, chunk_size, full_digest, entries, zero_byte_positions FROM digests WHERE id = ?", id)
	if err := row.Scan(&fileName, &totalChunks, &chunkSize, &fullDigest, &entriesJSON, &zeroBytePositions); err != nil {
		return Digest{}, err
	}

	var digest Digest
	if err := json.Unmarshal([]byte(entriesJSON), &digest.entries); err != nil {
		return Digest{}, err
	}
	if err := json.Unmarshal([]byte(zeroBytePositions), &digest.emptyPositions); err != nil {
		return Digest{}, err
	}

	digest.id = id
	digest.fullDigest = fullDigest
	digest.chunkSize = chunkSize
	digest.totalChunks = totalChunks
	digest.fileName = fileName

	return digest, nil
}
