package block

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func setup(store *sql.DB) {
	if err := setupDigestDB(store); err != nil {
		panic(err)
	}

	if err := os.MkdirAll("backups", 0755); err != nil {
		panic(err)
	}

	if err := os.MkdirAll("restores", 0755); err != nil {
		panic(err)
	}

}

func cleanup(t *testing.T) {
	if err := os.RemoveAll("backups/"); err != nil {
		t.Log(err)
	}

	if err := os.RemoveAll("restores/"); err != nil {
		t.Log(err)
	}

	if err := os.Remove("digests.db"); err != nil {
		t.Log(err)
	}
}

func TestFullBackup(t *testing.T) {
	// Setup sqlite connection
	store, err := sql.Open("sqlite3", "digests.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	setup(store)
	defer cleanup(t)

	fs := NewFilesystem("pg.ext4")

	digest, err := Backup(fs, store)
	if err != nil {
		t.Fatal(err)
	}

	if digest.fileName != "pg.ext4" {
		t.Errorf("expected file name to be pg.ext4, got %s", digest.fileName)
	}

	if digest.totalChunks != 50 {
		t.Errorf("expected total chunks to be 50, got %d", digest.totalChunks)
	}

	if digest.chunkSize != 1048576 {
		t.Fatalf("expected chunk size to be 1048576, got %d", digest.chunkSize)
	}

	if len(digest.entries) != 36 {
		t.Fatalf("expected total entries to be 36, got %d", len(digest.entries))
	}

	if len(digest.emptyPositions) != 14 {
		t.Errorf("expected 14 empty positions, got %d", len(digest.emptyPositions))
	}
}

func TestDifferentialBackup(t *testing.T) {
	// Setup sqlite connection
	store, err := sql.Open("sqlite3", "digests.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	setup(store)
	defer cleanup(t)

	fs := NewFilesystem("pg.ext4")

	// Perform a full backup
	_, err = Backup(fs, store)
	if err != nil {
		t.Fatal(err)
	}

	// Perform a differential backup
	digest, err := Backup(fs, store)
	if err != nil {
		t.Fatal(err)
	}

	if digest.totalChunks != 50 {
		t.Errorf("expected total chunks to be 50, got %d", digest.totalChunks)
	}

	if digest.chunkSize != 1048576 {
		t.Fatalf("expected chunk size to be 1048576, got %d", digest.chunkSize)
	}

	if len(digest.entries) != 0 {
		t.Fatalf("expected total entries to be 0, got %d", len(digest.entries))
	}
}

func TestDifferentialBackupWithChanges(t *testing.T) {
	// Setup sqlite connection
	store, err := sql.Open("sqlite3", "digests.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	setup(store)
	defer cleanup(t)

	fs := NewFilesystem("pg.ext4")

	// Perform a full backup
	_, err = Backup(fs, store)
	if err != nil {
		t.Fatal(err)
	}

	// Target the altered filesystem
	fs = NewFilesystem("pg_altered.ext4")

	// Perform a differential backup
	digest, err := Backup(fs, store)
	if err != nil {
		t.Fatal(err)
	}

	// TODO - Determine if the differential backup should inheret the emptyPostions from the full backup.

	if digest.totalChunks != 50 {
		t.Errorf("expected total chunks to be 50, got %d", digest.totalChunks)
	}

	if digest.chunkSize != 1048576 {
		t.Fatalf("expected chunk size to be 1048576, got %d", digest.chunkSize)
	}

	if len(digest.entries) != 1 {
		t.Fatalf("expected total entries to be 1, got %d", len(digest.entries))
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

}

// func TestFullBackupExcludingZero(t *testing.T) {
// 	fs := NewFilesystem("assets/pg.ext4")

// 	digest, err := FileBackup(fs, true, true)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	if digest.totalChunks != 50 {
// 		t.Errorf("expected total chunks to be 50, got %d", digest.totalChunks)
// 	}

// 	if len(digest.entries) != 36 {
// 		t.Errorf("expected 36 entries, got %d", len(digest.entries))
// 	}

// 	if len(digest.emptyPositions) != 14 {
// 		t.Errorf("expected 14 empty positions, got %d", len(digest.emptyPositions))
// 	}

// 	filePath := fmt.Sprintf("digests/%s.digest", digest.fileName)

// 	digestFile, err := os.Open(filePath)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer digestFile.Close()

// 	scanner := bufio.NewScanner(digestFile)
// 	lineCount := 0
// 	for scanner.Scan() {
// 		lineCount++
// 	}

// 	if err := scanner.Err(); err != nil {
// 		log.Fatalf("error while reading the file: %v", err)
// 	}

// 	if lineCount != 36 {
// 		t.Errorf("expected 36 lines, got %d", lineCount)
// 	}

// 	if err := os.Remove(filePath); err != nil {
// 		t.Fatal(err)
// 	}
// }
