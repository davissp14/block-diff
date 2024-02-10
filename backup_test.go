package block

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func setup(store *sql.DB) {
	if err := setupDB(store); err != nil {
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
	if err := os.Remove("backups.db"); err != nil {
		t.Log(err)
	}
}

func TestFullBackup(t *testing.T) {
	// Setup sqlite connection
	store, err := sql.Open("sqlite3", "backups.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	setup(store)
	defer cleanup(t)

	vol, err := insertVolume(store, "pg.ext4", "assets/pg.ext4")
	if err != nil {
		t.Fatal(err)
	}

	backupRecord, err := Backup(store, &vol)
	if err != nil {
		t.Fatal(err)
	}

	if backupRecord.volumeID != vol.id {
		t.Errorf("expected volume id to be %d, got %d", vol.id, backupRecord.volumeID)
	}

	if backupRecord.backupType != "full" {
		t.Errorf("expected backup type to be full, got %s", backupRecord.backupType)
	}

	if backupRecord.totalChunks != 50 {
		t.Errorf("expected total chunks to be 50, got %d", backupRecord.totalChunks)
	}

	if backupRecord.chunkSize != 1048576 {
		t.Fatalf("expected chunk size to be 1048576, got %d", backupRecord.chunkSize)
	}

	positions, err := findBlockPositionsByBackup(store, backupRecord.id)
	if err != nil {
		t.Fatal(err)
	}

	if len(positions) != 50 {
		t.Fatalf("expected 50 positions, got %d", len(positions))
	}

	totalBlocks, err := queryTotalBlocks(store)
	if err != nil {
		t.Fatal(err)
	}

	if totalBlocks != 37 {
		t.Fatalf("expected 37 blocks, got %d", totalBlocks)
	}
}

func TestDifferentialBackup(t *testing.T) {
	// Setup sqlite connection
	store, err := sql.Open("sqlite3", "backups.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	setup(store)
	defer cleanup(t)

	vol, err := insertVolume(store, "pg.ext4", "assets/pg.ext4")
	if err != nil {
		t.Fatal(err)
	}

	_, err = Backup(store, &vol)
	if err != nil {
		t.Fatal(err)
	}

	backupRecord, err := Backup(store, &vol)
	if err != nil {
		t.Fatal(err)
	}

	if backupRecord.volumeID != vol.id {
		t.Errorf("expected volume id to be %d, got %d", vol.id, backupRecord.volumeID)
	}

	if backupRecord.backupType != "differential" {
		t.Errorf("expected backup type to be differential, got %s", backupRecord.backupType)
	}

	if backupRecord.totalChunks != 50 {
		t.Errorf("expected total chunks to be 50, got %d", backupRecord.totalChunks)
	}

	if backupRecord.chunkSize != 1048576 {
		t.Fatalf("expected chunk size to be 1048576, got %d", backupRecord.chunkSize)
	}
}

func TestDifferentialBackupWithChanges(t *testing.T) {
	// Setup sqlite connection
	store, err := sql.Open("sqlite3", "backups.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	setup(store)
	defer cleanup(t)

	vol, err := insertVolume(store, "pg.ext4", "assets/pg.ext4")
	if err != nil {
		t.Fatal(err)
	}

	_, err = Backup(store, &vol)
	if err != nil {
		t.Fatal(err)
	}

	vol.devicePath = "assets/pg_altered.ext4"

	differential, err := Backup(store, &vol)
	if err != nil {
		t.Fatal(err)
	}

	positions, err := findBlockPositionsByBackup(store, differential.id)
	if err != nil {
		t.Fatal(err)
	}

	if len(positions) != 1 {
		t.Fatalf("expected 1 position, got %d", len(positions))
	}
}
