package block

import (
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func setup(s *Store) {
	if err := s.SetupDB(); err != nil {
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
	if err := os.Remove("backups.db-shm"); err != nil {
		t.Log(err)
	}
	if err := os.Remove("backups.db-wal"); err != nil {
		t.Log(err)
	}
}

func TestFullBackup(t *testing.T) {
	// Setup sqlite connection
	store, err := NewStore()
	defer store.Close()

	setup(store)
	defer cleanup(t)

	cfg := &BackupConfig{
		Store:           store,
		DevicePath:      "assets/pg.ext4",
		OutputFormat:    BackupOutputFormatFile,
		OutputDirectory: "backups/",
		BlockSize:       1048576,
		BlockBufferSize: 10,
	}

	b, err := NewBackup(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Run(); err != nil {
		t.Fatal(err)
	}

	if b.vol.DevicePath != cfg.DevicePath {
		t.Errorf("expected device path to be %s, got %s", cfg.DevicePath, b.vol.DevicePath)
	}

	if b.BackupType() != backupTypeFull {
		t.Errorf("expected backup type to be full, got %s", b.BackupType())
	}

	if b.TotalBlocks() != 50 {
		t.Errorf("expected total chunks to be 50, got %d", b.TotalBlocks())
	}

	if b.Config.BlockSize != 1048576 {
		t.Fatalf("expected chunk size to be 1048576, got %d", b.Config.BlockSize)
	}

	positions, err := store.findBlockPositionsByBackup(b.Record.Id)
	if err != nil {
		t.Fatal(err)
	}

	if len(positions) != 50 {
		t.Fatalf("expected 50 positions, got %d", len(positions))
	}

	totalBlocks, err := store.TotalBlocks()
	if err != nil {
		t.Fatal(err)
	}

	if totalBlocks != 37 {
		t.Fatalf("expected 37 blocks, got %d", totalBlocks)
	}
}

func TestDifferentialBackup(t *testing.T) {
	// Setup sqlite connection
	store, err := NewStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	setup(store)
	defer cleanup(t)

	cfg := &BackupConfig{
		Store:           store,
		DevicePath:      "assets/pg.ext4",
		OutputFormat:    BackupOutputFormatFile,
		OutputDirectory: "backups/",
		BlockSize:       1048576,
		BlockBufferSize: 10,
	}

	b, err := NewBackup(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Run(); err != nil {
		t.Fatal(err)
	}

	db, err := NewBackup(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Run(); err != nil {
		t.Fatal(err)
	}

	if db.vol.DevicePath != cfg.DevicePath {
		t.Errorf("expected device path to be %s, got %s", cfg.DevicePath, db.vol.DevicePath)
	}

	if db.Record.BackupType != backupTypeDifferential {
		t.Errorf("expected backup type to be differential, got %s", db.Record.BackupType)
	}

	if db.Record.TotalChunks != 50 {
		t.Errorf("expected total chunks to be 50, got %d", db.Record.TotalChunks)
	}

	if db.Record.ChunkSize != 1048576 {
		t.Fatalf("expected chunk size to be 1048576, got %d", db.Record.ChunkSize)
	}
}

func TestDifferentialBackupWithChanges(t *testing.T) {
	// Setup sqlite connection
	store, err := NewStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	setup(store)
	defer cleanup(t)

	cfg := &BackupConfig{
		Store:           store,
		DevicePath:      "assets/pg.ext4",
		OutputFormat:    BackupOutputFormatFile,
		OutputDirectory: "backups/",
		BlockSize:       1048576,
		BlockBufferSize: 4,
	}

	b, err := NewBackup(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Run(); err != nil {
		t.Fatal(err)
	}

	totalBlocks, err := b.store.TotalBlocks()
	if err != nil {
		t.Fatal(err)
	}

	if totalBlocks != 37 {
		t.Fatalf("expected 37 blocks, got %d", totalBlocks)
	}

	db, err := NewBackup(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Hack the device path to simulate a change
	db.vol.DevicePath = "assets/pg_altered.ext4"

	if err := db.Run(); err != nil {
		t.Fatal(err)
	}

	positions, err := store.findBlockPositionsByBackup(db.Record.Id)
	if err != nil {
		t.Fatal(err)
	}

	if len(positions) != 1 {
		t.Fatalf("expected 1 position, got %d", len(positions))
	}
}
