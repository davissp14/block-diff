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

	vol, err := store.InsertVolume("pg.ext4", "assets/pg.ext4")
	if err != nil {
		t.Fatal(err)
	}

	b, err := NewBackup(store, &vol, "backups/")
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Run(); err != nil {
		t.Fatal(err)
	}

	if b.Record.VolumeID != vol.Id {
		t.Errorf("expected volume id to be %d, got %d", vol.Id, b.Record.VolumeID)
	}

	if b.Record.BackupType != "full" {
		t.Errorf("expected backup type to be full, got %s", b.Record.BackupType)
	}

	if b.TotalChunks() != 50 {
		t.Errorf("expected total chunks to be 50, got %d", b.TotalChunks())
	}

	if b.Record.ChunkSize != 1048576 {
		t.Fatalf("expected chunk size to be 1048576, got %d", b.Record.ChunkSize)
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

	vol, err := store.InsertVolume("pg.ext4", "assets/pg.ext4")
	if err != nil {
		t.Fatal(err)
	}

	b, err := NewBackup(store, &vol, "backups/")
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Run(); err != nil {
		t.Fatal(err)
	}

	db, err := NewBackup(store, &vol, "backups/")
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Run(); err != nil {
		t.Fatal(err)
	}

	if db.Record.VolumeID != vol.Id {
		t.Errorf("expected volume id to be %d, got %d", vol.Id, db.Record.VolumeID)
	}

	if db.Record.BackupType != "differential" {
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

	vol, err := store.InsertVolume("pg.ext4", "assets/pg.ext4")
	if err != nil {
		t.Fatal(err)
	}

	b, err := NewBackup(store, &vol, "backups/")
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Run(); err != nil {
		t.Fatal(err)
	}

	vol.DevicePath = "assets/pg_altered.ext4"

	db, err := NewBackup(store, &vol, "backups/")
	if err != nil {
		t.Fatal(err)
	}

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
