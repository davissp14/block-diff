package block

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestFullRestore(t *testing.T) {
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
		BlockBufferSize: 1,
	}

	b, err := NewBackup(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Run(); err != nil {
		t.Fatal(err)
	}

	compareChecksum(t, b.vol.DevicePath, fullBackupChecksum)

	restoreConfig := RestoreConfig{
		Store:              store,
		RestoreInputFormat: RestoreInputFormatFile,
		SourceBackupID:     b.Record.ID,
		OutputDirectory:    "restores/",
		OutputFileName:     b.Record.FileName,
	}

	restore, err := NewRestore(restoreConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Perform full restore
	if err := restore.Run(); err != nil {
		t.Fatal(err)
	}

	// Confirm that the differential backup resulted in a block change.
	positions, err := store.findBlockPositionsByBackup(b.Record.ID)
	if err != nil {
		t.Fatal(err)
	}

	if len(positions) != 50 {
		t.Fatalf("expected 50 block position, got %d", len(positions))
	}

	targetChecksum, err := fileChecksum(restore.FullRestorePath())
	if err != nil {
		t.Fatal(err)
	}

	if fullBackupChecksum != targetChecksum {
		t.Fatalf("expected checksums to match, got %s and %s", fullBackupChecksum, targetChecksum)
	}
}

func TestFullRestoreFromDifferential(t *testing.T) {
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
		OutputDirectory: "backups",
		BlockSize:       1048576,
		BlockBufferSize: 7,
	}

	b, err := NewBackup(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Run(); err != nil {
		t.Fatal(err)
	}

	// Confirm that the differential backup resulted in a block change.
	positions, err := store.findBlockPositionsByBackup(b.Record.ID)
	if err != nil {
		t.Fatal(err)
	}

	if len(positions) != 50 {
		t.Fatalf("expected 1 block position, got %d", len(positions))
	}

	// for _, p := range positions {
	// 	// fmt.Printf("block position: %d, block_id: %d\n", p.position, p.blockID)
	// }
	compareChecksum(t, b.vol.DevicePath, fullBackupChecksum)

	db, err := NewBackup(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Hack the device path to simulate a change
	db.vol.DevicePath = "assets/pg_altered.ext4"

	if err := db.Run(); err != nil {
		t.Fatal(err)
	}

	compareChecksum(t, db.vol.DevicePath, diffWithChangesChecksum)

	// Confirm that the differential backup resulted in a block change.
	positions, err = store.findBlockPositionsByBackup(db.Record.ID)
	if err != nil {
		t.Fatal(err)
	}

	if len(positions) != 1 {
		t.Fatalf("expected 1 block position, got %d", len(positions))
	}

	restoreConfig := RestoreConfig{
		Store:              store,
		RestoreInputFormat: RestoreInputFormatFile,
		SourceBackupID:     db.Record.ID,
		OutputDirectory:    "restores",
		OutputFileName:     db.Record.FileName,
	}

	restore, err := NewRestore(restoreConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Perform full restore
	if err := restore.Run(); err != nil {
		t.Fatal(err)
	}

	// Compare the original file with the restored file

	targetChecksum, err := fileChecksum(restore.FullRestorePath())
	if err != nil {
		t.Fatal(err)
	}

	if diffWithChangesChecksum != targetChecksum {
		t.Fatalf("expected checksums to match, got %s and %s", diffWithChangesChecksum, targetChecksum)
	}
}

func fileChecksum(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}
