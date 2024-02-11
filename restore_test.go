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

	vol, err := store.InsertVolume("pg.ext4", "assets/pg.ext4")
	if err != nil {
		t.Fatal(err)
	}

	// Perform full backup
	backup, err := Backup(store, &vol, "backups/")
	if err != nil {
		t.Fatal(err)
	}

	// Perform full restore
	if err := Restore(store, backup); err != nil {
		t.Fatal(err)
	}

	// Compare the original file with the restored file
	sourceChecksum, err := fileChecksum(vol.DevicePath)
	if err != nil {
		t.Fatal(err)
	}

	restoreFilePath := fmt.Sprintf("%s/%s", restoreDirectory, backup.FileName+".restore")
	targetChecksum, err := fileChecksum(restoreFilePath)
	if err != nil {
		t.Fatal(err)
	}

	if sourceChecksum != targetChecksum {
		t.Fatalf("expected checksums to match, got %s and %s", sourceChecksum, targetChecksum)
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

	vol, err := store.InsertVolume("pg.ext4", "assets/pg.ext4")
	if err != nil {
		t.Fatal(err)
	}

	// Perform full backup
	if _, err = Backup(store, &vol, "backups/"); err != nil {
		t.Fatal(err)
	}

	// Perform a differential backup
	vol.DevicePath = "assets/pg_altered.ext4"

	backup, err := Backup(store, &vol, "backups/")
	if err != nil {
		t.Fatal(err)
	}

	// Confirm that the differential backup resulted in a block change.
	positions, err := store.findBlockPositionsByBackup(backup.Id)
	if err != nil {
		t.Fatal(err)
	}

	if len(positions) != 1 {
		t.Fatalf("expected 1 block position, got %d", len(positions))
	}

	// Perform a full restore
	if err := Restore(store, backup); err != nil {
		t.Fatal(err)
	}

	// Compare the original file with the restored file
	sourceChecksum, err := fileChecksum(vol.DevicePath)
	if err != nil {
		t.Fatal(err)
	}

	restoreFilePath := fmt.Sprintf("%s/%s", restoreDirectory, backup.FileName+".restore")
	targetChecksum, err := fileChecksum(restoreFilePath)
	if err != nil {
		t.Fatal(err)
	}

	if sourceChecksum != targetChecksum {
		t.Fatalf("expected checksums to match, got %s and %s", sourceChecksum, targetChecksum)
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
