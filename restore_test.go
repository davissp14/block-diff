package block

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestFullRestore(t *testing.T) {
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

	// Perform full backup
	backup, err := Backup(store, &vol)
	if err != nil {
		t.Fatal(err)
	}

	// Perform full restore
	if err := Restore(store, backup); err != nil {
		t.Fatal(err)
	}

	// Compare the original file with the restored file
	sourceChecksum, err := fileChecksum(vol.devicePath)
	if err != nil {
		t.Fatal(err)
	}

	restoreFilePath := fmt.Sprintf("%s/%s", restoreDirectory, backup.fileName+".restore")
	targetChecksum, err := fileChecksum(restoreFilePath)
	if err != nil {
		t.Fatal(err)
	}

	if sourceChecksum != targetChecksum {
		t.Fatalf("expected checksums to match, got %s and %s", sourceChecksum, targetChecksum)
	}
}

func TestFullRestoreFromDifferential(t *testing.T) {
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

	// Perform full backup
	if _, err = Backup(store, &vol); err != nil {
		t.Fatal(err)
	}

	// Perform a differential backup
	vol.name = "pg_altered.ext4"

	backup, err := Backup(store, &vol)
	if err != nil {
		t.Fatal(err)
	}

	// Perform a full restore
	if err := Restore(store, backup); err != nil {
		t.Fatal(err)
	}

	// Compare the original file with the restored file
	sourceChecksum, err := fileChecksum(vol.devicePath)
	if err != nil {
		t.Fatal(err)
	}

	restoreFilePath := fmt.Sprintf("%s/%s", restoreDirectory, backup.fileName+".restore")
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
