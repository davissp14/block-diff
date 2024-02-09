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
	_, err = Restore(store, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Compare the original file with the restored file
	sourceChecksum, err := fileChecksum(fs.filePath)
	if err != nil {
		t.Fatal(err)
	}

	restoreFilePath := fmt.Sprintf("%s/%s", restoreDirectory, "pg.ext4_0.full.restore")
	targetChecksum, err := fileChecksum(restoreFilePath)
	if err != nil {
		t.Fatal(err)
	}

	if sourceChecksum != targetChecksum {
		t.Fatalf("expected checksums to match, got %s and %s", sourceChecksum, targetChecksum)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFullRestoreFromDifferential(t *testing.T) {
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

	alteredFS := NewFilesystem("pg_altered.ext4")
	// Perform a differential backup
	_, err = Backup(alteredFS, store)
	if err != nil {
		t.Fatal(err)
	}

	// Restore from the differential backup
	restoreTarget, err := Restore(store, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Compare the original file with the restored file
	sourceChecksum, err := fileChecksum(alteredFS.filePath)
	if err != nil {
		t.Fatal(err)
	}

	targetChecksum, err := fileChecksum(restoreTarget.Name())
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
