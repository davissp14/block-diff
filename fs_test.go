package block

import (
	"testing"
)

func TestFSInitialization(t *testing.T) {
	fs := NewFilesystem("./assets/pg.ext4")
	if fs.blockCount != 12800 {
		t.Errorf("expected block count to be 12800, got %d", fs.blockCount)
	}
	if fs.blockSize != 4096 {
		t.Errorf("expected block size to be 4096, got %d", fs.blockSize)
	}
	if fs.chunkSize != 1048576 {
		t.Errorf("expected chunk size to be 1048576, got %d", fs.chunkSize)
	}
	if fs.totalChunks != 50 {
		t.Errorf("expected total chunks to be 50, got %d", fs.totalChunks)
	}
}

func TestWriteDigest(t *testing.T) {
	fs := NewFilesystem("assets/pg_altered.ext4")

	if err := fs.WriteDigest("digests/pg.ext4.digest"); err != nil {
		t.Fatal(err)
	}

	if err := fs.Backup("backups/pg_backup.ext4"); err != nil {
		t.Fatal("error backing up file:", err)
	}
}
