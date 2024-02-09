package block

import (
	"database/sql"
	"fmt"
	"io"
	"os"

	ext4 "github.com/davissp14/go-ext4"
	log "github.com/dsoprea/go-logging"
)

const (
	superblock0Offset = 1024
)

type FS struct {
	store       *sql.DB
	filePath    string
	blockCount  int
	blockSize   int
	chunkSize   int
	totalChunks int
}

const AssetsPath = "assets"

func NewFilesystem(deviceName string) *FS {
	devicePath := fmt.Sprintf("%s/%s", AssetsPath, deviceName)
	f, err := os.Open(devicePath)
	if err != nil {
		log.Panic(err)
	}
	defer f.Close()

	_, err = f.Seek(superblock0Offset, io.SeekStart)
	if err != nil {
		log.Panic(err)
	}

	sb, err := ext4.NewSuperblockWithReader(f)
	if err != nil {
		log.Panic(err)
	}

	blockSize := int(sb.BlockSize())
	blockCount := int(sb.BlockCount())
	totalSize := blockCount * blockSize
	chunkSize := 256 * blockSize
	totalChunks := totalSize / chunkSize

	if int(totalSize)%chunkSize != 0 {
		totalChunks++
	}

	return &FS{
		filePath:    devicePath,
		blockCount:  blockCount,
		blockSize:   blockSize,
		chunkSize:   chunkSize,
		totalChunks: totalChunks,
	}
}

func readBlock(disk *os.File, chunkSize, chunkNum int) ([]byte, error) {
	buffer := make([]byte, chunkSize)
	_, err := disk.Seek(int64(chunkSize*chunkNum), 0)
	if err != nil {
		return nil, err
	}
	_, err = disk.Read(buffer)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}
