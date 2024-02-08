package block

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	ext4 "github.com/davissp14/go-ext4"
	log "github.com/dsoprea/go-logging"
)

const (
	superblock0Offset = 1024
	zeroByteHash      = "30e14955ebf1352266dc2ff8067e68104607e750abb9d3b36582b8af909fcb58"
)

type FS struct {
	filePath    string
	blockCount  int
	blockSize   int
	chunkSize   int
	totalChunks int
}

func NewFilesystem(devicePath string) *FS {
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

func (fs *FS) WriteDigest(digestFilePath string) error {
	f, err := os.Open(fs.filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	backupFile, err := os.OpenFile(digestFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening backup file: %v", err)
	}
	defer backupFile.Close()

	for chunkNum := 0; chunkNum < fs.totalChunks; chunkNum++ {
		blockData, err := readBlock(f, fs.chunkSize, chunkNum)
		if err != nil {
			return err
		}

		hash := calculateBlockHash(blockData)
		if hash == zeroByteHash {
			continue
		}
		data := fmt.Sprintf("%d: %s\n", chunkNum, hash)
		// You might want to prepend each block with its number or metadata
		_, err = backupFile.Write([]byte(data))
		if err != nil {
			return err
		}
	}

	return nil
}

func (fs *FS) Backup(outputFilePath string) error {
	f, err := os.Open(fs.filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	backupFile, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening backup file: %v", err)
	}
	defer backupFile.Close()

	for chunkNum := 0; chunkNum < fs.totalChunks; chunkNum++ {
		blockData, err := readBlock(f, fs.chunkSize, chunkNum)
		if err != nil {
			return err
		}
		_, err = backupFile.Write(blockData)
		if err != nil {
			return err
		}
	}

	return nil
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

func calculateBlockHash(blockData []byte) string {
	hash := sha256.Sum256(blockData)
	return hex.EncodeToString(hash[:])
}
