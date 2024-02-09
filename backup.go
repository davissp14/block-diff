package block

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

const (
	restoreDirectory = "restores"
	backupDirectory  = "backups"
	zeroByteHash     = "30e14955ebf1352266dc2ff8067e68104607e750abb9d3b36582b8af909fcb58"
)

func Backup(fs *FS, store *sql.DB) (Digest, error) {
	// Open up filesystem device
	dev, err := os.Open(fs.filePath)
	if err != nil {
		return Digest{}, err
	}
	defer dev.Close()

	var digest Digest

	// Discover the last full digest if it exists
	lastDigest, err := lastFullDigest(store)
	switch {
	case err == sql.ErrNoRows:
		digest.fullDigest = true // Create a full digest
	case err != nil:
		return Digest{}, err
	}

	// Create the backup file
	sourceNameSlice := strings.Split(fs.filePath, "/")
	sourceName := sourceNameSlice[len(sourceNameSlice)-1]
	backupFileName := calculateBackupName(sourceName, &digest)
	backupFilePath := fmt.Sprintf("%s/%s", backupDirectory, backupFileName)

	backupFile, err := os.OpenFile(backupFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return Digest{}, fmt.Errorf("error opening backup file: %v", err)
	}
	defer backupFile.Close()

	digest.fileName = sourceName
	digest.entries = make(map[int]string)
	digest.chunkSize = fs.chunkSize
	digest.totalChunks = fs.totalChunks

	for chunkNum := 0; chunkNum < fs.totalChunks; chunkNum++ {
		blockData, err := readBlock(dev, fs.chunkSize, chunkNum)
		if err != nil {
			return Digest{}, err
		}

		hash := calculateBlockHash(blockData)

		// If the hash is all zeros, add it to the empty positions
		// Note: This will reduce the size of the digest and we track the numbers
		// for restore purposes.
		if hash == zeroByteHash {
			digest.emptyPositions = append(digest.emptyPositions, chunkNum)
			continue
		}

		// If the hash is the same as the last digest, skip it
		if !digest.fullDigest && lastDigest.entries[chunkNum] == hash {
			continue
		}

		// Write blockdata to the backup file
		_, err = backupFile.Write(blockData)
		if err != nil {
			return Digest{}, fmt.Errorf("error writing to backup file: %v", err)
		}

		// Add the hash to the digest
		digest.entries[chunkNum] = hash
	}

	if err := writeDigest(store, backupFileName, fs.totalChunks, fs.chunkSize, digest); err != nil {
		return Digest{}, err
	}

	return digest, nil
}

func dumpHashesToFile(fs *FS, sourcePath, targetPath string) (Digest, error) {
	// Open up filesystem device
	dev, err := os.Open(sourcePath)
	if err != nil {
		return Digest{}, err
	}
	defer dev.Close()

	targetFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return Digest{}, fmt.Errorf("error opening backup file: %v", err)
	}
	defer targetFile.Close()

	// digestFileName := fmt.Sprintf("digests/%s.digest", fileName)
	// digestFile, err := os.OpenFile(digestFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	// if err != nil {
	// 	return Digest{}, fmt.Errorf("error opening backup file: %v", err)
	// }
	// defer digestFile.Close()

	// digest.fileName = fileName
	// digest.entries = make(map[int]string)
	// digest.fullDigest = fullDigest
	// digest.chunkSize = fs.chunkSize
	// digest.totalChunks = fs.totalChunks

	var digest Digest
	digest.entries = make(map[int]string)

	for chunkNum := 0; chunkNum < fs.totalChunks; chunkNum++ {
		blockData, err := readBlock(dev, fs.chunkSize, chunkNum)
		if err != nil {
			return Digest{}, err
		}

		hash := calculateBlockHash(blockData)

		entry := fmt.Sprintf("%d: %s\n", chunkNum, hash)

		targetFile.Write([]byte(entry))
		// If the hash is all zeros, add it to the empty positions
		// Note: This will reduce the size of the digest and we track the numbers
		// for restore purposes.
		// if excludeZeros && hash == zeroByteHash {
		// 	digest.emptyPositions = append(digest.emptyPositions, chunkNum)
		// 	continue
		// }

		// If the hash is the same as the last digest, skip it
		// if !digest.fullDigest && lastDigest.entries[chunkNum] == hash {
		// 	continue
		// }
		// entry := fmt.Sprintf("%d: %s\n", chunkNum, hash)
		// _, err = digestFile.Write([]byte(entry))
		// if err != nil {
		// 	return Digest{}, fmt.Errorf("error writing to digest file: %v", err)
		// }

		// Write blockdata to the backup file
		// _, err = backupFile.Write(blockData)
		// if err != nil {
		// 	return Digest{}, fmt.Errorf("error writing to backup file: %v", err)
		// }

		// Add the hash to the digest
		digest.entries[chunkNum] = hash
	}

	return digest, nil
	// return writeDigest(store, backupFileName, fs.totalChunks, fs.chunkSize, digest)
}

func calculateBackupName(sourceFileName string, d *Digest) string {
	if d.fullDigest {
		return fmt.Sprintf("%s_%d.full", sourceFileName, d.id)
	} else {
		// Random number for partial backup
		rand := rand.Intn(100000)
		return fmt.Sprintf("%s_%d_%d.partial", sourceFileName, d.id, rand)
	}
}

func calculateBlockHash(blockData []byte) string {
	hash := sha256.Sum256(blockData)
	return hex.EncodeToString(hash[:])
}
