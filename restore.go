package block

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
)

func Restore(store *sql.DB, id int) (*os.File, error) {
	// Query the database for the target digest
	digest, err := findDigestByID(store, id)
	if err != nil {
		return nil, fmt.Errorf("error finding digest by id: %v", err)
	}

	restoreSourcePath := fmt.Sprintf("%s/%s", backupDirectory, digest.fileName)
	restoreFileName := fmt.Sprintf("%s.restore", digest.fileName)
	restorePath := fmt.Sprintf("%s/%s", restoreDirectory, restoreFileName)

	// Create the restore file
	restoreTarget, err := os.OpenFile(restorePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening restore file: %v", err)
	}
	defer restoreTarget.Close()

	if digest.fullDigest {
		// Open the file and stream the blocks to the restore file while filling in the empty chunks with zeros.
		restoreSource, err := os.Open(restoreSourcePath)
		if err != nil {
			return nil, fmt.Errorf("error opening restore source file: %v", err)
		}
		defer restoreSource.Close()

		// Restore the full digest
		err = restoreFullDigest(restoreSource, restoreTarget, digest)
		if err != nil {
			return nil, fmt.Errorf("error restoring full digest: %v", err)
		}
	} else {
		fullDigest, err := lastFullDigest(store)
		if err != nil {
			return nil, fmt.Errorf("error finding last full digest: %v", err)
		}

		// Open the file that holds last full backup.
		restoreFullSourcePath := fmt.Sprintf("%s/%s", backupDirectory, fullDigest.fileName)
		restoreFullSource, err := os.Open(restoreFullSourcePath)
		if err != nil {
			return nil, fmt.Errorf("error opening full restore source file: %v", err)
		}
		defer restoreFullSource.Close()

		// Restore the last full backup
		err = restoreFullDigest(restoreFullSource, restoreTarget, fullDigest)
		if err != nil {
			return nil, fmt.Errorf("error restoring full digest: %v", err)
		}

		// Open the file that holds last full backup.
		restorePartialSourcePath := fmt.Sprintf("%s/%s", backupDirectory, digest.fileName)
		restorePartialSource, err := os.Open(restorePartialSourcePath)
		if err != nil {
			return nil, err
		}
		defer restoreFullSource.Close()

		err = restorePartialDigest(restorePartialSource, restoreTarget, digest)
		if err != nil {
			return nil, fmt.Errorf("error restoring partial digest: %v", err)
		}
	}

	return restoreTarget, nil
}

func restoreFullDigest(source *os.File, target *os.File, digest Digest) error {
	chunkNum := 0
	emptyChunk := 0
	for (emptyChunk + chunkNum) < digest.totalChunks {
		var blockData []byte
		var err error
		// If the digest position at chunkNum is an empty position, write a zero chunk.
		if digest.entries[(chunkNum+emptyChunk)] == "" {
			emptyChunk++
			blockData = make([]byte, digest.chunkSize)
		} else {
			blockData, err = readBlock(source, digest.chunkSize, chunkNum)
			if err != nil {
				return err
			}
			chunkNum++
		}

		// Write blockdata to the restore file
		_, err = target.Write(blockData)
		if err != nil {
			return fmt.Errorf("error writing to restore file: %v", err)
		}
	}

	return nil
}

func restorePartialDigest(source *os.File, target *os.File, digest Digest) error {
	// Loop through the partial digest entries and restore the blocks
	for chunkNum, _ := range digest.entries {
		// Read the block data from the source file
		blockData, err := readBlock(source, digest.chunkSize, chunkNum)
		if err != nil {
			return fmt.Errorf("error reading block data at chunk position %d: %v", chunkNum, err)
		}

		// Write the block data to the destination file at the chunk position.
		_, err = target.WriteAt(blockData, int64(chunkNum*digest.chunkSize))
		if err != nil {
			return fmt.Errorf("error writing block data to restore file: %v", err)
		}
	}

	return nil
}

func RestoreFile(store *sql.DB, id int) error {
	// Query the database for the target digest
	digest, err := findDigestByID(store, id)
	if err != nil {
		return err
	}

	if digest.fullDigest {
		// open the file and stream the blocks to the restore file while
		// filling in the empty positions with zeros.
		backup, err := os.Open(digest.fileName)
		if err != nil {
			return err
		}
		defer backup.Close()

		// Create the restore file
		fileSlice := strings.Split(digest.fileName, "/")
		restoreFileName := fileSlice[len(fileSlice)-1] + ".restore"

		restorePath := fmt.Sprintf("%s/%s", restoreDirectory, restoreFileName)
		// Create the restore file
		restoreFile, err := os.OpenFile(restorePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("error opening restore file: %v", err)
		}
		defer restoreFile.Close()

		chunkNum := 0
		emptyChunk := 0
		for (emptyChunk + chunkNum) < digest.totalChunks {
			var blockData []byte
			// If the digest position at chunkNum is an empty position, write a zero chunk.
			if digest.entries[(chunkNum+emptyChunk)] == "" {
				emptyChunk++
				blockData = []byte(fmt.Sprintf("%d: %s\n", (chunkNum + emptyChunk), zeroByteHash))
			} else {
				rawData, err := readBlock(backup, digest.chunkSize, chunkNum)
				if err != nil {
					return err
				}
				hash := calculateBlockHash(rawData)
				blockData = []byte(fmt.Sprintf("%d: %s\n", (emptyChunk + chunkNum), hash))

				fmt.Println("Chunk data: ", blockData)
				// Iterate to the next chunk
				chunkNum++
			}

			// Write blockdata to the restore file
			_, err = restoreFile.Write(blockData)
			if err != nil {
				return fmt.Errorf("error writing to restore file: %v", err)
			}
		}

	} else {
		// parentDigest, err := lastFullDigest(store)
		// if err != nil {
		// 	return err
		// }

	}
	// Determine whether the digest is a full digest or partial digest

	// If it's a full digest, we can just restore the blocks while filling in
	// the empty positions with zeros.

	// If it's a partial digest, we need to restore the full digest need to first restore the full digest and
	// then restore the partial digest.

	return nil
}
