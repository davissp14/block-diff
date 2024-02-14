package main

import (
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/davissp14/block-diff"
	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:   "create <path-to-device>",
	Short: "Performs a backup operation",
	Long:  `Performs a backup operation on the specified device.`,
	Args:  cobra.ExactArgs(1), // This ensures exactly one argument is passed

	Run: func(cmd *cobra.Command, args []string) {
		devicePath := args[0]
		stderr := os.Stderr

		// Extract the output flag value
		outputDirPath, err := cmd.Flags().GetString("output-dir")
		if err != nil || outputDirPath == "" {
			fmt.Fprintln(stderr, "No output directory specified. Saving backup file to current directory.")
			outputDirPath = "."
		}

		outputFormat, err := cmd.Flags().GetString("output-format")
		if err != nil {
			fmt.Fprintln(stderr, "Error getting output-format flag")
		}

		blockSize, err := cmd.Flags().GetInt("block-size")
		if err != nil {
			fmt.Fprintln(stderr, "Error getting block-size flag")
		}

		blockBufferSize, err := cmd.Flags().GetInt("block-buffer-size")
		if err != nil {
			fmt.Fprintln(stderr, "Error getting block-buffer-size flag")
		}

		enablePprof, err := cmd.Flags().GetBool("enable-pprof")
		if err != nil {
			fmt.Fprintln(stderr, "Error getting pprof flag")
		}

		wg := &sync.WaitGroup{}
		if enablePprof {
			fmt.Fprintln(stderr, "Starting pprof server on port 6060")
			wg.Add(1)
			go func() {
				if err := http.ListenAndServe("localhost:6060", nil); err != nil {
					fmt.Fprintln(stderr, err)
					return
				}
			}()
		}

		if err := performBackup(devicePath, outputDirPath, outputFormat, blockSize, blockBufferSize); err != nil {
			fmt.Fprintln(stderr, err)
		}

		if enablePprof {
			fmt.Fprintf(stderr, "Backup completed. Pprof server is still running on port 6060. Ctrl+C to stop")
			wg.Wait()
		}
	},
}

// performBackup is a placeholder for your backup logic.
func performBackup(devicePath, outputDir, outputFormat string, blockSize int, bufferBlockSize int) error {
	store, err := block.NewStore()
	if err != nil {
		return fmt.Errorf("error creating store: %v", err)
	}

	if err := store.SetupDB(); err != nil {
		return fmt.Errorf("error setting up database: %v", err)
	}

	cfg := &block.BackupConfig{
		Store:           store,
		DevicePath:      devicePath,
		OutputFormat:    block.BackupOutputFormat(outputFormat),
		OutputDirectory: outputDir,
		BlockSize:       blockSize,
		BlockBufferSize: bufferBlockSize,
	}

	fmt.Fprintf(os.Stderr, "Performing backup of %s to %s\n", devicePath, outputDir)

	b, err := block.NewBackup(cfg)
	if err != nil {
		return fmt.Errorf("error creating backup: %v", err)
	}

	if err := b.Run(); err != nil {
		return fmt.Errorf("error performing backup: %v", err)
	}

	if cfg.OutputFormat == block.BackupOutputFormatFile {
		uniqueBlocks, err := store.UniqueBlocksInBackup(b.Record.ID)
		if err != nil {
			return fmt.Errorf("error getting unique blocks: %v", err)
		}

		sourceSizeInBytes, err := block.GetTargetSizeInBytes(devicePath)
		if err != nil {
			return fmt.Errorf("error getting device size: %v", err)
		}

		sizeDiff := int(sourceSizeInBytes - b.Record.SizeInBytes)

		fmt.Println("Backup completed successfully!")
		fmt.Println("=============Info=================")
		fmt.Printf("Backup file: %s/%s\n", outputDir, b.Record.FileName)
		fmt.Printf("Backup size %s\n", formatFileSize(float64(b.Record.SizeInBytes)))
		fmt.Printf("Source device size: %s\n", formatFileSize(float64(sourceSizeInBytes)))
		fmt.Printf("Space saved: %s\n", formatFileSize(float64(sizeDiff)))
		fmt.Printf("Blocks evaluated: %d\n", b.TotalBlocks())
		fmt.Printf("Blocks written: %d\n", uniqueBlocks)
		fmt.Println("==================================")
	}

	return nil
}
