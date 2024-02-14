package main

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/davissp14/block-diff"
	"github.com/spf13/cobra"
)

var restoreCmd = &cobra.Command{
	Use:   "restore <backup-id> -output-dir <path-to-dir> -enable-pprof",
	Short: "Restores from a specified backup",
	Long:  `Restores from a specified backup.`,
	Args:  cobra.ExactArgs(1), // This ensures exactly one argument is passed

	Run: func(cmd *cobra.Command, args []string) {
		backupIDStr := args[0]
		// Convert the backupID to an int
		backupID, err := strconv.ParseInt(backupIDStr, 10, 64)
		if err != nil {
			fmt.Println("Invalid backup ID")
			return
		}

		// Extract the output flag value
		outputDirPath, err := cmd.Flags().GetString("output-dir")
		if err != nil || outputDirPath == "" {
			fmt.Println("No output directory specified. Saving backup file to current directory.")
			outputDirPath = "."
		}

		enablePprof, err := cmd.Flags().GetBool("enable-pprof")
		if err != nil {
			fmt.Println("Error getting pprof flag")
		}

		wg := &sync.WaitGroup{}
		if enablePprof {
			fmt.Println("Starting pprof server on port 6060")
			wg.Add(1)
			go func() {
				if err := http.ListenAndServe("localhost:6060", nil); err != nil {
					fmt.Println(err)
					return
				}
			}()
		}

		if err := performRestore(int(backupID), outputDirPath); err != nil {
			fmt.Println(err)
		}

		if enablePprof {
			fmt.Println("Backup completed. Pprof server is still running on port 6060. Ctrl+C to stop")
			wg.Wait()
		}
	},
}

func performRestore(backupID int, outputPath string) error {
	store, err := block.NewStore()
	if err != nil {
		return fmt.Errorf("error creating store: %v", err)
	}

	restoreConfig := block.RestoreConfig{
		Store:              store,
		RestoreInputFormat: block.RestoreInputFormatFile,
		SourceBackupID:     backupID,
		OutputDirectory:    outputPath,
		OutputFileName:     "restored.backup",
	}

	restore, err := block.NewRestore(restoreConfig)
	if err != nil {
		return fmt.Errorf("error creating restore: %v", err)
	}

	// Perform full restore
	if err := restore.Run(); err != nil {
		return fmt.Errorf("error performing restore: %v", err)
	}

	return nil
}
