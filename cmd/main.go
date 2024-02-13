package main

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/davissp14/block-diff"
	"github.com/spf13/cobra"

	_ "net/http/pprof"
)

// main is the entry point for the application.
func main() {
	var rootCmd = &cobra.Command{Use: "bd"}
	rootCmd.AddCommand(backupCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// backupCmd represents the backup command
var backupCmd = &cobra.Command{
	Use:   "backup <path-to-device> -output-dir <path-to-dir> -enable-pprof",
	Short: "Performs a backup operation",
	Long:  `Performs a backup operation on the specified device.`,
	Args:  cobra.ExactArgs(1), // This ensures exactly one argument is passed

	Run: func(cmd *cobra.Command, args []string) {
		devicePath := args[0]

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
			go func() {
				wg.Add(1)
				if err := http.ListenAndServe("localhost:6060", nil); err != nil {
					fmt.Println(err)
					return
				}
			}()
		}

		if err := performBackup(devicePath, outputDirPath); err != nil {
			fmt.Println(err)
		}

		if enablePprof {
			fmt.Println("Backup completed. Pprof server is still running on port 6060. Ctrl+C to stop")
			wg.Wait()
		}
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists all backups",
	Long:  `Lists all available backups created.`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := listBackups(); err != nil {
			fmt.Println(err)
		}
	},
}

func init() {
	// Define flags for the backupCmd
	backupCmd.Flags().StringP("output-dir", "o", "", "output file path")
	backupCmd.Flags().BoolP("enable-pprof", "p", false, "enable pprof")
}

// performBackup is a placeholder for your backup logic.
func performBackup(devicePath, outputPath string) error {
	slice := strings.Split(devicePath, "/")
	deviceName := slice[len(slice)-1]

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
		OutputFormat:    block.BackupOutputFormatFile,
		OutputFileName:  deviceName + ".backup",
		OutputDirectory: outputPath,
		BlockSize:       4096 * 256,
		BlockBufferSize: 10,
	}

	b, err := block.NewBackup(cfg)
	if err != nil {
		return fmt.Errorf("error creating backup: %v", err)
	}

	startTime := time.Now()
	if err := b.Run(); err != nil {
		return fmt.Errorf("error performing backup: %v", err)
	}
	endTime := time.Now()
	diff := endTime.Sub(startTime)

	uniqueBlocks, err := store.UniqueBlocksInBackup(b.Record.Id)
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
	fmt.Printf("Backup duration: %s\n", diff)
	fmt.Printf("Backup file: %s/%s\n", outputPath, b.Record.FileName)
	fmt.Printf("Backup size %s\n", formatFileSize(float64(b.Record.SizeInBytes)))
	fmt.Printf("Source device size: %s\n", formatFileSize(float64(sourceSizeInBytes)))
	fmt.Printf("Space saved: %s\n", formatFileSize(float64(sizeDiff)))
	fmt.Printf("Blocks evaluated: %d\n", b.TotalBlocks())
	fmt.Printf("Blocks written: %d\n", uniqueBlocks)
	fmt.Println("==================================")

	return nil
}

func listBackups() error {
	return nil
}

var sizes = []string{"B", "KiB", "MiB", "GiB", "TiB"}

func formatFileSize(size float64) string {
	unitLimit := len(sizes)
	base := 1024.0
	i := 0
	for size >= base && i < unitLimit {
		size = size / 1024
		i++
	}

	if i == 0 {
		return fmt.Sprintf("%.f%s", size, sizes[i])
	}

	if math.Mod(size, 1024) <= 1.0 {
		return fmt.Sprintf("%.1f%s", size, sizes[i])
	}

	return fmt.Sprintf("%.2f%s", size, sizes[i])
}
