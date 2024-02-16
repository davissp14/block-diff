package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/davissp14/block-diff"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	"net/http"
	_ "net/http/pprof"
)

// main is the entry point for the application.
func main() {
	var rootCmd = &cobra.Command{Use: "bd"}
	var backupCmd = &cobra.Command{Use: "backup"}
	rootCmd.AddCommand(backupCmd)
	backupCmd.AddCommand(createCmd)
	backupCmd.AddCommand(listCmd)
	backupCmd.AddCommand(restoreCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	// Define flags for the createCmd
	createCmd.Flags().BoolP("enable-pprof", "p", false, "Enable pprof")
	createCmd.Flags().StringP("output-dir", "o", "", "Output file path. This is ignored if stdout is specified. (default is current directory)")
	createCmd.Flags().StringP("output-filename", "f", "", "Output file name.")
	createCmd.Flags().StringP("output-format", "", "file", "Output format. (file [default], stdout)")
	createCmd.Flags().IntP("block-size", "b", 4096, "The number of bytes to read at a time")
	createCmd.Flags().IntP("block-buffer-size", "", 5, "The number of blocks to buffer before writing to disk")

	// Define flags for the restoreCmd
	restoreCmd.Flags().BoolP("enable-pprof", "p", false, "Enable pprof")
	restoreCmd.Flags().StringP("output-dir", "o", "", "Output file path. This is ignored if stdout is specified. (default is current directory)")
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists all backups",
	Long:  `Lists all available backups created.`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := listBackups(); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	},
}

func listBackups() error {
	store, err := block.NewStore()
	if err != nil {
		return fmt.Errorf("error creating store: %v", err)
	}

	backups, err := store.ListBackups()
	if err != nil {
		return fmt.Errorf("error getting backups: %v", err)
	}

	if len(backups) == 0 {
		fmt.Println("No backups found")
		return nil
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"ID", "Type", "Block size", "Total Blocks", "Size", "Created At"})

	// Set table alignment, borders, padding, etc. as needed
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetBorder(true) // Set to false to hide borders
	table.SetCenterSeparator("|")
	table.SetColumnSeparator("|")
	table.SetRowSeparator("-")
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderLine(true) // Enable header line
	table.SetAutoWrapText(false)

	for _, b := range backups {
		table.Append([]string{
			strconv.Itoa(b.ID),
			strings.ToUpper(b.BackupType),
			fmt.Sprint(b.BlockSize),
			fmt.Sprint(b.TotalBlocks),
			fmt.Sprint(formatFileSize(float64(b.SizeInBytes))),
			b.FullPath,
			b.CreatedAt.String(),
		})
	}

	table.Render()

	return nil
}

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

	backupStartTime := time.Now()
	if err := b.Run(); err != nil {
		return fmt.Errorf("error performing backup: %v", err)
	}
	backupDuration := time.Since(backupStartTime)

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
		fmt.Printf("Backup Duration: %s\n", backupDuration)
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
		return fmt.Sprintf("%.1f %s", size, sizes[i])
	}
	return fmt.Sprintf("%.1f %s", size, sizes[i])
}
