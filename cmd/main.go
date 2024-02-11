package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/davissp14/block-diff"
	"github.com/spf13/cobra"
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
	Use:   "backup <path-to-device> -output <path-to-backup-file>",
	Short: "Performs a backup operation",
	Long:  `Performs a backup operation on the specified device.`,
	Args:  cobra.ExactArgs(1), // This ensures exactly one argument is passed

	Run: func(cmd *cobra.Command, args []string) {
		devicePath := args[0]
		// Extract the output flag value
		outputPath, err := cmd.Flags().GetString("output")
		if err != nil || outputPath == "" {
			fmt.Println("No output path specified. Sending output to the backups directory.")
			outputPath = "backups/"
		}
		if err := performBackup(devicePath); err != nil {
			fmt.Println(err)
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
	backupCmd.Flags().StringP("output", "o", "", "output file path")
}

// performBackup is a placeholder for your backup logic.
func performBackup(devicePath string) error {
	slice := strings.Split(devicePath, "/")
	deviceName := slice[len(slice)-1]

	store, err := block.NewStore()
	if err != nil {
		return fmt.Errorf("error creating store: %v", err)
	}

	store.SetupDB()

	vol, err := store.InsertVolume(deviceName, devicePath)
	if err != nil {
		return fmt.Errorf("error inserting volume: %v", err)
	}

	fmt.Printf("Performing backup on device: %s\n", devicePath)

	startTime := time.Now()
	backupRecord, err := block.Backup(store, &vol)
	if err != nil {
		return fmt.Errorf("error performing backup: %v", err)
	}
	endTime := time.Now()
	//calculate the difference
	diff := endTime.Sub(startTime)

	uniqueBlocks, err := store.UniqueBlocksInBackup(backupRecord.Id)
	if err != nil {
		return fmt.Errorf("error getting unique blocks: %v", err)
	}

	deviceSize, err := block.GetBlockDeviceSize(devicePath)
	if err != nil {
		return fmt.Errorf("error getting device size: %v", err)
	}

	sizeDiff := (int(deviceSize) - backupRecord.SizeInBytes)

	fmt.Println("Backup completed successfully!")
	fmt.Println("=============Info=================")
	fmt.Printf("Backup duration: %s\n", diff)
	fmt.Printf("Backup file: %s\n", backupRecord.FileName)
	fmt.Printf("Backup size %d bytes\n", backupRecord.SizeInBytes)
	fmt.Printf("Blocks evaluated: %d\n", backupRecord.TotalChunks)
	fmt.Printf("Blocks written: %d\n", uniqueBlocks)
	fmt.Printf("Source device size: %d bytes\n", deviceSize)
	fmt.Printf("Space saved: %d bytes\n", sizeDiff)
	fmt.Println("==================================")

	return nil
}

func listBackups() error {
	return nil
}
