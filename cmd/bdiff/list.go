package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/davissp14/block-diff"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

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
