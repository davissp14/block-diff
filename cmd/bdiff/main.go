package main

import (
	"fmt"
	"math"
	"os"

	"github.com/spf13/cobra"

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
