package block

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func GetTargetSizeInBytes(devicePath string) (int, error) {
	fileInfo, err := os.Stat(devicePath)
	if err != nil {
		return 0, fmt.Errorf("error getting file info: %v", err)
	}
	mode := fileInfo.Mode()

	totalSizeInBytes := fileInfo.Size()
	// Check to see if the file is a block device.
	if mode&os.ModeDevice != 0 && mode&os.ModeCharDevice == 0 {
		fmt.Println("Device is a block device")
		totalSizeInBytes, err = getBlockDeviceSize(devicePath)
		if err != nil {
			return 0, fmt.Errorf("error getting block device size: %v", err)
		}
	}
	return int(totalSizeInBytes), nil
}

func getBlockDeviceSize(devicePath string) (int64, error) {
	cmd := exec.Command("blockdev", "--getsize64", devicePath)
	result, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(strings.TrimSpace(string(result)), 10, 64)
}
