package block

import (
	"os/exec"
	"strconv"
	"strings"
)

func GetBlockDeviceSize(devicePath string) (int64, error) {
	cmd := exec.Command("blockdev", "--getsize64", devicePath)
	result, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(strings.TrimSpace(string(result)), 10, 64)
}
