package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func isServerFree() bool {
	jsonFile, err := os.Open("data/status.json")
	if err != nil {
		MainLogger.Debug(err.Error())
		panic(err)
	}
	defer jsonFile.Close()
	byteValue, _ := io.ReadAll(jsonFile)

	var status map[string]interface{}
	json.Unmarshal(byteValue, &status)

	if status["status"] == "free" {
		return true
	}
	return false
}

func setServerStatus(status string) error {
	statusMap := make(map[string]string)
	statusMap["status"] = status
	jsonData, err := json.Marshal(statusMap)
	if err != nil {
		return err
	}
	err = os.WriteFile("data/status.json", jsonData, 0644)

	return nil
}

// readLastOffset reads the last saved byte offset from the progress file.
func readLastOffset(filePath string) int64 {
    data, err := os.ReadFile(filePath)
    if err != nil {
        // If file doesn't exist or other read error, start from beginning
        return 0
    }
    offset, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
    if err != nil {
        // If conversion error, start from beginning
        MainLogger.Error(fmt.Sprintf("Error parsing offset from %s: %v. Starting from beginning.\n", filePath, err))
		panic(err)
    }
    MainLogger.Debug(fmt.Sprintf("Resuming from offset: %d\n", offset))
    return offset
}

// writeCurrentOffset writes the current byte offset to the progress file.
func writeCurrentOffset(filePath string, offset int64) error {
    return os.WriteFile(filePath, []byte(strconv.FormatInt(offset, 10)), 0644)
}


