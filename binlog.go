package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// BinlogPosition represents the structure of your JSON data
type BinlogPosition struct {
	Logfile string `json:"logfile"`
	Logpos  uint32  `json:"logpos"`
}

// CompareBinlogFiles reads two files containing binlog positions
// and returns which file path points to the newer position.
// It returns an empty string if they are the same, both empty,
// or an error occurs.
func CompareBinlogFiles(filePath1, filePath2 string) (string, error) {
	data1, err1 := os.ReadFile(filePath1)
	data2, err2 := os.ReadFile(filePath2)

	// Check for read errors (excluding file not exists for emptiness check)
	if err1 != nil && !os.IsNotExist(err1) {
		return "", fmt.Errorf("error reading file %s: %w", filePath1, err1)
	}
	if err2 != nil && !os.IsNotExist(err2) {
		return "", fmt.Errorf("error reading file %s: %w", filePath2, err2)
	}

	empty1 := len(strings.TrimSpace(string(data1))) == 0 || os.IsNotExist(err1)
	empty2 := len(strings.TrimSpace(string(data2))) == 0 || os.IsNotExist(err2)

	if empty1 && empty2 {
		return "", fmt.Errorf("both files are empty or do not exist")
	}
	if empty1 {
		// Check if data2 is valid JSON before declaring it newer
		var pos2 BinlogPosition
		if err := json.Unmarshal(data2, &pos2); err != nil {
			return "", fmt.Errorf("file %s is empty, but file %s contains invalid JSON: %w", filePath1, filePath2, err)
		}
		return filePath2, nil
	}
	if empty2 {
		// Check if data1 is valid JSON before declaring it newer
		var pos1 BinlogPosition
		if err := json.Unmarshal(data1, &pos1); err != nil {
			return "", fmt.Errorf("file %s is empty, but file %s contains invalid JSON: %w", filePath2, filePath1, err)
		}
		return filePath1, nil
	}

	var pos1 BinlogPosition
	if err := json.Unmarshal(data1, &pos1); err != nil {
		return "", fmt.Errorf("error unmarshalling JSON from %s: %w", filePath1, err)
	}

	var pos2 BinlogPosition
	if err := json.Unmarshal(data2, &pos2); err != nil {
		return "", fmt.Errorf("error unmarshalling JSON from %s: %w", filePath2, err)
	}

	// Compare Logfile names
	if pos1.Logfile > pos2.Logfile {
		return filePath1, nil
	}
	if pos2.Logfile > pos1.Logfile {
		return filePath2, nil
	}

	// If Logfile names are the same, compare Logpos
	if pos1.Logpos > pos2.Logpos {
		return filePath1, nil
	}
	if pos2.Logpos > pos1.Logpos {
		return filePath2, nil
	}

	return "", fmt.Errorf("binlog positions are identical") // Or handle as "same"
}

func GetMainBinlogPositionFilePath() (string) {
   return "data/main-binlog-position.json" 
}

func ParseBinlogCoordinatesFile(filePath string) (BinlogPosition, error) {
	data, err := os.ReadFile(filePath)

	// Check for read errors (excluding file not exists for emptiness check)
	if err != nil && !os.IsNotExist(err) {
        return BinlogPosition{}, fmt.Errorf("error reading file %s: %w", filePath, err)
	}

	empty := len(strings.TrimSpace(string(data))) == 0 || os.IsNotExist(err)

	if empty{
		return BinlogPosition{}, fmt.Errorf("file is empty or does not exist")
	}

	var pos BinlogPosition
	if err := json.Unmarshal(data, &pos); err != nil {
		return BinlogPosition{}, fmt.Errorf("error unmarshalling JSON from %s: %w", filePath, err)
	}
    return pos, nil
}
