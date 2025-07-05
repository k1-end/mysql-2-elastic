package syncer

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

func GetMainBinlogPositionFilePath() (string) {
   return "data/main-binlog-position.json" 
}

func GetTableBinlogPositionFilePath(tableName string) (string) {
   return "data/dumps/" + tableName + "/" + tableName + "-dump-binlog-position.json" 
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
