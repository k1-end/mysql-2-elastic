package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	syncerpack "github.com/k1-end/mysql-elastic-go/internal/syncer"
)



func ParseBinlogCoordinatesFile(filePath string) (syncerpack.BinlogPosition, error) {
	data, err := os.ReadFile(filePath)

	// Check for read errors (excluding file not exists for emptiness check)
	if err != nil && !os.IsNotExist(err) {
        return syncerpack.BinlogPosition{}, fmt.Errorf("error reading file %s: %w", filePath, err)
	}

	empty := len(strings.TrimSpace(string(data))) == 0 || os.IsNotExist(err)

	if empty{
		return syncerpack.BinlogPosition{}, fmt.Errorf("file is empty or does not exist")
	}

	var pos syncerpack.BinlogPosition
	if err := json.Unmarshal(data, &pos); err != nil {
		return syncerpack.BinlogPosition{}, fmt.Errorf("error unmarshalling JSON from %s: %w", filePath, err)
	}
    return pos, nil
}
