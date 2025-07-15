package table

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const (
    registeredTablesFilePath = "data/registered-tables.json"
)

type RegisteredTable struct {
    Name string `json:"name"`
    Status string `json:"status"`
}

type ColumnInfo struct {
	Name string `json:"name"`
    Type string `json:"type"`
    Position int `json:"positions"`
}

func GetTableStructure(structurePath string) ([]map[string]any, error) {
    jsonFile, err := os.Open(structurePath)
    defer jsonFile.Close()
    if err != nil {
        return nil, fmt.Errorf("Failed to open structure file: %w", err)
    }

    byteValue, err := io.ReadAll(jsonFile)
    if err != nil {
        return nil, fmt.Errorf("Failed to read structure file: %w", err)
    }
    var result []map[string]any

    err = json.Unmarshal(byteValue, &result)
    if err != nil {
        return nil, fmt.Errorf("Failed to unmarshal JSON: %w", err)
    }
    return result, nil

}
