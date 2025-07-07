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

func SetTableStatus(tableName string, status string) error {
    registeredTables := GetRegisteredTables()
    table, exists := registeredTables[tableName]
    if !exists {
        return fmt.Errorf("table %s not found in registered tables", tableName)
    }
    table.Status = status
    registeredTables[table.Name] = table
    jsonData, _ := json.Marshal(registeredTables)
    os.WriteFile(registeredTablesFilePath, jsonData, 0644)
    return nil
}

func GetRegisteredTables() map[string]RegisteredTable {
	file, err := os.Open(registeredTablesFilePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}

	var tables map[string]RegisteredTable
	err = json.Unmarshal(byteValue, &tables)
	if err != nil {
		panic(err)
	}

	return tables
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

func TableExists(tableName string) bool {
    _, exists := GetRegisteredTables()[tableName]
    return exists
}
