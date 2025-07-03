package main

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

func GetRegisteredTables() map[string]RegisteredTable {
	file, err := os.Open(registeredTablesFilePath)
	if err != nil {
		MainLogger.Error(err.Error())
		panic(err)
	}
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	if err != nil {
		MainLogger.Error(err.Error())
		panic(err)
	}

	var tables map[string]RegisteredTable
	err = json.Unmarshal(byteValue, &tables)
	if err != nil {
		MainLogger.Error(err.Error())
		panic(err)
	}

	return tables
}

func getUninitializedTables(registeredTables map[string]RegisteredTable) map[string]RegisteredTable {
	var uninitializedTables = make(map[string]RegisteredTable)
	for _, table := range registeredTables {
		if table.Status == "created" {
			uninitializedTables[table.Name] = table
		}
	}
	return uninitializedTables
}

func getTableStructure(structurePath string) ([]map[string]interface{}, error) {
    jsonFile, err := os.Open(structurePath)
    defer jsonFile.Close()
    if err != nil {
        return nil, fmt.Errorf("Failed to open structure file: %w", err)
    }

    byteValue, err := io.ReadAll(jsonFile)
    if err != nil {
        return nil, fmt.Errorf("Failed to read structure file: %w", err)
    }
    var result []map[string]interface{}

    err = json.Unmarshal(byteValue, &result)
    if err != nil {
        fmt.Println("Error unmarshalling JSON:", err)
        return nil, fmt.Errorf("Failed to unmarshal JSON: %w", err)
    }
    return result, nil

}

func tableExists(tableName string) bool {
    _, exists := GetRegisteredTables()[tableName]
    return exists
}

