package filesystem

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/k1-end/mysql-2-elastic/internal/table"
)

const (
    registeredTablesFilePath = "data/registered-tables.json"
)

type FileStorage struct {}

// Creates a new FileStorage instance.
func NewFileStorage() (*FileStorage, error) {
    return &FileStorage{}, nil
}

func (fs *FileStorage) GetRegisteredTables() (map[string]table.RegisteredTable, error)  {
	
	file, err := os.Open(registeredTablesFilePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}

	var tables map[string]table.RegisteredTable
	err = json.Unmarshal(byteValue, &tables)
	if err != nil {
		panic(err)
	}

	return tables, nil
}


func (fs *FileStorage) GetTableStatus(tableName string) (string, error)  {
	
	table, err := fs.GetTable(tableName)
	if err != nil {
		return "", err
	}
	status := table.Status
	return status, nil
}

func (fs *FileStorage) SetTableStatus(tableName string, status string) (error)  {
	
    registeredTables, err := fs.GetRegisteredTables()

    if err != nil {
        return err
    }

    t, exists := registeredTables[tableName]
    if !exists {
        return fmt.Errorf("table %s not found in registered tables", tableName)
    }
    t.Status = status
    registeredTables[t.Name] = t
    jsonData, err := json.Marshal(registeredTables)
    if err != nil {
        return err
    }
    err = os.WriteFile(registeredTablesFilePath, jsonData, 0644)
    if err != nil {
        return err
    }
    return nil
}


func (fs *FileStorage) GetTable(tableName string) (table.RegisteredTable, error)  {
	tbs, err := fs.GetRegisteredTables()
	if err != nil {
		return table.RegisteredTable{}, err
	}

	tb, ok := tbs[tableName]
	if  !ok {
		return table.RegisteredTable{}, fmt.Errorf("tableName not found: " + tableName)
	}

	return tb, nil
}

func (fs *FileStorage) SetTableColsInfo(tableName string, colsInfo []table.ColumnInfo) (error){
    registeredTables, err := fs.GetRegisteredTables()

    if err != nil {
        return err
    }

    t, exists := registeredTables[tableName]
    if !exists {
        return fmt.Errorf("table %s not found in registered tables", tableName)
    }
	t.Columns = &colsInfo
    registeredTables[t.Name] = t
    jsonData, err := json.Marshal(registeredTables)
    if err != nil {
        return err
    }
    err = os.WriteFile(registeredTablesFilePath, jsonData, 0644)
    if err != nil {
        return err
    }
    return nil
}

func (fs *FileStorage) GetDumpReadProgress(tableName string) (int, error){
	return 0, nil
}

func (fs *FileStorage) SetDumpReadProgress(tableName string, progress int) (error){
	return nil
}

func (fs *FileStorage) GetDumpFilePath(tableName string) (string, error) {
	return "", nil
}
