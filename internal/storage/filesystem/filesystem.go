package filesystem

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/k1-end/mysql-2-elastic/internal/storage"
	"github.com/k1-end/mysql-2-elastic/internal/syncer"
	"github.com/k1-end/mysql-2-elastic/internal/table"
)

const (
    registeredTablesFilePath = "data/registered-tables.json"
)

type FileStorage struct {}

// Creates a new FileStorage instance.
func NewFileStorage(tableNames []string) (*FileStorage, error) {
	if !fileExists(registeredTablesFilePath) {
		jsonData, err := json.Marshal(make(map[string]interface{}))
		if err != nil {
			return nil, err
		}
		err = os.WriteFile(registeredTablesFilePath, jsonData, 0644)
		if err != nil {
			return nil, err
		}
	}
	fs := &FileStorage{}

	for _, t := range tableNames{
		_, err := fs.GetTable(t)
		if err != nil {
			var notFoundErr *storage.TableNotFoundError
			if errors.As(err, &notFoundErr) {
				fs.addTable(t)
			}
		}
	}
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


func (fs *FileStorage) GetTableStatus(tableName string) (table.TableStatus, error)  {

	table, err := fs.GetTable(tableName)
	if err != nil {
		return "", err
	}
	status := table.Status
	return status, nil
}

func (fs *FileStorage) SetTableStatus(tableName string, status table.TableStatus) (error)  {

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
		return table.RegisteredTable{}, &storage.TableNotFoundError{TableName: tableName}
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

func (fs *FileStorage) SetDumpReadProgress(tableName string, progress int) (error){
	registeredTables, err := fs.GetRegisteredTables()

	if err != nil {
		return err
	}

	t, exists := registeredTables[tableName]
	if !exists {
		return fmt.Errorf("table %s not found in registered tables", tableName)
	}
	t.DumpReadProgress = &progress
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

func (fs *FileStorage) GetDumpFilePath(tableName string) (string, error) {
	return fs.GetDumpFileDirectory(tableName) + "/" + tableName + ".sql", nil
}

func (fs *FileStorage) GetDumpFileDirectory(tableName string) (string) {
	return "data/dumps/" + tableName
}

func (fs *FileStorage) SetTableBinlogPos(tableName string, binlogPos syncer.BinlogPosition) (error) {
	registeredTables, err := fs.GetRegisteredTables()

	if err != nil {
		return err
	}

	t, exists := registeredTables[tableName]
	if !exists {
		return fmt.Errorf("table %s not found in registered tables", tableName)
	}
	t.BinlogPos = &binlogPos
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

func fileExists(filename string) bool {
	info, err := os.Stat(filename) // Get file info
	if os.IsNotExist(err) {
		return false // File does not exist
	}
	// Check if it's a directory (optional, depending on your needs)
	// If you only care about regular files, add info.IsDir() check
	return !info.IsDir() // Return true if it exists and is not a directory
}


func (fs *FileStorage) addTable(tableName string) (error) {
	registeredTables, err := fs.GetRegisteredTables()

	if err != nil {
		return err
	}

	_, exists := registeredTables[tableName]
	if exists {
		return fmt.Errorf("table %s already exists in registered tables", tableName)
	}
	registeredTables[tableName] = table.RegisteredTable{Name: tableName, Status: "created"}
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
