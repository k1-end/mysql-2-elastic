package filesystem

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/k1-end/mysql-2-elastic/internal/storage"
	"github.com/k1-end/mysql-2-elastic/internal/table"
)

const registeredTablesFilePath = "data/registered-tables.json"

type FileStorage struct{}

func NewFileStorage(tableNames []string) (*FileStorage, error) {
	if !fileExists(registeredTablesFilePath) {
		data, err := json.Marshal(make(map[string]interface{}))
		if err != nil {
			return nil, fmt.Errorf("failed to marshal empty tables: %w", err)
		}
		if err := os.WriteFile(registeredTablesFilePath, data, 0644); err != nil {
			return nil, fmt.Errorf("failed to create tables file: %w", err)
		}
	}

	fs := &FileStorage{}
	for _, t := range tableNames {
		_, err := fs.GetTable(t)
		if err != nil {
			var nf *storage.TableNotFoundError
			if errors.As(err, &nf) {
				if err := fs.addTable(t); err != nil {
					return nil, fmt.Errorf("failed to add table %q: %w", t, err)
				}
			}
		}
	}
	return fs, nil
}

func (fs *FileStorage) GetRegisteredTables() (map[string]table.RegisteredTable, error) {
	file, err := os.Open(registeredTablesFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open tables file: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read tables file: %w", err)
	}

	var tables map[string]table.RegisteredTable
	if err := json.Unmarshal(data, &tables); err != nil {
		return nil, fmt.Errorf("failed to parse tables file: %w", err)
	}
	return tables, nil
}

func (fs *FileStorage) GetTableStatus(tableName string) (table.TableStatus, error) {
	t, err := fs.GetTable(tableName)
	if err != nil {
		return "", err
	}
	return t.Status, nil
}

func (fs *FileStorage) GetTable(tableName string) (table.RegisteredTable, error) {
	tables, err := fs.GetRegisteredTables()
	if err != nil {
		return table.RegisteredTable{}, err
	}
	t, ok := tables[tableName]
	if !ok {
		return table.RegisteredTable{}, &storage.TableNotFoundError{TableName: tableName}
	}
	return t, nil
}

func (fs *FileStorage) SetTableStatus(tableName string, status table.TableStatus) error {
	return fs.updateTable(tableName, func(t *table.RegisteredTable) {
		t.Status = status
	})
}

func (fs *FileStorage) SetTableColsInfo(tableName string, colsInfo []table.ColumnInfo) error {
	return fs.updateTable(tableName, func(t *table.RegisteredTable) {
		t.Columns = &colsInfo
	})
}

func (fs *FileStorage) SetDumpReadProgress(tableName string, progress int) error {
	return fs.updateTable(tableName, func(t *table.RegisteredTable) {
		t.DumpReadProgress = &progress
	})
}

func (fs *FileStorage) SetTableBinlogPos(tableName string, binlogPos table.BinlogPosition) error {
	return fs.updateTable(tableName, func(t *table.RegisteredTable) {
		t.BinlogPos = &binlogPos
	})
}

func (fs *FileStorage) GetDumpFilePath(tableName string) (string, error) {
	return fs.GetDumpFileDirectory(tableName) + "/" + tableName + ".sql", nil
}

func (fs *FileStorage) GetDumpFileDirectory(tableName string) string {
	return "data/dumps/" + tableName
}

// updateTable reads the JSON, applies fn to the matching table, then writes it back.
func (fs *FileStorage) updateTable(tableName string, fn func(*table.RegisteredTable)) error {
	tables, err := fs.GetRegisteredTables()
	if err != nil {
		return err
	}
	t, ok := tables[tableName]
	if !ok {
		return &storage.TableNotFoundError{TableName: tableName}
	}
	fn(&t)
	tables[tableName] = t

	data, err := json.Marshal(tables)
	if err != nil {
		return fmt.Errorf("failed to marshal tables: %w", err)
	}
	if err := os.WriteFile(registeredTablesFilePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write tables file: %w", err)
	}
	return nil
}

func (fs *FileStorage) addTable(tableName string) error {
	return fs.updateTable(tableName, func(t *table.RegisteredTable) {
		t.Name = tableName
		t.Status = table.Created
	})
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}
