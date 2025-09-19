package storage

import (
	"fmt"

	"github.com/k1-end/mysql-2-elastic/internal/syncer"
	"github.com/k1-end/mysql-2-elastic/internal/table"
)

type TableStorage interface {
	GetRegisteredTables() (map[string]table.RegisteredTable, error)
	GetTableStatus(tableName string) (table.TableStatus, error)
	GetTable(tableName string) (table.RegisteredTable, error)
	GetDumpFilePath(tableName string) (string, error)
	GetDumpFileDirectory(tableName string) (string)
	SetDumpReadProgress(tableName string, progress int) (error)
	SetTableStatus(tableName string, status table.TableStatus) (error)
	SetTableColsInfo(tableName string, colsInfo []table.ColumnInfo) (error)
	SetTableBinlogPos(tableName string, binlogPos syncer.BinlogPosition) (error)
}

// TableNotFoundError is a custom error type returned when a table is not found.
type TableNotFoundError struct {
	TableName string
}

// Error implements the error interface for TableNotFoundError.
func (e *TableNotFoundError) Error() string {
	return fmt.Sprintf("table '%s' not found", e.TableName)
}
