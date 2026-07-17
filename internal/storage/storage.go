package storage

import (
	"fmt"

	"github.com/k1-end/mysql-2-elastic/internal/table"
)

type TableStorage interface {
	GetRegisteredTables() (map[string]table.RegisteredTable, error)
	GetTableStatus(tableName string) (table.TableStatus, error)
	GetTable(tableName string) (table.RegisteredTable, error)
	GetDumpFilePath(tableName string) (string, error)
	GetDumpFileDirectory(tableName string) string
	SetDumpReadProgress(tableName string, progress int) error
	SetTableStatus(tableName string, status table.TableStatus) error
	SetTableColsInfo(tableName string, colsInfo []table.ColumnInfo) error
	SetTableBinlogPos(tableName string, binlogPos table.BinlogPosition) error
}

type TableNotFoundError struct {
	TableName string
}

func (e *TableNotFoundError) Error() string {
	return fmt.Sprintf("table %q not found", e.TableName)
}
