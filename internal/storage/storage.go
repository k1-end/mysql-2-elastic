package storage

import (
	"github.com/k1-end/mysql-2-elastic/internal/syncer"
	"github.com/k1-end/mysql-2-elastic/internal/table"
)

type TableStorage interface {
	GetRegisteredTables() (map[string]table.RegisteredTable, error)
	GetTableStatus(tableName string) (string, error)
	GetTable(tableName string) (table.RegisteredTable, error)
	GetDumpFilePath(tableName string) (string, error)
	SetDumpReadProgress(tableName string, progress int) (error)
	SetTableStatus(tableName string, status string) (error)
	SetTableColsInfo(tableName string, colsInfo []table.ColumnInfo) (error)
	SetTableBinlogPos(tableName string, binlogPos syncer.BinlogPosition) (error)
}
