package storage

import "github.com/k1-end/mysql-2-elastic/internal/table"

type TableStorage interface {
	GetRegisteredTables() (map[string]table.RegisteredTable, error)
	GetTableStatus(tableName string) (string, error)
	GetTable(tableName string) (table.RegisteredTable, error)
	SetTableStatus(tableName string, status string) (error)
	SetTableColsInfo(tableName string, colsInfo []table.ColumnInfo) (error)
}
