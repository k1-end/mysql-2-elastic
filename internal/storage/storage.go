package storage

import "github.com/k1-end/mysql-2-elastic/internal/table"

type TableStorage interface {
	GetRegisteredTables() (map[string]table.RegisteredTable, error)
	GetTableStatus(tableName string) (string, error)
	SetTableStatus(tableName string, status string) (error)
}
