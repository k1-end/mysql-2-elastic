package table

import "github.com/k1-end/mysql-2-elastic/internal/syncer"


const (
    registeredTablesFilePath = "data/registered-tables.json"
)

type RegisteredTable struct {
    Name string `json:"name"`
    Status string `json:"status"`
	Columns *[]ColumnInfo `json:"columns"`
	BinlogPos *syncer.BinlogPosition `json:"binlog_pos"`
	DumpReadProgress *int `json:"dump_read_progress"`
	PrimaryKey *[]ColumnInfo `json:"primary_key"`
}

type ColumnInfo struct {
	Name string `json:"name"`
    Type string `json:"type"`
    Position int `json:"positions"`
    IsInPrimaryKey bool `json:"is_in_primary_key"`
}

type DbRecord struct {
	PrimaryKey string
	ColValues map[string]any  
}
