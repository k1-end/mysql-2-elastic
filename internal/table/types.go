package table

import (
	"fmt"
	"slices"
)

// BinlogPosition represents a position in the MySQL binlog.
type BinlogPosition struct {
	Logfile string `json:"logfile"`
	Logpos  uint32 `json:"logpos"`
}

// TableStatus tracks the sync lifecycle of a table.
type TableStatus string

const (
	Created              TableStatus = "created"
	Dumping              TableStatus = "dumping"
	Dumped               TableStatus = "dumped"
	InitializedInElastic TableStatus = "initialized_in_elastic"
	Moving               TableStatus = "moving"
	Moved                TableStatus = "moved"
	Syncing              TableStatus = "syncing"
)

type RegisteredTable struct {
	Name             string          `json:"name"`
	Status           TableStatus     `json:"status"`
	Columns          *[]ColumnInfo   `json:"columns"`
	BinlogPos        *BinlogPosition `json:"binlog_pos"`
	DumpReadProgress *int            `json:"dump_read_progress"`
}

type ColumnInfo struct {
	Name           string `json:"name"`
	Type           string `json:"type"`
	Position       int    `json:"position"`
	IsInPrimaryKey bool   `json:"is_in_primary_key"`
}

type DbRecord struct {
	PrimaryKey string
	ColValues  map[string]any
}

// ColumnMap builds a position-indexed map for O(1) lookups.
func ColumnMap(cols []ColumnInfo) map[int]ColumnInfo {
	m := make(map[int]ColumnInfo, len(cols))
	for _, c := range cols {
		m[c.Position] = c
	}
	return m
}

func (r DbRecord) GeneratePK(cols []ColumnInfo) (string, error) {
	slices.SortStableFunc(cols, func(c1, c2 ColumnInfo) int {
		if c1.Position > c2.Position {
			return 1
		} else if c1.Position < c2.Position {
			return -1
		}
		return 0
	})

	var pk string
	for i := range cols {
		if cols[i].IsInPrimaryKey {
			colval, ok := r.ColValues[cols[i].Name]
			if !ok {
				return "", fmt.Errorf("primary key column value not available: %s", cols[i].Name)
			}
			switch v := colval.(type) {
			case []byte:
				pk += string(v)
			case string:
				pk += v
			case int, int8, int16, int32, int64:
				pk += fmt.Sprintf("%d", v)
			case float32, float64:
				pk += fmt.Sprintf("%f", v)
			default:
				pk += fmt.Sprintf("%v", v)
			}
		}
	}
	return pk, nil
}
