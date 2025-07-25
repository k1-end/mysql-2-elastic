package table

import (
	"fmt"
	"slices"

	"github.com/k1-end/mysql-2-elastic/internal/syncer"
)


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

func (r DbRecord) GeneratePK(cols []ColumnInfo) (string, error) {
	slices.SortStableFunc(cols, func(c1, c2 ColumnInfo) int {
		if c1.Position > c2.Position {
			return 1
		} else if c1.Position < c2.Position {
			return -1
		} else{
			return 0
		}
	})

	var pk = ""
	for i := range cols{
		if cols[i].IsInPrimaryKey {
			colval, ok :=r.ColValues[cols[i].Name] 
			if !ok {
				return "", fmt.Errorf("PrimaryKey col val is not available")
			}
			switch v := colval.(type) {
            case []byte:
                pk += string(v) // Convert []byte to string
            case string:
                pk += v // Already a string
            case int, int8, int16, int32, int64:
                pk += fmt.Sprintf("%d", v) // For integers, use %d
            case float32, float64:
                pk += fmt.Sprintf("%f", v) // For floats, use %f (consider precision)
            // Add other types as needed
            default:
                // Fallback for other types, might still be problematic for complex types
                pk += fmt.Sprintf("%v", v) 
            }
		}
	}
	return pk, nil
}
