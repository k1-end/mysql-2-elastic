package handler

import (
	"context"

	"github.com/k1-end/mysql-2-elastic/internal/table"
)

type Operation string

const (
	OpInsert Operation = "insert"
	OpUpdate Operation = "update"
	OpDelete Operation = "delete"
)

type Record struct {
	PrimaryKey string
	Columns    map[string]any
}

type Event struct {
	Operation  Operation
	Table      string
	Records    []Record
	ColumnInfo []table.ColumnInfo
	BinlogPos  *table.BinlogPosition
}

// EventHandler processes MySQL change events. Implement this interface
// to create custom handlers (e.g., ES sync, webhooks, file logging).
type EventHandler interface {
	Name() string
	HandleEvent(ctx context.Context, event Event) error
	OnTableInit(ctx context.Context, tableName string, columns []table.ColumnInfo) error
	Close() error
}
