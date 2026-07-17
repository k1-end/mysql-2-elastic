package elasticsearch

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/k1-end/mysql-2-elastic/internal/es"
	"github.com/k1-end/mysql-2-elastic/internal/handler"
	"github.com/k1-end/mysql-2-elastic/internal/table"
)

type ESHandler struct {
	client *elasticsearch.Client
	log    *slog.Logger
}

func New(config map[string]any, log *slog.Logger) (*ESHandler, error) {
	address, _ := config["address"].(string)
	username, _ := config["username"].(string)
	password, _ := config["password"].(string)

	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{address},
		Username:  username,
		Password:  password,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	res, err := client.Info()
	if err != nil {
		return nil, fmt.Errorf("failed to verify elasticsearch connection: %w", err)
	}
	defer res.Body.Close()

	return &ESHandler{client: client, log: log}, nil
}

func (h *ESHandler) Name() string {
	return "elasticsearch"
}

func (h *ESHandler) OnTableInit(ctx context.Context, tableName string, columns []table.ColumnInfo) error {
	exists, err := es.CheckIndexExists(h.client, tableName)
	if err != nil {
		return fmt.Errorf("check index exists: %w", err)
	}
	if !exists {
		t := table.RegisteredTable{Name: tableName, Columns: &columns}
		if err := es.CreateIndex(h.client, t, h.log); err != nil {
			return fmt.Errorf("create index: %w", err)
		}
	}
	return nil
}

func (h *ESHandler) HandleEvent(ctx context.Context, event handler.Event) error {
	records := toDbRecords(event.Records)
	switch event.Operation {
	case handler.OpInsert:
		return es.BulkIndex(event.Table, records, h.client, h.log)
	case handler.OpUpdate:
		return es.BulkUpdate(event.Table, records, h.client, h.log)
	case handler.OpDelete:
		return es.BulkDelete(event.Table, records, h.client, h.log)
	}
	return nil
}

func (h *ESHandler) Close() error {
	return nil
}

func toDbRecords(recs []handler.Record) []table.DbRecord {
	out := make([]table.DbRecord, len(recs))
	for i, r := range recs {
		out[i] = table.DbRecord{
			PrimaryKey: r.PrimaryKey,
			ColValues:  r.Columns,
		}
	}
	return out
}
