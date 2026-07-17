package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/k1-end/mysql-2-elastic/internal/config"
	"github.com/k1-end/mysql-2-elastic/internal/table"
)

// NewClient creates an Elasticsearch client and verifies connectivity.
func NewClient(cfg *config.Config) (*elasticsearch.Client, error) {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{cfg.Elastic.Address},
		Username:  cfg.Elastic.Username,
		Password:  cfg.Elastic.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	res, err := es.Info()
	if err != nil {
		return nil, fmt.Errorf("failed to get elasticsearch info: %w", err)
	}
	defer res.Body.Close()
	return es, nil
}

// BulkIndex sends documents to ES using the bulk API with "index" action.
func BulkIndex(indexName string, docs []table.DbRecord, client *elasticsearch.Client, log *slog.Logger) error {
	return bulkOp(indexName, "index", docs, client, log, "wait_for")
}

// BulkUpdate sends documents to ES using the bulk API with "update" action.
func BulkUpdate(indexName string, docs []table.DbRecord, client *elasticsearch.Client, log *slog.Logger) error {
	return bulkOp(indexName, "update", docs, client, log, "")
}

// BulkDelete sends documents to ES using the bulk API with "delete" action.
func BulkDelete(indexName string, docs []table.DbRecord, client *elasticsearch.Client, log *slog.Logger) error {
	return bulkOp(indexName, "delete", docs, client, log, "wait_for")
}

func bulkOp(indexName, action string, docs []table.DbRecord, client *elasticsearch.Client, log *slog.Logger, refresh string) error {
	if len(docs) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, doc := range docs {
		if doc.PrimaryKey == "" {
			return fmt.Errorf("document missing primary key for %s", action)
		}

		meta := map[string]any{
			action: map[string]any{
				"_index": indexName,
				"_id":    doc.PrimaryKey,
			},
		}
		metaBytes, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal bulk metadata: %w", err)
		}
		buf.Write(metaBytes)
		buf.WriteByte('\n')

		if action != "delete" {
			var body map[string]any
			if action == "update" {
				body = map[string]any{"doc": doc.ColValues}
			} else {
				body = doc.ColValues
			}
			docBytes, err := json.Marshal(body)
			if err != nil {
				return fmt.Errorf("failed to marshal document: %w", err)
			}
			buf.Write(docBytes)
			buf.WriteByte('\n')
		}
	}

	opts := []func(*esapi.BulkRequest){client.Bulk.WithContext(context.Background())}
	if refresh != "" {
		opts = append(opts, client.Bulk.WithRefresh(refresh))
	}

	res, err := client.Bulk(bytes.NewReader(buf.Bytes()), opts...)
	if err != nil {
		return fmt.Errorf("bulk %s request failed: %w", action, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var raw map[string]any
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			return fmt.Errorf("bulk %s failed, could not parse error response: %w", action, err)
		}
		return fmt.Errorf("bulk %s failed: [%s] %v", action, res.Status(), raw["error"])
	}

	var bulkRes map[string]any
	if err := json.NewDecoder(res.Body).Decode(&bulkRes); err != nil {
		return fmt.Errorf("failed to parse bulk response: %w", err)
	}

	if hasErrors, _ := bulkRes["errors"].(bool); hasErrors {
		log.Warn("bulk operation had item-level errors", "action", action, "index", indexName)
	}

	log.Debug("bulk operation completed", "action", action, "index", indexName, "count", len(docs), "status", res.Status())
	return nil
}

// CheckIndexExists checks whether an ES index exists.
func CheckIndexExists(client *elasticsearch.Client, indexName string) (bool, error) {
	res, err := client.Indices.Exists([]string{indexName})
	if err != nil {
		return false, fmt.Errorf("failed to check if index %q exists: %w", indexName, err)
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status code checking index %q: %d", indexName, res.StatusCode)
	}
}

// CreateIndex creates an ES index with a mapping derived from the table columns.
func CreateIndex(client *elasticsearch.Client, t table.RegisteredTable, log *slog.Logger) error {
	mapping, err := generateMapping(t)
	if err != nil {
		return fmt.Errorf("failed to generate mapping for %q: %w", t.Name, err)
	}
	data, err := json.Marshal(mapping)
	if err != nil {
		return fmt.Errorf("failed to marshal mapping: %w", err)
	}

	res, err := client.Indices.Create(t.Name, client.Indices.Create.WithBody(bytes.NewReader(data)))
	if err != nil {
		return fmt.Errorf("failed to create index %q: %w", t.Name, err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("elasticsearch error creating index %q: %s", t.Name, res.String())
	}
	log.Info("index created", "index", t.Name)
	return nil
}

func generateMapping(t table.RegisteredTable) (map[string]any, error) {
	if t.Columns == nil {
		return nil, fmt.Errorf("no columns for table %q", t.Name)
	}
	props := make(map[string]any)
	for _, col := range *t.Columns {
		esType := mysqlTypeToEsType(col.Type)
		field := map[string]any{"type": esType}
		if esType == "date" {
			field["format"] = "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
		}
		props[col.Name] = field
	}
	return map[string]any{
		"mappings": map[string]any{"properties": props},
	}, nil
}

func mysqlTypeToEsType(mysqlType string) string {
	t := strings.ToLower(mysqlType)
	re := regexp.MustCompile(`\([0-9]+\)`)
	t = re.ReplaceAllString(t, "")

	switch t {
	case "int", "tinyint", "smallint", "mediumint":
		return "integer"
	case "bigint":
		return "long"
	case "float":
		return "float"
	case "double", "real", "decimal", "numeric":
		return "double"
	case "char", "varchar":
		return "keyword"
	case "text", "mediumtext", "longtext", "tinytext":
		return "text"
	case "date", "datetime", "timestamp":
		return "date"
	case "boolean", "bool":
		return "boolean"
	default:
		return "text"
	}
}
