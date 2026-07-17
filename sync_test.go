package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	_ "github.com/go-sql-driver/mysql"
	"github.com/k1-end/mysql-2-elastic/internal/config"
	"github.com/k1-end/mysql-2-elastic/internal/database"
	"github.com/k1-end/mysql-2-elastic/internal/es"
	"github.com/k1-end/mysql-2-elastic/internal/logger"
	"github.com/k1-end/mysql-2-elastic/internal/table"
)

var testLog = logger.NewLogger(slog.LevelDebug)

func TestSync(t *testing.T) {
	appConfig, err := config.LoadConfig()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		appConfig.Database.Username,
		appConfig.Database.Password,
		appConfig.Database.Host,
		appConfig.Database.Port,
		appConfig.Database.Name)

	mysqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to open MySQL: %v", err)
	}
	defer mysqlDB.Close()

	if err = mysqlDB.Ping(); err != nil {
		t.Fatalf("failed to ping MySQL: %v", err)
	}

	esClient, err := es.NewClient(appConfig)
	if err != nil {
		t.Fatalf("failed to connect to Elasticsearch: %v", err)
	}

	testTable := "city"
	mysqlRow := table.DbRecord{
		ColValues: map[string]any{
			"CountryCode": "NLD",
			"Name":        "TestCity",
			"District":    "Utrecht",
			"Population":  10,
		},
	}

	primaryKeyValue, err := database.InsertRowIntoTable(mysqlDB, testTable, mysqlRow)
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}
	t.Logf("inserted row with ID %d", primaryKeyValue)

	time.Sleep(3 * time.Second)

	exists, err := checkElasticsearchDocument(t, esClient, testTable, strconv.FormatInt(primaryKeyValue, 10), mysqlRow)
	if err != nil {
		t.Fatalf("error checking document: %v", err)
	}
	if !exists {
		t.Errorf("document with ID %d not found in Elasticsearch", primaryKeyValue)
	}

	_, err = mysqlDB.Exec(fmt.Sprintf("DELETE FROM `%s` WHERE `ID` = ?", testTable), primaryKeyValue)
	if err != nil {
		t.Errorf("failed to cleanup MySQL data: %v", err)
	}
}

func checkElasticsearchDocument(t *testing.T, client *elasticsearch.Client, index, docID string, dbRecord table.DbRecord) (bool, error) {
	req := esapi.GetRequest{Index: index, DocumentID: docID}
	res, err := req.Do(context.Background(), client)
	if err != nil {
		return false, fmt.Errorf("GET request failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("error getting document: %s", res.Status())
	}

	var result map[string]any
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode response: %w", err)
	}

	source, ok := result["_source"].(map[string]any)
	if !ok {
		return false, fmt.Errorf("_source is not a map")
	}

	for key, dbVal := range dbRecord.ColValues {
		esVal, ok := source[key]
		if !ok {
			return false, fmt.Errorf("key %q found in DB but not in ES", key)
		}
		if !compareValues(dbVal, esVal) {
			t.Logf("value mismatch for %q: DB %v (%T) != ES %v (%T)", key, dbVal, dbVal, esVal, esVal)
		}
	}
	return true, nil
}

func compareValues(dbVal, esVal any) bool {
	if fmt.Sprintf("%v", dbVal) == fmt.Sprintf("%v", esVal) {
		return true
	}
	dbFloat := toFloat64(dbVal)
	esFloat := toFloat64(esVal)
	return dbFloat != 0 && esFloat != 0 && dbFloat == esFloat
}

func toFloat64(v any) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	case []byte:
		f, err := strconv.ParseFloat(string(val), 64)
		if err == nil {
			return f
		}
	}
	return 0
}
