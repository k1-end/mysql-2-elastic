package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	_ "github.com/go-sql-driver/mysql"
	"github.com/k1-end/mysql-2-elastic/internal/config"
	"github.com/k1-end/mysql-2-elastic/internal/database"
	"github.com/k1-end/mysql-2-elastic/internal/logger"
	"github.com/k1-end/mysql-2-elastic/internal/storage/filesystem"
	"github.com/k1-end/mysql-2-elastic/internal/table"
)

var seedLog = logger.NewLogger(slog.LevelDebug)

func TestSeed(t *testing.T) {
	seedLog.Debug("starting seed verification")

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

	esClient := newTestESClient(t, appConfig)

	tableNames, err := database.GetTableNames(mysqlDB)
	if err != nil {
		t.Fatalf("failed to get table names: %v", err)
	}
	if len(tableNames) == 0 {
		t.Log("no tables found")
		return
	}

	store, err := filesystem.NewFileStorage(appConfig.Database.Tables)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	totalVerified := 0
	totalMissing := 0

	for _, tableName := range tableNames {
		mysqlRows, err := database.GetMySQLRows(mysqlDB, tableName)
		if err != nil {
			t.Logf("error getting rows from %s: %v", tableName, err)
			continue
		}
		if len(mysqlRows) == 0 {
			continue
		}

		registered, err := store.GetTable(tableName)
		if err != nil {
			t.Logf("error getting registered table %s: %v", tableName, err)
			continue
		}

		for _, mysqlRow := range mysqlRows {
			pkValue, err := mysqlRow.GeneratePK(*registered.Columns)
			if err != nil {
				continue
			}

			exists, err := checkSeedDocument(esClient, tableName, pkValue, mysqlRow)
			if err != nil {
				t.Logf("error checking %s/%s: %v", tableName, pkValue, err)
				totalMissing++
				continue
			}
			if exists {
				totalVerified++
			} else {
				totalMissing++
			}
		}
		t.Logf("table %s: verified=%d missing=%d", tableName, totalVerified, totalMissing)
	}

	t.Logf("verification complete: verified=%d missing=%d", totalVerified, totalMissing)
	if totalMissing > 0 {
		t.Errorf("%d rows missing in Elasticsearch", totalMissing)
	}
}

func checkSeedDocument(client *elasticsearch.Client, index, docID string, dbRecord table.DbRecord) (bool, error) {
	req := esapi.GetRequest{Index: index, DocumentID: docID}
	res, err := req.Do(context.Background(), client)
	if err != nil {
		return false, fmt.Errorf("GET failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("error: %s", res.Status())
	}

	var result map[string]any
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("decode failed: %w", err)
	}

	source, ok := result["_source"].(map[string]any)
	if !ok {
		return false, fmt.Errorf("_source is not a map")
	}

	for key, esVal := range source {
		dbVal, ok := dbRecord.ColValues[key]
		if !ok {
			return false, fmt.Errorf("key %q in ES but not in DB record", key)
		}
		if !compareSeedValues(dbVal, esVal) {
			return false, fmt.Errorf("mismatch for %q: DB %v (%T) != ES %v (%T)", key, dbVal, dbVal, esVal, esVal)
		}
	}
	return true, nil
}

func compareSeedValues(dbVal, esVal any) bool {
	if fmt.Sprintf("%v", dbVal) == fmt.Sprintf("%v", esVal) {
		return true
	}
	dbFloat := toFloat64(dbVal)
	esFloat := toFloat64(esVal)
	return dbFloat != 0 && esFloat != 0 && dbFloat == esFloat
}
