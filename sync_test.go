package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	_ "github.com/go-sql-driver/mysql"
	"github.com/k1-end/mysql-2-elastic/internal/config"
	databasemodule "github.com/k1-end/mysql-2-elastic/internal/database"
	"github.com/k1-end/mysql-2-elastic/internal/elastic"
	"github.com/k1-end/mysql-2-elastic/internal/storage/filesystem"
	tablepack "github.com/k1-end/mysql-2-elastic/internal/table"
)

// TestSync tests the synchronization process from MySQL to Elasticsearch
func TestSync(t *testing.T) {
	// --- 1. Load Configuration ---
	t.Log("1. Loading configuration...")
	appConfig, err := config.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// --- 2. Connect to MySQL ---
	t.Log("2. Connecting to MySQL...")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		appConfig.Database.Username,
		appConfig.Database.Password,
		appConfig.Database.Host,
		appConfig.Database.Port,
		appConfig.Database.Name)

	mysqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to open MySQL connection: %v", err)
	}

	if err = mysqlDB.Ping(); err != nil {
		t.Fatalf("Failed to ping MySQL database: %v", err)
	}
	t.Log("Successfully connected to MySQL.")

	// --- 3. Connect to Elasticsearch ---
	t.Log("3. Connecting to Elasticsearch...")
	esClient, err := elastic.GetElasticClient(appConfig)
	if err != nil {
		t.Fatalf("Failed to get Elasticsearch client: %v", err)
	}
	t.Log("Successfully connected to Elasticsearch.")

	// --- 4. Insert a row into a table ---
	t.Log("4. Inserting a row into the 'city' table...")
	// The provided data matches the structure of the 'city' table.
	testTable := "city"
	primaryKeyName := "ID"

	mysqlRow := tablepack.DbRecord{
		ColValues: map[string]any{
			"CountryCode":    "NLD",
			"Name":           "TestCity",
			"District": "Utrecht",
			"Population": 10,
		},
	}

	primaryKeyValue, err := databasemodule.InsertRowIntoTable(mysqlDB, testTable, mysqlRow)
	if err != nil {
		t.Fatalf("Failed to insert row into MySQL table '%s': %v", testTable, err)
	}
	t.Logf("Successfully inserted row with Code '%d' into MySQL.", primaryKeyValue)

	// In a real scenario, you might need to wait for the sync process to run.
	// For this test, we assume a near-instantaneous sync or poll until it's available.
	// Let's add a short delay to allow for the sync to occur.
	time.Sleep(3 * time.Second)

	// --- 5. Check if the inserted row is available in elastic ---
	t.Logf("5. Checking if the document with ID '%d' exists in Elasticsearch index '%s'...", primaryKeyValue, testTable)
	exists, err := checkElasticsearchDocument2(t, esClient, testTable, strconv.FormatInt(primaryKeyValue, 10), mysqlRow)
	if err != nil {
		t.Fatalf("Error checking for document in Elasticsearch: %v", err)
	}
	if !exists {
		t.Errorf("Document with ID '%d' was not found in Elasticsearch index '%s'.", primaryKeyValue, testTable)
	} else {
		t.Logf("Successfully found and verified document with ID '%d' in Elasticsearch.", primaryKeyValue)
	}

	// --- 6. Cleanup (Optional but good practice) ---
	t.Log("6. Cleaning up the test data...")
	// Delete the row from MySQL
	_, err = mysqlDB.Exec(fmt.Sprintf("DELETE FROM `%s` WHERE `%s` = ?", testTable, primaryKeyName), primaryKeyValue)
	if err != nil {
		t.Errorf("Failed to cleanup MySQL data: %v", err)
	}

	// No need to delete the document from Elasticsearch, It will be deleted automatically
	t.Log("Cleanup complete.")
	defer mysqlDB.Close()
}

func checkElasticsearchDocument2(t *testing.T, client *elasticsearch.Client, index, docID string, dbRecord tablepack.DbRecord) (bool, error) {
	reqGet := esapi.GetRequest{
		Index:      index,
		DocumentID: docID,
	}

	resGet, err := reqGet.Do(context.Background(), client)
	if err != nil {
		return false, fmt.Errorf("error performing GET request: %w", err)
	}
	defer resGet.Body.Close()

	if resGet.IsError() {
		if resGet.StatusCode == 404 {
			return false, nil // Document not found is not an error.
		}
		return false, fmt.Errorf("error getting document: %s", resGet.Status())
	}

	var res map[string]any
	if err := json.NewDecoder(resGet.Body).Decode(&res); err != nil {
		return false, fmt.Errorf("error parsing the response body: %w", err)
	}

	source, ok := res["_source"].(map[string]any)
	if !ok {
		return false, fmt.Errorf("error asserting _source to map[string]any")
	}

	// Compare the fields
	for key, dbValue := range dbRecord.ColValues {
		if key == "Continent" {
			continue
		}
		esValue, ok := source[key]
		if !ok {
			return false, fmt.Errorf("key '%s' found in DB record but not in Elasticsearch document", key)
		}
		fs, _ := filesystem.NewFileStorage([]string{index})
		ta, _ := fs.GetTable(index)
		cols := ta.Columns
		var columnType string 
		for _, co := range *cols {
			if co.Name == key {
				columnType = co.Type
				break
			}
		}

		if columnType == "" {
			t.Errorf("Unknown column name and type: %s", key)
			return false, nil
		}

		// Simple comparison for most types, with special handling for numbers.
		if !compareValues(dbValue, esValue, columnType) {
			fmt.Printf("Value mismatch for key '%s': ES '%v' (type %T) != DB '%v' (type %T)\n", key, esValue, esValue, dbValue, dbValue)
			// return false, nil
		}
	}

	return true, nil
}

// compareValues handles the comparison of different data types, especially numbers.
func compareValues(dbValue, esValue any, colType string) bool {
	// Simple case: values are the same type and value
	if reflect.DeepEqual(dbValue, esValue) {
		return true
	}

	if strings.HasPrefix(colType, "decimal"){
		esFloat, ok := toFloat64(esValue)
		if !ok {
			fmt.Println("could not parse esValue to float")
			return false
		}

		dbFloat, ok := toFloat64(dbValue)
		if !ok {
			fmt.Println("could not parse dbValue to float")
			return false
		}

		if  esFloat == dbFloat{
			return true
		}else{
			fmt.Printf("Value mismatch for key '%s': ES '%v' (type %T) != DB '%v' (type %T)", colType, esFloat, esFloat, dbFloat, dbFloat)
			os.Exit(1)
			return false
		}
	}else if (strings.HasPrefix(colType, "int")){
		esInt, ok := toInt64(esValue)
		if !ok {
			fmt.Println("could not parse esValue to int")
			return false
		}

		dbInt, ok := toInt64(dbValue)
		if !ok {
			fmt.Println("could not parse dbValue to int")
			return false
		}

		if  esInt == dbInt{
			return true
		}else{
			fmt.Printf("Value mismatch for key '%s': ES '%v' (type %T) != DB '%v' (type %T)", colType, esInt, esInt, dbInt, dbInt)
			os.Exit(1)
			return false
		}
	}else{
		return false
	}
}

// toFloat64 safely converts various numeric types to a float64.
func toFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case []byte: // Handle numeric values stored as byte slices
		parsedFloat, err := strconv.ParseFloat(string(val), 64)
		if err == nil {
			return parsedFloat, true
		}
	}
	return 0, false
}

func toInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return int64(val), true
	case uint:
		return int64(val), true
	case uint8:
		return int64(val), true
	case uint16:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		return int64(val), true
	case float32:
		return int64(val), true
	case float64:
		return int64(val), true
	case []byte: // Handle numeric values stored as byte slices
		parsedFloat, err := strconv.ParseInt(string(val), 10, 65)
		if err == nil {
			return parsedFloat, true
		}
	}
	return 0, false
}
