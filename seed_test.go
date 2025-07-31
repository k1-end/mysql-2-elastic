package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	_ "github.com/go-sql-driver/mysql"
	"github.com/k1-end/mysql-2-elastic/internal/config"
	databasemodule "github.com/k1-end/mysql-2-elastic/internal/database"
	"github.com/k1-end/mysql-2-elastic/internal/elastic"
	"github.com/k1-end/mysql-2-elastic/internal/storage/filesystem"
	"github.com/k1-end/mysql-2-elastic/internal/table"
)


func TestSeed(t *testing.T) {

	MainLogger.Debug("Starting MySQL to Elasticsearch migration verification program...")

	// --- 1. Load Configuration ---
	// In a real application, you might load this from a file or environment variables.
	// For simplicity, we'll hardcode it here.
	// IMPORTANT: Replace with your actual credentials and URLs!
	// cfg := Config{
	// 	MySQLDSN:       "root:password@tcp(127.0.0.1:3306)/your_mysql_db?parseTime=true", // Example DSN
	// 	ElasticsearchURL: "http://localhost:9200",                                    // Example ES URL
	// }
	appConfig, err := config.LoadConfig()
	if err != nil {
		MainLogger.Error(fmt.Sprintf("Fatal error: could not load config %v", err))
		os.Exit(1)
	}

	// --- 2. Connect to MySQL ---
	mysqlDB, err := sql.Open("mysql", appConfig.Database.Username + ":" + appConfig.Database.Password + "@tcp(" + appConfig.Database.Host + ":" + strconv.FormatInt(int64(appConfig.Database.Port), 10) + ")/" + appConfig.Database.Name + "?parseTime=true")
	if err != nil {
		MainLogger.Error(fmt.Sprintf("Error opening MySQL connection: %v", err))
		panic(err)
	}
	defer mysqlDB.Close()

	if err = mysqlDB.Ping(); err != nil {
		MainLogger.Error(fmt.Sprintf("Error connecting to MySQL: %v", err))
		panic(err)
	}
	MainLogger.Debug("Successfully connected to MySQL.")

	// --- 3. Connect to Elasticsearch ---
	// esClient, err := elastic.NewClient(
	// 	elastic.SetURL(appConfig.Elastic.Address ),
	// 	elastic.SetSniff(false), // Disable sniffing for local/single-node setups if needed
	// 	elastic.SetHealthcheckInterval(10*time.Second),
	// 	elastic.SetInfoLog(MainLogWriter),
	// 	elastic.SetErrorLog(MainLogWriter),
	// 	elastic.SetBasicAuth(appConfig.Elastic.Username, appConfig.Elastic.Password),
	// )

	esClient, err := elastic.GetElasticClient(appConfig) 
	if err != nil {
		MainLogger.Error(fmt.Sprintf("Error creating Elasticsearch client: %v", err))
		os.Exit(1)
	}

	// --- 4. Get List of Tables from MySQL ---
	// You might want to specify which tables to check explicitly instead of all.
	// For this example, we'll get all non-system tables.
	tableNames, err := databasemodule.GetTableNames(mysqlDB)
	if err != nil {
		MainLogger.Debug(fmt.Sprintf("Failed to get table names from MySQL: %v", err))
	}
	MainLogger.Debug(strings.Join(tableNames[:], ", "))

	if len(tableNames) == 0 {
		MainLogger.Debug("No tables found in MySQL database to check.")
		return
	}

	MainLogger.Debug(fmt.Sprintf("Found %d tables in MySQL: %v\n", len(tableNames), tableNames))

	// --- 5. Iterate through Tables and Verify Data ---
	totalVerifiedRows := 0
	totalMissingRows := 0

	for _, tableName := range tableNames {
		MainLogger.Debug(fmt.Sprintf("\n--- Verifying table: %s ---\n", tableName))

		// IMPORTANT: Adjust primaryKeyColumnName and elasticsearchIndex/DocumentID logic
		// to match how your migration program maps MySQL tables/rows to Elasticsearch.
		// By default, we assume:
		// 1. Elasticsearch index name is the same as the MySQL table name (lowercase).
		// 2. The MySQL primary key column is named 'id' and its value is used as Elasticsearch's _id.
		primaryKeyColumnName := "ID" // Adjust this if your primary key column is named differently
		elasticsearchIndex := tableName // Often, ES index names are lowercase

		// Get all rows from the MySQL table
		mysqlRows, err := databasemodule.GetMySQLRows(mysqlDB, tableName, primaryKeyColumnName)
		if err != nil {
			MainLogger.Debug(fmt.Sprintf("Error getting rows from MySQL table %s: %v\n", tableName, err))
			continue
		}

		if len(mysqlRows) == 0 {
			MainLogger.Debug(fmt.Sprintf("No rows found in MySQL table '%s'. Skipping verification.\n", tableName))
			continue
		}

		MainLogger.Debug(fmt.Sprintf("Found %d rows in MySQL table '%s'.\n", len(mysqlRows), tableName))

		tableMissingRows := 0
		tableVerifiedRows := 0

		fs, err := filesystem.NewFileStorage(appConfig.Database.Tables)
		if err != nil {
			MainLogger.Debug(fmt.Sprintf("Error getting filesystem instance:%s %v\n", tableName, err))
			continue
		}

		registeredTable, err := fs.GetTable(tableName)
		if err != nil {
			MainLogger.Debug(fmt.Sprintf("Error getting table:%s %v\n", tableName, err))
			continue
		}


		for _, mysqlRow := range mysqlRows {
			primaryKeyValue, err := mysqlRow.GeneratePK(*registeredTable.Columns)
			if err != nil {
				MainLogger.Debug(fmt.Sprintf("Warning: Primary key column '%s' not found in row from table '%s'. Skipping row.\n", primaryKeyColumnName, tableName))
				continue
			}

			// Check if the document exists in Elasticsearch
			exists, err := checkElasticsearchDocument(esClient, elasticsearchIndex, primaryKeyValue, mysqlRow)
			if err != nil {
				MainLogger.Debug(fmt.Sprintf("Error checking document %s/%s in Elasticsearch: %v\n", elasticsearchIndex, primaryKeyValue, err))
				// Consider this a failure for reporting purposes
				tableMissingRows++
				continue
			}

			if exists {
				// Optional: Fetch the document and perform a deeper comparison.
				// This requires careful handling of data types and potential transformations.
				MainLogger.Debug(fmt.Sprintf("  [OK] Row with ID '%s' in table '%s' successfully found in Elasticsearch index '%s'.\n", primaryKeyValue, tableName, elasticsearchIndex))
				tableVerifiedRows++
			}
		}
		MainLogger.Debug(fmt.Sprintf("--- Summary for table '%s': Verified: %d, Missing: %d ---\n", tableName, tableVerifiedRows, tableMissingRows))
		totalVerifiedRows += tableVerifiedRows
		totalMissingRows += tableMissingRows
	}

	MainLogger.Debug(fmt.Sprintf("\n--- Verification Complete ---"))
	MainLogger.Debug(fmt.Sprintf("Total Rows Verified: %d\n", totalVerifiedRows))
	MainLogger.Debug(fmt.Sprintf("Total Rows Missing in Elasticsearch: %d\n", totalMissingRows))

	if totalMissingRows > 0 {
		MainLogger.Debug("WARNING: Some rows were found to be missing in Elasticsearch. Please investigate.")
	} else {
		MainLogger.Debug("SUCCESS: All checked rows were found in Elasticsearch.")
	}
}

// getTableNames retrieves a list of table names from the MySQL database.
// checkElasticsearchDocument checks if a document with a given ID exists in an Elasticsearch index.
func checkElasticsearchDocument(client *elasticsearch.Client, index, docID string, dbRecord table.DbRecord) (bool, error) {
	reqGet := esapi.GetRequest{
		Index:      index,
		DocumentID: docID,
	}
	resGet, err := reqGet.Do(context.Background(), client)
	if err != nil {
		return false, fmt.Errorf("Error performing GET request: %s", err)
	}
	defer resGet.Body.Close()
	if resGet.IsError() {
		if resGet.StatusCode == 404 {
			return false, fmt.Errorf("Document with ID '%s' not found in index '%s %v'\n", docID, index, resGet)
		} else {
			return false, fmt.Errorf("Error getting document: %s", resGet.Status())
		}
	}

	var r map[string]any
	if err := json.NewDecoder(resGet.Body).Decode(&r); err != nil {
		return false, fmt.Errorf("Error parsing the response body: %s", err)
	}

	// Access the document source
	source, ok := r["_source"].(map[string]any)
	if !ok {
		return false, fmt.Errorf("Error asserting _source to map[string]interface{}")
	}

	for key, esValue := range source {
		dbValue, ok := dbRecord.ColValues[key]
        if !ok {
            // This means a field exists in ES but not in your DB record.
            // You might want to skip it, or consider it an error.
            // For now, let's just log it and potentially skip.
            // fmt.Printf("Warning: Key '%s' found in Elasticsearch document but not in DB record.\n", key)
			return false, fmt.Errorf("A field exists in ES but not in your DB record: %s", key)
        }

		switch v := esValue.(type) {
        case string:
            // Compare string from ES with string from DB (or convert DB []byte to string)
            dbValStr, dbIsString := dbValue.(string)
            dbValBytes, dbIsBytes := dbValue.([]byte)

            if dbIsString && v == dbValStr {
                continue
            } else if dbIsBytes && v == string(dbValBytes) {
                continue
            } else {
                return false, fmt.Errorf("value mismatch for key '%s': ES '%v' (type %T) != DB '%v' (type %T)", key, esValue, esValue, dbValue, dbValue)
            }
		case float64: // Numbers from ES are typically float64 (e.g., 1375.0)
            // Handle DB values that could be int, int64, or []byte (string representation of number)
            dbValInt, dbIsInt := dbValue.(int)
            dbValInt64, dbIsInt64 := dbValue.(int64)
            dbValBytes, dbIsBytes := dbValue.([]byte) // This is the new problematic type

            if dbIsInt && v == float64(dbValInt) {
                continue
            } else if dbIsInt64 && v == float64(dbValInt64) {
                continue
            } else if dbIsBytes { // If DB value is []byte, try to convert it to a number
                dbStr := string(dbValBytes) // Convert []byte to string "1375"
                parsedFloat, err := strconv.ParseFloat(dbStr, 64)
                if err == nil && v == parsedFloat { // Compare as float64
                    continue
                } else {
                    return false, fmt.Errorf("value mismatch for key '%s': ES '%v' (type %T) != DB '%v' (type %T) (Failed to parse or mismatch for []byte)", key, esValue, esValue, dbValue, dbValue)
                }
            } else {
                // Fallback for other non-float64 numeric types from DB that don't match
                return false, fmt.Errorf("value mismatch for key '%s': ES '%v' (type %T) != DB '%v' (type %T)", key, esValue, esValue, dbValue, dbValue)
            }
        // Add more cases for other types if necessary (e.g., bool, map[string]interface{}, []interface{})
        // For example, if you have boolean fields:
        case bool:
            dbValBool, dbIsBool := dbValue.(bool)
            if dbIsBool && v == dbValBool {
                continue
            } else {
                 return false, fmt.Errorf("value mismatch for key '%s': ES '%v' (type %T) != DB '%v' (type %T)", key, esValue, esValue, dbValue, dbValue)
            }
        default:
            // Fallback for types not explicitly handled.
            // This will still perform a direct interface comparison.
            if dbValue != esValue {
                 return false, fmt.Errorf("value mismatch for key '%s' (unhandled type): ES '%v' (type %T) != DB '%v' (type %T)", key, esValue, esValue, dbValue, dbValue)
            }
        }
	}

	return true, nil
}
