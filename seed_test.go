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

	_ "github.com/go-sql-driver/mysql"
	"github.com/k1-end/mysql-2-elastic/internal/config"
	databasemodule "github.com/k1-end/mysql-2-elastic/internal/database"
	"github.com/olivere/elastic/v7"
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
	esClient, err := elastic.NewClient(
		elastic.SetURL(appConfig.Elastic.Address ),
		elastic.SetSniff(false), // Disable sniffing for local/single-node setups if needed
		elastic.SetHealthcheckInterval(10*time.Second),
		elastic.SetInfoLog(MainLogWriter),
		elastic.SetErrorLog(MainLogWriter),
		elastic.SetBasicAuth(appConfig.Elastic.Username, appConfig.Elastic.Password),
	)
	if err != nil {
		MainLogger.Error(fmt.Sprintf("Error creating Elasticsearch client: %v", err))
		panic(err)
	}

	// Ping the Elasticsearch cluster to ensure connection
	info, code, err := esClient.Ping(appConfig.Elastic.Address).Do(context.Background())
	if err != nil {
		MainLogger.Error(fmt.Sprintf("Error pinging Elasticsearch: %v", err))
		panic(err)
	}
	MainLogger.Debug(fmt.Sprintf("Successfully connected to Elasticsearch. Version: %s, Code: %d\n", info.Version.Number, code))

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
		primaryKeyColumnName := "id" // Adjust this if your primary key column is named differently
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

		for _, mysqlRow := range mysqlRows {
			primaryKeyValue, ok := mysqlRow[primaryKeyColumnName]
			if !ok {
				MainLogger.Debug(fmt.Sprintf("Warning: Primary key column '%s' not found in row from table '%s'. Skipping row.\n", primaryKeyColumnName, tableName))
				continue
			}

			// Convert primary key value to string for Elasticsearch _id
			docID := fmt.Sprintf("%v", primaryKeyValue)

			// Check if the document exists in Elasticsearch
			exists, err := checkElasticsearchDocument(esClient, elasticsearchIndex, docID)
			if err != nil {
				MainLogger.Debug(fmt.Sprintf("Error checking document %s/%s in Elasticsearch: %v\n", elasticsearchIndex, docID, err))
				// Consider this a failure for reporting purposes
				tableMissingRows++
				continue
			}

			if exists {
				// Optional: Fetch the document and perform a deeper comparison.
				// This requires careful handling of data types and potential transformations.
				// For now, we'll just confirm existence.
				// Example of fetching and printing (not a full comparison):
				// getResponse, err := esClient.Get().Index(elasticsearchIndex).Id(docID).Do(context.Background())
				// if err == nil && getResponse.Found {
				// 	log.Printf("  Row with ID '%s' found in ES. Source: %s\n", docID, *getResponse.Source)
				// }

				MainLogger.Debug(fmt.Sprintf("  [OK] Row with ID '%s' in table '%s' successfully found in Elasticsearch index '%s'.\n", docID, tableName, elasticsearchIndex))
				tableVerifiedRows++
			} else {
				MainLogger.Debug(fmt.Sprintf("  [FAIL] Row with ID '%s' in table '%s' IS MISSING from Elasticsearch index '%s'.\n", docID, tableName, elasticsearchIndex))
				tableMissingRows++
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
func checkElasticsearchDocument(client *elastic.Client, index, docID string) (bool, error) {
	// We only need to check for existence, so a Head request is efficient.
	// You could also use a Get request and check getResponse.Found.
	exists, err := client.Exists().Index(index).Id(docID).Do(nil)
	if err != nil {
		// Log the error but don't fail the whole program
		return false, fmt.Errorf("Elasticsearch exists check failed for index '%s', ID '%s': %w", index, docID, err)
	}
	return exists, nil
}

// --- Optional: For a more thorough comparison (advanced) ---
// compareDocuments compares two maps representing documents.
// This is a basic example and might need to be more sophisticated
// depending on your data types and transformations.
func compareDocuments(mysqlDoc, esDoc map[string]any) bool {
	// A simple check: are all keys from MySQL present in ES with same values?
	// This does not handle missing keys in MySQL that might be in ES, or type conversions.
	for k, v := range mysqlDoc {
		esVal, ok := esDoc[k]
		if !ok {
			MainLogger.Debug(fmt.Sprintf("  Key '%s' missing in Elasticsearch document.\n", k))
			return false
		}
		if !reflect.DeepEqual(v, esVal) {
			MainLogger.Debug(fmt.Sprintf("  Value mismatch for key '%s': MySQL='%v', ES='%v'\n", k, v, esVal))
			return false
		}
	}
	return true
}

// prettyPrintJSON prints a map as indented JSON.
func prettyPrintJSON(data map[string]any) string {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		MainLogger.Debug(fmt.Sprintf("Error marshalling JSON: %v", err))
		return err.Error()
	}
	return string(b)
}

