package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/olivere/elastic/v7"
)


func TestSeed(t *testing.T) {

	log.Println("Starting MySQL to Elasticsearch migration verification program...")

	// --- 1. Load Configuration ---
	// In a real application, you might load this from a file or environment variables.
	// For simplicity, we'll hardcode it here.
	// IMPORTANT: Replace with your actual credentials and URLs!
	// cfg := Config{
	// 	MySQLDSN:       "root:password@tcp(127.0.0.1:3306)/your_mysql_db?parseTime=true", // Example DSN
	// 	ElasticsearchURL: "http://localhost:9200",                                    // Example ES URL
	// }

	// --- 2. Connect to MySQL ---
	mysqlDB, err := sql.Open("mysql", AppConfiguration.Database.Username + ":" + AppConfiguration.Database.Password + "@tcp(" + AppConfiguration.Database.Host + ":" + strconv.FormatInt(int64(AppConfiguration.Database.Port), 10) + ")/" + AppConfiguration.Database.Name + "?parseTime=true")
	if err != nil {
		log.Fatalf("Error opening MySQL connection: %v", err)
	}
	defer mysqlDB.Close()

	if err = mysqlDB.Ping(); err != nil {
		log.Fatalf("Error connecting to MySQL: %v", err)
	}
	log.Println("Successfully connected to MySQL.")

	// --- 3. Connect to Elasticsearch ---
	esClient, err := elastic.NewClient(
		elastic.SetURL(AppConfiguration.Elastic.Address ),
		elastic.SetSniff(false), // Disable sniffing for local/single-node setups if needed
		elastic.SetHealthcheckInterval(10*time.Second),
		elastic.SetInfoLog(log.New(os.Stdout, "ELASTIC_INFO: ", log.Ldate|log.Ltime|log.Lshortfile)),
		elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC_ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)),
		elastic.SetBasicAuth(AppConfiguration.Elastic.Username, AppConfiguration.Elastic.Password),
	)
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %v", err)
	}

	// Ping the Elasticsearch cluster to ensure connection
	info, code, err := esClient.Ping(AppConfiguration.Elastic.Address).Do(context.Background())
	if err != nil {
		log.Fatalf("Error pinging Elasticsearch: %v", err)
	}
	log.Printf("Successfully connected to Elasticsearch. Version: %s, Code: %d\n", info.Version.Number, code)
	fmt.Println(info)

	// --- 4. Get List of Tables from MySQL ---
	// You might want to specify which tables to check explicitly instead of all.
	// For this example, we'll get all non-system tables.
	tableNames, err := getTableNames(mysqlDB)
	if err != nil {
		log.Fatalf("Failed to get table names from MySQL: %v", err)
	}
	fmt.Println(tableNames)

	if len(tableNames) == 0 {
		log.Println("No tables found in MySQL database to check.")
		return
	}

	log.Printf("Found %d tables in MySQL: %v\n", len(tableNames), tableNames)

	// --- 5. Iterate through Tables and Verify Data ---
	totalVerifiedRows := 0
	totalMissingRows := 0

	for _, tableName := range tableNames {
		log.Printf("\n--- Verifying table: %s ---\n", tableName)

		// IMPORTANT: Adjust primaryKeyColumnName and elasticsearchIndex/DocumentID logic
		// to match how your migration program maps MySQL tables/rows to Elasticsearch.
		// By default, we assume:
		// 1. Elasticsearch index name is the same as the MySQL table name (lowercase).
		// 2. The MySQL primary key column is named 'id' and its value is used as Elasticsearch's _id.
		primaryKeyColumnName := "id" // Adjust this if your primary key column is named differently
		elasticsearchIndex := tableName // Often, ES index names are lowercase

		// Get all rows from the MySQL table
		mysqlRows, err := getMySQLRows(mysqlDB, tableName, primaryKeyColumnName)
		if err != nil {
			log.Printf("Error getting rows from MySQL table %s: %v\n", tableName, err)
			continue
		}

		if len(mysqlRows) == 0 {
			log.Printf("No rows found in MySQL table '%s'. Skipping verification.\n", tableName)
			continue
		}

		log.Printf("Found %d rows in MySQL table '%s'.\n", len(mysqlRows), tableName)

		tableMissingRows := 0
		tableVerifiedRows := 0

		for _, mysqlRow := range mysqlRows {
			primaryKeyValue, ok := mysqlRow[primaryKeyColumnName]
			if !ok {
				log.Printf("Warning: Primary key column '%s' not found in row from table '%s'. Skipping row.\n", primaryKeyColumnName, tableName)
				continue
			}

			// Convert primary key value to string for Elasticsearch _id
			docID := fmt.Sprintf("%v", primaryKeyValue)

			// Check if the document exists in Elasticsearch
			exists, err := checkElasticsearchDocument(esClient, elasticsearchIndex, docID)
			if err != nil {
				log.Printf("Error checking document %s/%s in Elasticsearch: %v\n", elasticsearchIndex, docID, err)
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

				log.Printf("  [OK] Row with ID '%s' in table '%s' successfully found in Elasticsearch index '%s'.\n", docID, tableName, elasticsearchIndex)
				tableVerifiedRows++
			} else {
				log.Printf("  [FAIL] Row with ID '%s' in table '%s' IS MISSING from Elasticsearch index '%s'.\n", docID, tableName, elasticsearchIndex)
				tableMissingRows++
			}
		}
		log.Printf("--- Summary for table '%s': Verified: %d, Missing: %d ---\n", tableName, tableVerifiedRows, tableMissingRows)
		totalVerifiedRows += tableVerifiedRows
		totalMissingRows += tableMissingRows
	}

	log.Printf("\n--- Verification Complete ---")
	log.Printf("Total Rows Verified: %d\n", totalVerifiedRows)
	log.Printf("Total Rows Missing in Elasticsearch: %d\n", totalMissingRows)

	if totalMissingRows > 0 {
		log.Println("WARNING: Some rows were found to be missing in Elasticsearch. Please investigate.")
	} else {
		log.Println("SUCCESS: All checked rows were found in Elasticsearch.")
	}
}

// getTableNames retrieves a list of table names from the MySQL database.
func getTableNames(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("failed to execute SHOW TABLES: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		// You might want to filter out system tables if necessary
		// if !strings.HasPrefix(tableName, "sys") && !strings.HasPrefix(tableName, "mysql") {
		tables = append(tables, tableName)
		// }
	}
	return tables, nil
}

// getMySQLRows retrieves all rows from a given MySQL table.
// It returns a slice of maps, where each map represents a row
// and keys are column names.
func getMySQLRows(db *sql.DB, tableName, primaryKeyColumn string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM `%s`", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query table %s: %w", tableName, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns for table %s: %w", tableName, err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		// Create a slice of interface{} to hold the values for scanning
		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row from table %s: %w", tableName, err)
		}

		rowMap := make(map[string]interface{})
		for i, colName := range columns {
			val := values[i]
			if valBytes, ok := val.([]byte); ok {
				// Handle byte slices (e.g., VARCHAR, TEXT, BLOB) by converting to string
				rowMap[colName] = string(valBytes)
			} else {
				rowMap[colName] = val
			}
		}
		results = append(results, rowMap)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration for table %s: %w", tableName, err)
	}

	return results, nil
}

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
func compareDocuments(mysqlDoc, esDoc map[string]interface{}) bool {
	// A simple check: are all keys from MySQL present in ES with same values?
	// This does not handle missing keys in MySQL that might be in ES, or type conversions.
	for k, v := range mysqlDoc {
		esVal, ok := esDoc[k]
		if !ok {
			log.Printf("  Key '%s' missing in Elasticsearch document.\n", k)
			return false
		}
		if !reflect.DeepEqual(v, esVal) {
			log.Printf("  Value mismatch for key '%s': MySQL='%v', ES='%v'\n", k, v, esVal)
			return false
		}
	}
	return true
}

// prettyPrintJSON prints a map as indented JSON.
func prettyPrintJSON(data map[string]interface{}) string {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Sprintf("Error marshalling JSON: %v", err)
	}
	return string(b)
}

