package database

import (
	"database/sql"
	"fmt"
)

// getMySQLRows retrieves all rows from a given MySQL table.
// It returns a slice of maps, where each map represents a row
// and keys are column names.
func GetMySQLRows(db *sql.DB, tableName, primaryKeyColumn string) ([]map[string]interface{}, error) {
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

func GetTableNames(db *sql.DB) ([]string, error) {
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


