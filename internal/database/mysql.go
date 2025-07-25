package database

import (
	"database/sql"
	"fmt"

	tablepack "github.com/k1-end/mysql-2-elastic/internal/table"
)

// getMySQLRows retrieves all rows from a given MySQL table.
// It returns a slice of maps, where each map represents a row
// and keys are column names.
func GetMySQLRows(db *sql.DB, tableName, primaryKeyColumn string) ([]tablepack.DbRecord, error) {
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

	var records []tablepack.DbRecord
	for rows.Next() {
		// Create a slice of any to hold the values for scanning
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row from table %s: %w", tableName, err)
		}

        var singleRecord tablepack.DbRecord
		singleRecord.ColValues = make(map[string]any)
		for i, colName := range columns {
			val := values[i]
            singleRecord.ColValues[colName] = val
		}

		records = append(records, singleRecord)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration for table %s: %w", tableName, err)
	}

	return records, nil
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


