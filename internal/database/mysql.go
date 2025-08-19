package database

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

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

// InsertRowIntoTable safely inserts a new row into the specified table using a parameterized query.
func InsertRowIntoTable(db *sql.DB, tableName string, dbRecord tablepack.DbRecord) (int64, error) {

	// Get the keys from the map and sort them
	keys := make([]string, 0, len(dbRecord.ColValues))
	for k := range dbRecord.ColValues {
		keys = append(keys, k)
	}
	sort.Strings(keys) // Sort the column names alphabetically

	// Now, iterate over the sorted keys to build your slices in a predictable order
	colNames := make([]string, 0, len(keys))
	placeholders := make([]string, 0, len(keys))
	args := make([]any, 0, len(keys))

	for _, col := range keys {
		val := dbRecord.ColValues[col]
		colNames = append(colNames, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
		args = append(args, val)
	}

	// Build the final query string.
	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES (%s)",
		tableName,
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
		)

	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() // The rollback will be called unless we commit


	// Prepare the statement on the transaction
	stmt, err := tx.Prepare(query)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	// Execute the prepared statement
	result, err := stmt.Exec(args...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		// Handle error retrieving rows affected
		return 0, err
	}

	if rowsAffected == 0 {
		// This indicates a silent failure where the database rejected the insert.
		// You may want to log more details here.
		return 0, fmt.Errorf("no rows were inserted, possibly due to a constraint violation")
	}

	fmt.Println("No error")
	fmt.Println("Rows effected", rowsAffected)
	lastInsertedId, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	fmt.Println("lastInsertedId", lastInsertedId)
	// If all operations were successful, commit the transaction.
	err = tx.Commit()
	return lastInsertedId, err
}

