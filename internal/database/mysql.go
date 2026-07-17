package database

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/k1-end/mysql-2-elastic/internal/table"
)

func GetMySQLRows(db *sql.DB, tableName string) ([]table.DbRecord, error) {
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

	var records []table.DbRecord
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row from table %s: %w", tableName, err)
		}

		rec := table.DbRecord{ColValues: make(map[string]any)}
		for i, colName := range columns {
			rec.ColValues[colName] = values[i]
		}
		records = append(records, rec)
	}

	return records, rows.Err()
}

func GetTableNames(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("failed to execute SHOW TABLES: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, name)
	}
	return tables, nil
}

func InsertRowIntoTable(db *sql.DB, tableName string, record table.DbRecord) (int64, error) {
	keys := make([]string, 0, len(record.ColValues))
	for k := range record.ColValues {
		keys = append(keys, k)
	}

	colNames := make([]string, 0, len(keys))
	placeholders := make([]string, 0, len(keys))
	args := make([]any, 0, len(keys))

	for _, col := range keys {
		colNames = append(colNames, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
		args = append(args, record.ColValues[col])
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES (%s)",
		tableName,
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
	)

	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(query)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	result, err := stmt.Exec(args...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute insert: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return 0, fmt.Errorf("no rows inserted for table %s", tableName)
	}

	lastID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get last insert id: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return lastID, nil
}
