package main

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type ColumnInfo struct {
	Name string
	Type string
}

func extractValue(expr ast.ExprNode) (interface{}, error) {
    switch v := expr.(type) {
    case ast.ValueExpr:
        return v.GetValue(), nil
    case *ast.DefaultExpr:
        return "DEFAULT", nil
    default:
        return nil, fmt.Errorf("unsupported expression type: %T", expr)
    }
}


func getColumnNameFromPosition(tableStructure []map[string]interface{}, position int) (string, error) {
    for _, col := range tableStructure {
        if pos, ok := col["position"].(float64); ok {
            if int(pos) == position {
                if name, ok := col["name"].(string); ok {
                    return name, nil
                }
            }
        }
    }
    return "", fmt.Errorf("Column Not found")
}


func processInsertString(tableName string, insertStatement string, tableStructure []map[string]interface{}) error {
    p := parser.New()
    // Parse the SQL statement
    // The last two arguments are charset and collation, which can be empty for default.
    stmtNodes, _, err := p.Parse(insertStatement, "", "")
    if err != nil {
        return err
    }
    if len(stmtNodes) == 0 {
        return fmt.Errorf("No statements found.")
    }

    // We expect a single INSERT statement
    insertStmt, ok := stmtNodes[0].(*ast.InsertStmt)
    if !ok {
        return fmt.Errorf("The provided SQL is not an INSERT statement.")
    }
    var values []map[string]interface{} 
    for i, row := range insertStmt.Lists {
        var singleValues = make(map[string]interface{})
        for j, expr := range row {
            val, err := extractValue(expr)
            if err != nil {
                fmt.Printf("Error extracting value for column %d in row %d: %v\n", j+1, i+1, err)
                continue
            }
            columnName, err := getColumnNameFromPosition(tableStructure, j)
            if err != nil {
                return fmt.Errorf("Error getting column name from position %d: %w", j, err)
            }
            singleValues[columnName] = val
        }
        values = append(values, singleValues)
    }

    err = bulkSendToElastic(tableName, values)
    if err != nil {
        return err
    }
    return nil
}
