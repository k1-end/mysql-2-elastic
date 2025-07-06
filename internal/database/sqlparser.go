package database

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser/ast"
)


func ExtractValue(expr ast.ExprNode) (interface{}, error) {
    switch v := expr.(type) {
    case ast.ValueExpr:
        return v.GetValue(), nil
    case *ast.DefaultExpr:
        return "DEFAULT", nil
    case *ast.UnaryOperationExpr:
        return v.Text(), nil

    default:
        return nil, fmt.Errorf("unsupported expression type: %T", expr)
    }
}


func GetColumnNameFromPosition(tableStructure []map[string]interface{}, position int) (string, error) {
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


