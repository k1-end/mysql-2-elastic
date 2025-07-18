package database

import (
	"fmt"

	"github.com/k1-end/mysql-2-elastic/internal/table"
	"github.com/pingcap/tidb/pkg/parser/ast"
)


func ExtractValue(expr ast.ExprNode) (any, error) {
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


func GetColumnFromPosition(tableCols []table.ColumnInfo, position int) (table.ColumnInfo, error) {
    for _, col := range tableCols {
		if col.Position == position {
			return col, nil
		}
    }
    return table.ColumnInfo{}, fmt.Errorf("Column Not found")
}


