package database

import (
	"fmt"
	"strconv"

	"github.com/k1-end/mysql-2-elastic/internal/table"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
)

func ExtractValue(expr ast.ExprNode) (any, error) {
	switch v := expr.(type) {
	case ast.ValueExpr:
		val := v.GetValue()
		switch vVal := val.(type) {
		case int64:
			return vVal, nil
		case uint64:
			return vVal, nil
		case float64:
			return vVal, nil
		case string:
			return vVal, nil
		case []byte:
			s := string(vVal)
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f, nil
			}
			return s, nil
		case *test_driver.MyDecimal:
			s := vVal.String()
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f, nil
			}
			return s, nil
		case nil:
			return nil, nil
		default:
			return nil, fmt.Errorf("unsupported ValueExpr type: %T for value %v", val, val)
		}
	case *ast.DefaultExpr:
		return "DEFAULT", nil
	case *ast.UnaryOperationExpr:
		op := v.Op.String()
		operand, err := ExtractValue(v.V)
		if err != nil {
			return nil, fmt.Errorf("error parsing unary expression: %w", err)
		}
		if op == "minus" {
			if f, err := strconv.ParseFloat("-"+fmt.Sprintf("%v", operand), 64); err == nil {
				return f, nil
			}
		}
		return op + fmt.Sprintf("%v", operand), nil
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
	return table.ColumnInfo{}, fmt.Errorf("column not found at position %d", position)
}
