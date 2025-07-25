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
		switch v_val := val.(type) {
		case int64:
			return v_val, nil
		case uint64: // For unsigned integers
			return v_val, nil
		case float64:
			return v_val, nil
		case string:
			return v_val, nil
		case []byte:
			// This is the most likely culprit for decimal values from the parser's ValueExpr,
			// or for string literals that are returned as []byte.
			s := string(v_val) // Convert the byte slice to a string
			// Attempt to parse it as a float. If it's a valid decimal string, this will succeed.
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f, nil // Return as float64
			}
			// If it's not a valid float (e.g., it was binary data, or a string that's not a number),
			// return it as a string.
			return s, nil

		// If the parser *ever* returns a specific decimal type (like pingcap/tidb/pkg/types.Decimal
		// or pingcap/parser/types.BinaryLiteral), you would add a case here:
		// case tidb_types.BinaryLiteral: // Assuming tidb_types is an alias for the specific types package
		//     s := v_val.String()
		//     if f, err := strconv.ParseFloat(s, 64); err == nil {
		//         return f, nil
		//     }
		//     return s, nil // Fallback to string if parsing fails or precision is critical
		case *test_driver.MyDecimal: // <--- NEW CASE ADDED HERE!
			// MyDecimal has a String() method and can also be converted to float64.
			// It's generally safer to convert to string first to preserve precision,
			// then parse to float64 if numerical comparisons are expected.
			s := v_val.String()
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f, nil // Convert to float64 for compatibility with Elasticsearch
			}
			return s, nil // Fallback to string if parsing to float fails or precision is critical

		case nil: // <--- NEW CASE ADDED HERE!
			// This handles SQL NULL values. In Go, SQL NULL is typically represented as a Go nil.
			return nil, nil // Return Go's nil and no error.
		default:
			return nil, fmt.Errorf("unsupported ValueExpr type for value %v (type %T)", val, val)
		}
	case *ast.DefaultExpr:
		return "DEFAULT", nil
	case *ast.UnaryOperationExpr:
		// For simple unary ops like '-5' or 'NOT true', Text() gives the literal string.
		// For more complex expressions, you might need to recursively evaluate v.V.
		op := v.Op.String()
		operand, err := ExtractValue(v.V)  
		if err != nil {
			return nil, fmt.Errorf("error parsing UnaryOperationExpr %w", err)
		}
		if op == "minus" {
			
			if f, err := strconv.ParseFloat("-" + fmt.Sprintf("%v", operand), 64); err == nil {
				return f, nil // Convert to float64 for compatibility with Elasticsearch
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
    return table.ColumnInfo{}, fmt.Errorf("Column Not found")
}


