package dump

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/k1-end/mysql-2-elastic/internal/config"
	"github.com/k1-end/mysql-2-elastic/internal/table"
	"github.com/k1-end/mysql-2-elastic/internal/util"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func GetTableColsInfoFromDumpFile(dumpFilePath string) ([]table.ColumnInfo, error) {
	createStmt, err := getCreateTableStatement(dumpFilePath)
	if err != nil {
		return nil, err
	}
	return columnsFromCreateStatement(createStmt)
}

func columnsFromCreateStatement(cts *ast.CreateTableStmt) ([]table.ColumnInfo, error) {
	primaryKeyColumns := make(map[string]bool)
	for _, constraint := range cts.Constraints {
		if constraint.Tp == ast.ConstraintPrimaryKey {
			for _, keyPart := range constraint.Keys {
				primaryKeyColumns[keyPart.Column.Name.O] = true
			}
			break
		}
	}

	var cols []table.ColumnInfo
	for i, colDef := range cts.Cols {
		colType := colDef.Tp.String()
		if strings.Contains(strings.ToLower(colType), "enum") {
			return nil, fmt.Errorf("enum types are not supported yet")
		}
		cols = append(cols, table.ColumnInfo{
			Name:           colDef.Name.Name.O,
			Type:           colType,
			Position:       i,
			IsInPrimaryKey: primaryKeyColumns[colDef.Name.Name.O],
		})
	}
	return cols, nil
}

func getCreateTableStatement(dumpFilePath string) (*ast.CreateTableStmt, error) {
	f, err := os.Open(dumpFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open dump file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var buf bytes.Buffer
	inCreate := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "CREATE TABLE") {
			inCreate = true
			buf.WriteString(line)
		} else if inCreate {
			buf.WriteString(" " + line)
			if strings.HasSuffix(line, ";") {
				inCreate = false
				break
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading dump file: %w", err)
	}

	stmtStr := buf.String()
	if stmtStr == "" {
		return nil, fmt.Errorf("CREATE TABLE statement not found in %s", dumpFilePath)
	}

	p := parser.New()
	nodes, _, err := p.Parse(stmtStr, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to parse CREATE TABLE: %w", err)
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no statements found")
	}
	stmt, ok := nodes[0].(*ast.CreateTableStmt)
	if !ok {
		return nil, fmt.Errorf("statement is not a CREATE TABLE")
	}
	return stmt, nil
}

// GetBinlogCoordinates extracts the master binlog position from a dump file.
func GetBinlogCoordinates(dumpFilePath string) (table.BinlogPosition, error) {
	f, err := os.Open(dumpFilePath)
	if err != nil {
		return table.BinlogPosition{}, fmt.Errorf("failed to open dump file: %w", err)
	}
	defer f.Close()

	re := regexp.MustCompile(`MASTER_LOG_FILE='(.*)', MASTER_LOG_POS=(\d+)`)
	reader := bufio.NewReader(f)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return table.BinlogPosition{}, fmt.Errorf("error reading dump file: %w", err)
		}
		if matches := re.FindStringSubmatch(line); len(matches) == 3 {
			pos, err := strconv.ParseUint(matches[2], 10, 32)
			if err != nil {
				return table.BinlogPosition{}, fmt.Errorf("failed to parse binlog position: %w", err)
			}
			return table.BinlogPosition{Logfile: matches[1], Logpos: uint32(pos)}, nil
		}
	}
	return table.BinlogPosition{}, fmt.Errorf("binlog coordinates not found in %s", dumpFilePath)
}

// InitialDump runs mysqldump to create a dump file for the given table.
func InitialDump(tableName string, cfg *config.Config, log *slog.Logger, store interface {
	GetDumpFileDirectory(tableName string) string
	GetDumpFilePath(tableName string) (string, error)
}) error {
	args := []string{
		"--skip-ssl-verify",
		"--single-transaction",
		"--master-data=2",
		fmt.Sprintf("--user=%s", cfg.Database.Username),
		fmt.Sprintf("--password=%s", cfg.Database.Password),
		fmt.Sprintf("--host=%s", cfg.Database.Host),
		fmt.Sprintf("--port=%d", cfg.Database.Port),
		cfg.Database.Name,
		tableName,
	}
	log.Debug("running mysqldump", "args", args)

	cmd := exec.CommandContext(context.Background(), cfg.MysqlDumpPath, args...)

	if err := util.CreateDirectoryIfNotExists(store.GetDumpFileDirectory(tableName)); err != nil {
		return fmt.Errorf("failed to create dump directory: %w", err)
	}
	outputFile, err := store.GetDumpFilePath(tableName)
	if err != nil {
		return fmt.Errorf("failed to get dump file path: %w", err)
	}
	outfile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outfile.Close()

	errPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to get stderr pipe: %w", err)
	}
	cmd.Stdout = outfile

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start mysqldump: %w", err)
	}

	errScanner := bufio.NewScanner(errPipe)
	for errScanner.Scan() {
		log.Debug("mysqldump", "stderr", errScanner.Text())
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("mysqldump failed: %w", err)
	}
	return nil
}

// ClearIncompleteDump removes a partial dump file if it exists.
func ClearIncompleteDump(dumpFilePath string) error {
	_, err := os.Stat(dumpFilePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to stat dump file %s: %w", dumpFilePath, err)
	}
	return os.RemoveAll(dumpFilePath)
}

// ReadLastOffset reads a saved byte offset from a file, returning 0 on any error.
func ReadLastOffset(filePath string) (int64, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0, nil
	}
	offset, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse offset from %s: %w", filePath, err)
	}
	return offset, nil
}

// WriteCurrentOffset writes a byte offset to a file.
func WriteCurrentOffset(filePath string, offset int64) error {
	return os.WriteFile(filePath, []byte(strconv.FormatInt(offset, 10)), 0644)
}

// ParseInsertStatements parses a raw INSERT statement into DbRecords.
func ParseInsertStatements(insertSQL string, cols []table.ColumnInfo, log *slog.Logger) ([]table.DbRecord, error) {
	p := parser.New()
	nodes, _, err := p.Parse(insertSQL, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to parse INSERT: %w", err)
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no statements found")
	}

	insertStmt, ok := nodes[0].(*ast.InsertStmt)
	if !ok {
		return nil, fmt.Errorf("statement is not an INSERT")
	}

	colMap := table.ColumnMap(cols)
	var records []table.DbRecord
	for i, row := range insertStmt.Lists {
		rec := table.DbRecord{ColValues: make(map[string]any)}
		for j, expr := range row {
			val, err := extractValueSafe(expr)
			if err != nil {
				return nil, fmt.Errorf("error extracting value at col %d, row %d: %w", j+1, i+1, err)
			}
			col, ok := colMap[j]
			if !ok {
				return nil, fmt.Errorf("no column at position %d", j)
			}
			rec.ColValues[col.Name] = val
			if col.IsInPrimaryKey {
				rec.PrimaryKey += fmt.Sprintf("%v", val)
			}
		}
		records = append(records, rec)
	}
	return records, nil
}

func extractValueSafe(expr ast.ExprNode) (any, error) {
	switch v := expr.(type) {
	case ast.ValueExpr:
		val := v.GetValue()
		switch vVal := val.(type) {
		case nil:
			return nil, nil
		case int64, uint64, float64, string:
			return vVal, nil
		case []byte:
			s := string(vVal)
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f, nil
			}
			return s, nil
		default:
			return fmt.Sprintf("%v", vVal), nil
		}
	case *ast.UnaryOperationExpr:
		operand, err := extractValueSafe(v.V)
		if err != nil {
			return nil, err
		}
		if v.Op.String() == "minus" {
			if f, err := strconv.ParseFloat("-"+fmt.Sprintf("%v", operand), 64); err == nil {
				return f, nil
			}
		}
		return v.Op.String() + fmt.Sprintf("%v", operand), nil
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}
