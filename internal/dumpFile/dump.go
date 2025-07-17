package dumpfile

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
	"github.com/k1-end/mysql-2-elastic/internal/storage"
	"github.com/k1-end/mysql-2-elastic/internal/table"
	"github.com/k1-end/mysql-2-elastic/internal/syncer"
	"github.com/k1-end/mysql-2-elastic/internal/util"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver" // Required for the parser to work
)

type columnData struct {
    Name string `json:"name"`
    Type string `json:"type"`
    Position int `json:"position"`
}

func GetTableColsInfoFromDumpFile(dumpFilePath string) ([]table.ColumnInfo, error) {
	createStatementStr, err := getCreateTableStatementFromDumpFileAsString(dumpFilePath)
	if err != nil {
		return nil, err
	}

	createStatement, err := parseCreateStatement(createStatementStr)
	if err != nil {
		return nil, err
	}

	columnsInfo, err := getColumnsInfoFromCreateStatement(createStatement)
	return columnsInfo, nil
}

func getColumnsInfoFromCreateStatement(cts *ast.CreateTableStmt) ([]table.ColumnInfo, error) {
	var columnsInfo []table.ColumnInfo
	position := 0
	for _, colDef := range cts.Cols {
		colName := colDef.Name.Name.O // Column Name
		colType := colDef.Tp.String() // Data Type string representation
		columnsInfo = append(columnsInfo, table.ColumnInfo{
			Name:     colName,
			Type:     colType,
			Position: position,
		})
		position += 1
	}
	return columnsInfo, nil
}

func parseCreateStatement(ctr string) (*ast.CreateTableStmt, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(ctr, "", "")
	if err != nil {
		return nil, err
	}

	if len(stmtNodes) == 0 {
		return nil, fmt.Errorf("No statements found.")
	}

	createTableStmt, ok := stmtNodes[0].(*ast.CreateTableStmt)
	if !ok {
		return nil, fmt.Errorf("The provided SQL is not a CREATE TABLE statement.")
	}
	return createTableStmt, nil
}


func getCreateTableStatementFromDumpFileAsString(dumpFilePath string) (string, error) {
	file, err := os.Open(dumpFilePath)
	if err != nil {
        return "", fmt.Errorf("failed to open dump file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var currentStatement bytes.Buffer
	inCreateTable := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, "CREATE TABLE") {
			inCreateTable = true
			currentStatement.WriteString(line)
		} else if inCreateTable {
			currentStatement.WriteString(" " + line)
			if strings.HasSuffix(line, ";") {
				inCreateTable = false
				createTableStatement := currentStatement.String()
                return createTableStatement, nil
			}
			continue
		}
	}

	return "", fmt.Errorf("CREATE TABLE statement not found in dump file")
}

func GetBinlogCoordinatesFromDumpfile(dumpFilePath string) (syncer.BinlogPosition, error) {
	file, err := os.Open(dumpFilePath)
	if err != nil {
		return syncer.BinlogPosition{}, fmt.Errorf("failed to open dump file for parsing: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	re := regexp.MustCompile(`MASTER_LOG_FILE='(.*)', MASTER_LOG_POS=(\d+)`)

	var logFile string
	var logPos uint32

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return syncer.BinlogPosition{}, fmt.Errorf("error reading dump file: %w", err)
		}
		if matches := re.FindStringSubmatch(line); len(matches) == 3 {
			logFile = matches[1]
			pos, err := strconv.ParseUint(matches[2], 10, 32)
			if err != nil {
				return syncer.BinlogPosition{}, fmt.Errorf("failed to parse binlog position: %w", err)
			}
			logPos = uint32(pos)
			return syncer.BinlogPosition{Logfile: logFile, Logpos: logPos}, nil
		}
	}

	return syncer.BinlogPosition{}, fmt.Errorf("binlog coordinates not found in dump file " + dumpFilePath)
}

func InitialDump(tableName string, appConfig *config.Config, logger *slog.Logger, tableStorage storage.TableStorage) error{
	table, err := tableStorage.GetTable(tableName)
    if err != nil {
        return err
    }
	logger.Debug("Dumping table: " + table.Name)
    if table.Status != "created" {
        return fmt.Errorf("table %s is not in the created state", table.Name)
    }

    // Set the table status to "dumping"
	err = tableStorage.SetTableStatus(table.Name, "dumping")
	if err != nil {
		return fmt.Errorf("set table status %s: %w", table.Name, err)
	}

    args := []string{
        "--skip-ssl-verify",
		"--single-transaction",
		"--master-data=2",
		fmt.Sprintf("--user=%s", appConfig.Database.Username),
		fmt.Sprintf("--password=%s", appConfig.Database.Password),
        fmt.Sprintf("--host=%s", appConfig.Database.Host),
        fmt.Sprintf("--port=%d", appConfig.Database.Port),
		appConfig.Database.Name,
	}

	args = append(args, []string{table.Name}...)
	logger.Debug(strings.Join(args[:], " "))
    ctx := context.Background()
	cmd := exec.CommandContext(
		ctx,
		appConfig.MysqlDumpPath,
		args...,
	)

    util.CreateDirectoryIfNotExists("data/dumps")
    util.CreateDirectoryIfNotExists("data/dumps/" + tableName)
    outputFile, err := tableStorage.GetDumpFilePath(tableName)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
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

    // Read stderr to log any mysqldump errors
	errScanner := bufio.NewScanner(errPipe)
	for errScanner.Scan() {
		logger.Debug(fmt.Sprintf("mysqldump stderr: %s", errScanner.Text()))
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("mysqldump failed: %w", err)
	}

	err = tableStorage.SetTableStatus(table.Name, "dumped")
	if err != nil {
		return fmt.Errorf("set table status %s: %w", table.Name, err)
	}

	logger.Debug("Dump completed successfully.")

    return nil
}

func ClearIncompleteDumpedData(dumpFilePath string) error{
	_, err := os.Stat(dumpFilePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to remove file %s: %w", dumpFilePath, err)
	}
	err = os.RemoveAll(dumpFilePath)
	if err != nil {
		return fmt.Errorf("failed to remove file %s: %w", dumpFilePath, err)
	}
    return nil
}

// readLastOffset reads the last saved byte offset from the progress file.
func ReadLastOffset(filePath string) int64 {
    data, err := os.ReadFile(filePath)
    if err != nil {
        // If file doesn't exist or other read error, start from beginning
        return 0
    }
    offset, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
    if err != nil {
        // If conversion error, start from beginning
		panic(err)
    }
    return offset
}

// writeCurrentOffset writes the current byte offset to the progress file.
func WriteCurrentOffset(filePath string, offset int64) error {
    return os.WriteFile(filePath, []byte(strconv.FormatInt(offset, 10)), 0644)
}


