package syncer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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

func WriteTableStructureFromDumpfile(tableName string) error {

	createStatement, err := getCreateTableStatementFromDumpFile(GetDumpFilePath(tableName))
	p := parser.New()

	stmtNodes, _, err := p.Parse(createStatement, "", "")
	if err != nil {
		return err
	}

	if len(stmtNodes) == 0 {
		return fmt.Errorf("No statements found.")
	}

	createTableStmt, ok := stmtNodes[0].(*ast.CreateTableStmt)
	if !ok {
		return fmt.Errorf("The provided SQL is not a CREATE TABLE statement.")
	}

	var columnDatas []columnData
	position := 0
	for _, colDef := range createTableStmt.Cols {
		colName := colDef.Name.Name.O // Column Name
		colType := colDef.Tp.String() // Data Type string representation
		columnDatas = append(columnDatas, columnData{
			Name:     colName,
			Type:     colType,
			Position: position,
		})
		position += 1
	}

	jsonData, _ := json.Marshal(columnDatas)

	os.WriteFile(GetDumpTableStructureFilePath(tableName), jsonData, 0644)

	return nil
}


func getCreateTableStatementFromDumpFile(dumpFilePath string) (string, error) {
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

func GetBinlogCoordinatesFromDumpfile(dumpFilePath string) (BinlogPosition, error) {
	file, err := os.Open(dumpFilePath)
	if err != nil {
		return BinlogPosition{}, fmt.Errorf("failed to open dump file for parsing: %w", err)
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
			return BinlogPosition{}, fmt.Errorf("error reading dump file: %w", err)
		}
		if matches := re.FindStringSubmatch(line); len(matches) == 3 {
			logFile = matches[1]
			pos, err := strconv.ParseUint(matches[2], 10, 32)
			if err != nil {
				return BinlogPosition{}, fmt.Errorf("failed to parse binlog position: %w", err)
			}
			logPos = uint32(pos)
			return BinlogPosition{Logfile: logFile, Logpos: logPos}, nil
		}
	}

	return BinlogPosition{}, fmt.Errorf("binlog coordinates not found in dump file " + dumpFilePath)
}

func WriteDumpfilePosition(tableName string) error {

    binlogPos, err := GetBinlogCoordinatesFromDumpfile(GetDumpFilePath(tableName))
    // write the above info to a json file
    if err != nil {
        return fmt.Errorf("failed to parse binlog coordinates from dump: %w", err)
    }
    jsonData, err := json.Marshal(map[string]any{
        "logfile": binlogPos.Logfile,
        "logpos":  binlogPos.Logpos,
    })
    if err != nil {
        return fmt.Errorf("failed to marshal binlog coordinates: %w", err)
    }
    err = os.WriteFile(GetTableBinlogPositionFilePath(tableName), jsonData, 0644)
    return nil
}

func GetDumpTableStructureFilePath(tableName string) (string) {
   return "data/dumps/"+tableName+"/"+tableName+"-structure.json" 
}

func GetDumpReadProgressFilePath(tableName string) (string) {
   return "data/dumps/" + tableName + "/" + "read_progress.txt"
}


func GetDumpFilePath(tableName string) (string) {
   return "data/dumps/" + tableName + "/" + tableName + ".sql"
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
    outputFile := GetDumpFilePath(tableName)
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

func ClearIncompleteDumpedData(tableName string) error{
    util.CreateDirectoryIfNotExists("data/dumps")
    util.CreateDirectoryIfNotExists("data/dumps/" + tableName)
    // clear every file and directory in the above directory
    files, err := os.ReadDir("data/dumps/" + tableName)
    if err != nil {
        return fmt.Errorf("failed to read dump directory: %w", err)
    }
    for _, file := range files {
        filePath := "data/dumps/" + tableName + "/" + file.Name()
        err = os.RemoveAll(filePath)
        if err != nil {
            return fmt.Errorf("failed to remove file %s: %w", filePath, err)
        }
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


