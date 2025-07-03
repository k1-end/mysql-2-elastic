package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver" // Required for the parser to work
	"github.com/spf13/viper"
)

type ColumnData struct {
    Name string `json:"name"`
    Type string `json:"type"`
    Position int `json:"position"`
}

type DatabaseConfig struct {
	Driver   string `mapstructure:"driver"`
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	ServerId int    `mapstructure:"server_id"`
	Name     string `mapstructure:"name"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type ElasticConfig struct {
    Address string `mapstructure:"address"`
    Username string `mapstructure:"username"`
    Password string `mapstructure:"password"`
}

type Config struct {
	Database DatabaseConfig `mapstructure:"database"`
    Elastic ElasticConfig `mapstructure:"elastic"`
    MysqlDumpPath string `mapstructure:"mysqldump_path"`
}

func init() {
	MainLogger = NewLogger()
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath(".") // Look in the current directory
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			MainLogger.Error("Warning: config.json not found. Using defaults or environment variables.")
			panic(err)
		} else {
			MainLogger.Error(fmt.Sprintf("Fatal error reading config file: %v", err))
			panic(err)
		}
	}

	if err := viper.Unmarshal(&AppConfiguration); err != nil {
		MainLogger.Error(fmt.Sprintf("Unable to decode config into struct: %v", err))
		panic(err)
	}

	if err := viper.Unmarshal(&AppConfiguration); err != nil {
		MainLogger.Error(fmt.Sprintf("Unable to decode config into struct after env binding: %v", err))
		panic(err)
	}
}

var AppConfiguration Config // Global variable to hold your configuration
var MainLogger *slog.Logger // Global variable to hold the main logger

func writeTableStructureFromDumpfile(tableName string) error {

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

	var columnDatas []ColumnData
	position := 0
	for _, colDef := range createTableStmt.Cols {
		colName := colDef.Name.Name.O // Column Name
		colType := colDef.Tp.String() // Data Type string representation
		columnDatas = append(columnDatas, ColumnData{
			Name:     colName,
			Type:     colType,
			Position: position,
		})
		position += 1
	}

	jsonData, _ := json.Marshal(columnDatas)

	os.WriteFile(getDumpTableStructureFilePath(tableName), jsonData, 0644)

	return nil
}

func SendDataToElasticFromDumpfile(tableName string) error {
	dumpFilePath := GetDumpFilePath(tableName)
	progressFile := getDumpReadProgressFilePath(tableName)


    table := GetRegisteredTables()[tableName]
    if table.Status != "moving" {
       SetTableStatus(tableName, "moving")
    }

    writeTableStructureFromDumpfile(tableName)
	tableStructure, _ := getTableStructure(getDumpTableStructureFilePath(tableName))

	currentOffset := readLastOffset(progressFile)
	file, err := os.OpenFile(dumpFilePath, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open dump file: %w", err)
	}
	defer file.Close()

	_, err = file.Seek(currentOffset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("Failed to seek in dump file: %w", err)
	}

	scanner := bufio.NewScanner(file)

	var currentStatement bytes.Buffer
	inInsertStatement := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, "INSERT INTO") && strings.HasSuffix(line, ";") {

			err = processInsertString(tableName, line, tableStructure)
			if err != nil {
				return err
			}

			inInsertStatement = false
		} else if strings.HasPrefix(line, "INSERT INTO") {
			inInsertStatement = true
			currentStatement.WriteString(" " + line)
		} else if inInsertStatement {
			currentStatement.WriteString(" " + line)
			if strings.HasSuffix(line, ";") {
				insertStatement := currentStatement.String()

				err = processInsertString(tableName, insertStatement, tableStructure)
				if err != nil {
					return err
				}

				inInsertStatement = false
			}
		}

		currentOffset += int64(len(scanner.Bytes())) + 2 // +1 for the newline character consumed by scanner
		if !inInsertStatement {
			err = writeCurrentOffset(progressFile, currentOffset)
			if err != nil {
				return err
			}
			currentStatement.Reset()
		}
	}

	if err := scanner.Err(); err != nil {
		// The currentOffset might not be at the end of the last successfully processed line
		// if the error occurred mid-line or during the read operation for the next line.
		// The last successfully written offset to progressFile is your best bet.
		return err
	}

    SetTableStatus(tableName, "moved")
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

	return BinlogPosition{}, fmt.Errorf("binlog coordinates not found in dump file")
}

func WriteDumpfilePosition(tableName string) error {

    binlogPos, err := GetBinlogCoordinatesFromDumpfile(GetDumpFilePath(tableName))
    // write the above info to a json file
    if err != nil {
        return fmt.Errorf("failed to parse binlog coordinates from dump: %w", err)
    }
    jsonData, err := json.Marshal(map[string]interface{}{
        "logfile": binlogPos.Logfile,
        "logpos":  binlogPos.Logpos,
    })
    if err != nil {
        return fmt.Errorf("failed to marshal binlog coordinates: %w", err)
    }
    err = os.WriteFile(GetDumpBinlogPositionFilePath(tableName), jsonData, 0644)
    return nil
}

func getDumpTableStructureFilePath(tableName string) (string) {
   return "data/dumps/"+tableName+"/"+tableName+"-structure.json" 
}

func getDumpReadProgressFilePath(tableName string) (string) {
   return "data/dumps/" + tableName + "/" + "read_progress.txt"
}

func GetDumpBinlogPositionFilePath(tableName string) (string) {
   return "data/dumps/" + tableName + "/" + tableName + "-dump-binlog-position.json" 
}

func GetDumpFilePath(tableName string) (string) {
   return "data/dumps/" + tableName + "/" + tableName + ".sql"
}

func InitialDump(tableName string) error{
    registeredTables := GetRegisteredTables()
    table, exists := registeredTables[tableName]
    if !exists {
        return fmt.Errorf("table %s not found in registered tables", tableName)
    }
	MainLogger.Debug("Dumping table: " + table.Name)
    if table.Status != "created" {
        return fmt.Errorf("table %s is not in the created state", table.Name)
    }

    // Set the table status to "dumping"
    SetTableStatus(tableName, "dumping")

    args := []string{
        "--skip-ssl-verify",
		"--single-transaction",
		"--master-data=2",
		fmt.Sprintf("--user=%s", AppConfiguration.Database.Username),
		fmt.Sprintf("--password=%s", AppConfiguration.Database.Password),
        fmt.Sprintf("--host=%s", AppConfiguration.Database.Host),
        fmt.Sprintf("--port=%d", AppConfiguration.Database.Port),
		AppConfiguration.Database.Name,
	}

	args = append(args, []string{table.Name}...)
	MainLogger.Debug(strings.Join(args[:], " "))
    ctx := context.Background()
	cmd := exec.CommandContext(
		ctx,
		AppConfiguration.MysqlDumpPath,
		args...,
	)

    createDirectoryIfNotExists("data/dumps")
    createDirectoryIfNotExists("data/dumps/" + tableName)
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
		MainLogger.Debug(fmt.Sprintf("mysqldump stderr: %s", errScanner.Text()))
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("mysqldump failed: %w", err)
	}

    SetTableStatus(tableName, "dumped")
	MainLogger.Debug("Dump completed successfully.")

    return nil
}



func ClearIncompleteDumpedData(tableName string) error{
    createDirectoryIfNotExists("data/dumps")
    createDirectoryIfNotExists("data/dumps/" + tableName)
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

    // Reset the table status to "created"
    SetTableStatus(tableName, "created")

    return nil
}

func SetTableStatus(tableName string, status string) error {
    registeredTables := GetRegisteredTables()
    table, exists := registeredTables[tableName]
    if !exists {
        return fmt.Errorf("table %s not found in registered tables", tableName)
    }
    table.Status = status
    registeredTables[table.Name] = table
    jsonData, _ := json.Marshal(registeredTables)
    os.WriteFile(registeredTablesFilePath, jsonData, 0644)
    return nil
}
