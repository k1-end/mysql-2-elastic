package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"

	"github.com/k1-end/mysql-2-elastic/internal/api"
	"github.com/k1-end/mysql-2-elastic/internal/config"
	"github.com/k1-end/mysql-2-elastic/internal/database"
	elasticpack "github.com/k1-end/mysql-2-elastic/internal/elastic"
	"github.com/k1-end/mysql-2-elastic/internal/logger"
	"github.com/k1-end/mysql-2-elastic/internal/storage"
	"github.com/k1-end/mysql-2-elastic/internal/storage/filesystem"
	syncerpack "github.com/k1-end/mysql-2-elastic/internal/syncer"
	tablepack "github.com/k1-end/mysql-2-elastic/internal/table"
)

var RestartChannel chan bool
var DoneChannel chan bool
var MainLogger *slog.Logger // Global variable to hold the main logger
var MainLogWriter *logger.SlogWriter

func init() {
	MainLogger = logger.NewLogger()
	MainLogWriter = logger.NewSlogWriter(MainLogger, slog.LevelDebug)

}

func main() {
	appConfig, err := config.LoadConfig()
	if err != nil {
		MainLogger.Error(err.Error())
		os.Exit(1)
	}
	esClient, err := elasticpack.GetElasticClient(appConfig)
    if err != nil {
		MainLogger.Error(err.Error())
		panic(err)
    }

	syncer, err := syncerpack.GetDatabaseSyncer(appConfig, MainLogger)
    if err != nil {
		MainLogger.Error(err.Error())
		panic(err)
    }

	fs, err := filesystem.NewFileStorage()
	if err != nil {
		MainLogger.Error(err.Error())
		os.Exit(1)
	}
	err = initializeTables(appConfig, esClient, syncer, fs)
	if err != nil {
		MainLogger.Error(err.Error())
	}

    go runTheSyncer(appConfig, esClient, syncer, fs)

	api.Serve(MainLogger)
}

func initializeTables(appConfig *config.Config, esClient *elasticsearch.Client, syncer *replication.BinlogSyncer, tableStorage storage.TableStorage) error{
    
    registeredTables, err := tableStorage.GetRegisteredTables()
	if err != nil {
		MainLogger.Error(err.Error())
		os.Exit(1)
	}
    // Is there any registered tables?
	if len(registeredTables) == 0 {
		return errors.New("No registered tables.\n--- End ---")
	}

	MainLogger.Info("Processing table")

    for _, table := range registeredTables {
		if table.Status == "created" || table.Status == "dumping" {
			MainLogger.Debug(table.Name + ": " + table.Status)
			err := syncerpack.ClearIncompleteDumpedData(table.Name)
			if err != nil {
				MainLogger.Error(fmt.Sprintf("Fatal error ClearIncompleteDumpedData: %v", err))
				panic(err)
			}
			// Reset the table status to "created"
			err = tableStorage.SetTableStatus(table.Name, "created")
			if err != nil {
				return fmt.Errorf("set table status %s: %w", table.Name, err)
			}

            err = syncerpack.InitialDump(table.Name, appConfig, MainLogger, tableStorage)
			if err != nil {
				MainLogger.Error(fmt.Sprintf("Fatal error InitialDump: %v", err))
				panic(err)
			}
			table.Status, err = tableStorage.GetTableStatus(table.Name)
			if err != nil {
				MainLogger.Error(err.Error())
				os.Exit(1)
			}
		}

		if table.Status == "dumped" || table.Status == "moving" {
			MainLogger.Debug(table.Name + ": " + table.Status)
			err = tableStorage.SetTableStatus(table.Name, "moving")
			if err != nil {
				return fmt.Errorf("set table status %s: %w", table.Name, err)
			}

			columnsInfo, err:= syncerpack.GetTableColsInfoFromDumpFile(table.Name)
			if err != nil {
				return err
			}
			tableStorage.SetTableColsInfo(table.Name, columnsInfo)

            err = sendDataToElasticFromDumpfile(table, esClient)
			if err != nil {
				return fmt.Errorf("set table status %s: %w", table.Name, err)
			}
			err = tableStorage.SetTableStatus(table.Name, "moved")
			if err != nil {
				return fmt.Errorf("set table status %s: %w", table.Name, err)
			}
			table.Status, err = tableStorage.GetTableStatus(table.Name)
			if err != nil {
				MainLogger.Error(err.Error())
				os.Exit(1)
			}
		}

		if table.Status == "moved" {
			MainLogger.Debug("Syncing with the main loop for table: " + table.Name)
			tableBinlogPos, err := syncerpack.GetBinlogCoordinatesFromDumpfile(syncerpack.GetDumpFilePath(table.Name))
			if err != nil {
				return fmt.Errorf("failed to parse binlog coordinates from dump file: %w", err)
			}
			err = syncerpack.WriteDumpfilePosition(table.Name) // for safety
			if err != nil {
				return fmt.Errorf("failed to write dump file position: %w", err)
			}

			mainBinlogPos, err := syncerpack.GetStoredBinlogCoordinates("main")
			if err != nil {
				return fmt.Errorf("failed to get binlog coordinates: %w", err)
			}

			newerBinlog := syncerpack.GetNewerBinlogPosition(&mainBinlogPos, &tableBinlogPos)

			if newerBinlog == &mainBinlogPos{
				// Main binlog is newer, so we need to sync the dump file with the main binlog
				MainLogger.Debug("Main binlog is newer than dump file. Syncing dump file with main binlog...")

				err = SyncTablesTillDestination([]string{table.Name}, mainBinlogPos, tableBinlogPos, esClient, syncer, tableStorage)
				if err != nil {
					return fmt.Errorf("failed to sync table until destination: %w", err)
				}
			} else if newerBinlog == &tableBinlogPos{
				MainLogger.Debug("Dump file is newer than main binlog. Syncing main binlog with dump file...")
				currentBinlogPos, err := syncerpack.GetStoredBinlogCoordinates("main")
				if err != nil {
					return fmt.Errorf("failed to parse binlog coordinates from dump file: %w", err)
				}

				registeredTables, err := tableStorage.GetRegisteredTables()
				if err != nil {
					MainLogger.Error(err.Error())
					os.Exit(1)
				}
				// filter registeredTables by syncing status
				var synchingTableNames []string
				for name, table := range registeredTables {
					if table.Status == "syncing" {
						synchingTableNames = append(synchingTableNames, name)
					}
				}
				err = SyncTablesTillDestination(synchingTableNames, tableBinlogPos, currentBinlogPos, esClient, syncer, tableStorage)
				if err != nil {
					return fmt.Errorf("failed to sync table until destination: %w", err)
				}
			}
			MainLogger.Debug("Sync completed for table:" + table.Name)
			err = tableStorage.SetTableStatus(table.Name, "syncing")
			if err != nil {
				return fmt.Errorf("set table status %s: %w", table.Name, err)
			}

			table.Status, err = tableStorage.GetTableStatus(table.Name)
			if err != nil {
				MainLogger.Error(err.Error())
				os.Exit(1)
			}
		}
    }
	MainLogger.Info("Finished Processing table")
	return nil
}


func runTheSyncer(appConfig *config.Config, esClient *elasticsearch.Client, syncer *replication.BinlogSyncer, tableStorage storage.TableStorage) {
    registeredTables, err := tableStorage.GetRegisteredTables()
	if err != nil {
		MainLogger.Error(err.Error())
		os.Exit(1)
	}
    var tableNames []string
    for name, table := range registeredTables {
        if table.Status == "syncing" {
            tableNames = append(tableNames, name)
        }
    }
	MainLogger.Debug("Syncing: " + strings.Join(tableNames[:], ","))

    currentBinlogPos, err := syncerpack.GetStoredBinlogCoordinates("main")
    if err != nil {
		MainLogger.Error(err.Error())
		panic(err)
    }

    // Start sync with specified binlog file and position
    streamer, err := syncer.StartSync(mysql.Position{
        Name: currentBinlogPos.Logfile,
        Pos: currentBinlogPos.Logpos,
    })

    if err != nil {
		MainLogger.Error(err.Error())
		panic(err)
    }

    for {
        select {
        case <-DoneChannel:
            return
        case <-RestartChannel:
            initializeTables(appConfig, esClient, syncer, tableStorage)
        default:

			ev, err := streamer.GetEvent(context.Background())
			if err != nil {
				MainLogger.Error(err.Error())
				panic(err)
			}
			if ev == nil {
				MainLogger.Error("nil pointer to event")
			}
            err = processBinlogEvent(ev, &currentBinlogPos, tableNames, esClient, tableStorage)
			if err != nil {
				MainLogger.Error(err.Error())
			}
        }
    }
}

func convertBinlogRowsToArrayOfMaps(rows [][]any, tableCols []tablepack.ColumnInfo) ([]map[string]any, error) {
    var values []map[string]any 
    for _, row := range rows {
        var singleRecord = make(map[string]any)
        for j, val := range row {
            columnName, err := database.GetColumnNameFromPosition(tableCols, j)
            if err != nil {
                return nil, fmt.Errorf("Error getting column name from position %d: %w", j, err)
            }
            singleRecord[columnName] = val
        }
        values = append(values, singleRecord)
    }
	return values, nil
}

func SyncTablesTillDestination(
	tableNames []string,
	desBinlogPos, currentBinlogPos syncerpack.BinlogPosition,
	esClient *elasticsearch.Client,
	syncer *replication.BinlogSyncer,
	tableStorage storage.TableStorage,
) error {
	MainLogger.Debug("Syncing: " + strings.Join(tableNames[:], ","))

    // Start sync with specified binlog file and position
    streamer, _ := syncer.StartSync(mysql.Position{
        Name: currentBinlogPos.Logfile,
        Pos: currentBinlogPos.Logpos,
    })

    for syncerpack.GetNewerBinlogPosition(&currentBinlogPos, &desBinlogPos) == &desBinlogPos {
		ev, err := streamer.GetEvent(context.Background())
		if err != nil {
			MainLogger.Error(err.Error())
			return err
		}
		if ev == nil {
			MainLogger.Error("nil pointer to event")
			return err
		}
		err = processBinlogEvent(ev, &currentBinlogPos, tableNames, esClient, tableStorage)
		if err != nil {
			MainLogger.Error(err.Error())
			return err
		}
    }

    return nil
}

func processBinlogEvent(ev *replication.BinlogEvent, currentBinlogPos *syncerpack.BinlogPosition, tableNames []string, esClient *elasticsearch.Client, tableStorage storage.TableStorage) error {
    currentBinlogPos.Logpos = uint32(ev.Header.LogPos) // Update the current position from the event header
    //print position get from event
    switch e := ev.Event.(type) {
    case *replication.RotateEvent:
        currentBinlogPos.Logfile = string(e.NextLogName)
		MainLogger.Debug(fmt.Sprintf("🔄 Binlog rotated to: %s at position %d\n", currentBinlogPos.Logfile, currentBinlogPos.Logpos))

    case *replication.RowsEvent:
        // This event contains the row data for INSERT, UPDATE, DELETE
        eventTableName := string(e.Table.Table) // Get table name from the event
        if !slices.Contains(tableNames, eventTableName) {
            syncerpack.WriteBinlogPosition(*currentBinlogPos, "main") // Update the position after not registered table or rotation
            return nil
        }
        schemaName := string(e.Table.Schema) // Get schema name

		MainLogger.Debug(fmt.Sprintf("ROW EVENT for %s.%s\n", schemaName, eventTableName))

        switch ev.Header.EventType {
        case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			MainLogger.Debug("  ➡️ INSERT:")
			tb, err := tableStorage.GetTable(eventTableName)
			if err != nil {
				MainLogger.Error(err.Error())
				return err
			}
			records, err := convertBinlogRowsToArrayOfMaps(e.Rows, *tb.Columns)
			if err != nil {
				MainLogger.Error(err.Error())
				return err
			}
			err = elasticpack.BulkSendToElastic(eventTableName, records, esClient, MainLogger)
			if err != nil {
				MainLogger.Error(err.Error())
				return fmt.Errorf("Error sending data to Elastic: %w", err)
			}
        case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			MainLogger.Debug("  🔄 UPDATE:")
			MainLogger.Debug(string(e.ColumnBitmap2))
            // For UPDATE events, e.Rows contains pairs of [before-image, after-image]
            // The length of e.Rows will be N*2, where N is the number of updated rows.
            var afterDocs [][]any
            for i := 0; i < len(e.Rows); i += 2 {
                afterValues := e.Rows[i+1]
                afterDocs = append(afterDocs, afterValues)
            }

			tb, err := tableStorage.GetTable(eventTableName)
			if err != nil {
				MainLogger.Error(err.Error())
				return err
			}

			records, err := convertBinlogRowsToArrayOfMaps(e.Rows, *tb.Columns)
			if err != nil {
				MainLogger.Error(err.Error())
				return err
			}
			err = elasticpack.BulkUpdateToElastic(eventTableName, records, esClient, MainLogger)
			if err != nil {
				MainLogger.Error(err.Error())
				return err
			}
        case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			MainLogger.Debug("  🗑️ DELETE:")
			tb, err := tableStorage.GetTable(eventTableName)
			if err != nil {
				MainLogger.Error(err.Error())
				return err
			}
			records, err:= convertBinlogRowsToArrayOfMaps(e.Rows, *tb.Columns)
			if err != nil {
				MainLogger.Error(err.Error())
				return err
			}
			err = elasticpack.BulkDeleteFromElastic(eventTableName, records, esClient, MainLogger)
			if err != nil {
				MainLogger.Error(err.Error())
				return err
			}
        }
    case *replication.QueryEvent:
    // DDL changes, etc.
    default:
    }
    syncerpack.WriteBinlogPosition(*currentBinlogPos, "main") // Update the position after rotation
    return nil
}

func sendDataToElasticFromDumpfile(table tablepack.RegisteredTable, esClient *elasticsearch.Client) error {
	dumpFilePath := syncerpack.GetDumpFilePath(table.Name)
	progressFile := syncerpack.GetDumpReadProgressFilePath(table.Name)


    if table.Status != "moving" {
		return fmt.Errorf("Table status in not *moving*")
    }

	currentOffset := syncerpack.ReadLastOffset(progressFile)
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

			err = processInsertString(table.Name, line, *table.Columns, esClient)
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

				err = processInsertString(table.Name, insertStatement, *table.Columns, esClient)
				if err != nil {
					return err
				}

				inInsertStatement = false
			}
		}

		currentOffset += int64(len(scanner.Bytes())) + 2 // +1 for the newline character consumed by scanner
		if !inInsertStatement {
			err = syncerpack.WriteCurrentOffset(progressFile, currentOffset)
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
	return nil
}

func processInsertString(tableName string, insertStatement string, tableCols []tablepack.ColumnInfo, esClient *elasticsearch.Client) error {
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
    var values []map[string]any 
    for i, row := range insertStmt.Lists {
        var singleRecord = make(map[string]any)
        for j, expr := range row {
            val, err := database.ExtractValue(expr)
            if err != nil {
                MainLogger.Error(fmt.Sprintf("Error extracting value for column %d in row %d: %v\n", j+1, i+1, err))
				panic(err)
            }
            columnName, err := database.GetColumnNameFromPosition(tableCols, j)
            if err != nil {
                return fmt.Errorf("Error getting column name from position %d: %w", j, err)
            }
            singleRecord[columnName] = val
        }
        values = append(values, singleRecord)
    }

    err = elasticpack.BulkSendToElastic(tableName, values, esClient, MainLogger)
    if err != nil {
        return err
    }
    return nil
}
