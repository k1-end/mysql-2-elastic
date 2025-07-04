package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/k1-end/mysql-elastic-go/internal/config"
)

var RestartChannel chan bool
var DoneChannel chan bool


func initializeTables(appConfig *config.Config)  error{
    
    registeredTables := GetRegisteredTables()
    // Is there any registered tables?
	if len(registeredTables) == 0 {
		return errors.New("No registered tables.\n--- End ---")
	}

	MainLogger.Info("Processing table")
	err := processTables(registeredTables, appConfig)
	if err != nil {
		return err
	}

	MainLogger.Info("Finished Processing table")
	return nil
}

func main()  {
	appConfig, err := config.LoadConfig()
	if err != nil {
		MainLogger.Error(err.Error())
		os.Exit(1)
	}
	err = initializeTables(appConfig)
	if err != nil {
		MainLogger.Error(err.Error())
	}
    go runTheSyncer(appConfig)
	MainLogger.Debug("")
    http.HandleFunc("/", sendRestartSignal)
    http.ListenAndServe(":8080", nil)
}

func sendRestartSignal(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Hello, World!")
    // RestartChannel<- true
}

// NewLogger creates a new structured logger with a JSON handler.
func NewLogger() *slog.Logger {
	// Configure the handler to output JSON and include source code location for errors.
	handlerOptions := &slog.HandlerOptions{
		AddSource: true, // Add file and line number
		Level:     slog.LevelDebug, // Set default logging level
	}

	// Create a JSON handler that writes to stderr.
	handler := slog.NewJSONHandler(os.Stderr, handlerOptions)
	return slog.New(handler)
}

func runTheSyncer(appConfig *config.Config) {
    registeredTables := GetRegisteredTables()
    var tableNames []string
    for name, table := range registeredTables {
        if table.Status == "syncing" {
            tableNames = append(tableNames, name)
        }
    }
	MainLogger.Debug("Syncing: " + strings.Join(tableNames[:], ","))
    cfg := replication.BinlogSyncerConfig {
        ServerID: uint32(appConfig.Database.ServerId),
        Flavor  : appConfig.Database.Driver,
        Host    : appConfig.Database.Host,
        Port    : uint16(appConfig.Database.Port),
        User    : appConfig.Database.Username,
        Password: appConfig.Database.Password,
		Logger  : MainLogger,
    }

    syncer := replication.NewBinlogSyncer(cfg)

    currentBinlogPos, _ := GetBinlogCoordinates("main")

    // Start sync with specified binlog file and position
    streamer, err := syncer.StartSync(mysql.Position{
        Name: currentBinlogPos.Logfile,
        Pos: currentBinlogPos.Logpos,
    })
    if err != nil {
		MainLogger.Error(err.Error())
    }

    for {
        select {
        case <-DoneChannel:
            return
        case <-RestartChannel:
            initializeTables(appConfig)
        default:
            err = processBinlogEvent(streamer, &currentBinlogPos, tableNames, appConfig)
			if err != nil {
				MainLogger.Error(err.Error())
			}
        }
    }
}

func processTables(registeredTables map[string]RegisteredTable, appConfig *config.Config) error{
    for _, table := range registeredTables {
		if table.Status == "created" || table.Status == "dumping" {
			MainLogger.Debug(table.Name + ": " + table.Status)
			err := ClearIncompleteDumpedData(table.Name)
			if err != nil {
				MainLogger.Error(fmt.Sprintf("Fatal error ClearIncompleteDumpedData: %v", err))
				panic(err)
			}
            err = InitialDump(table.Name, appConfig)
			if err != nil {
				MainLogger.Error(fmt.Sprintf("Fatal error InitialDump: %v", err))
				panic(err)
			}
			table.Status = GetRegisteredTables()[table.Name].Status
		}

		if table.Status == "dumped" || table.Status == "moving" {
			MainLogger.Debug(table.Name + ": " + table.Status)
            SendDataToElasticFromDumpfile(table.Name, appConfig)
			table.Status = GetRegisteredTables()[table.Name].Status
		}

		if table.Status == "moved" {
			MainLogger.Debug(table.Name + ": " + table.Status)
			err := SyncWithTheMainLoop(table.Name, appConfig)
			if err != nil {
				return err
			}
			table.Status = GetRegisteredTables()[table.Name].Status
		}
    }
	return nil
}

func SyncWithTheMainLoop(tableName string, appConfig *config.Config) error{
	MainLogger.Debug("Syncing with the main loop for table: " + tableName)
    tableBinlogPos, err := GetBinlogCoordinates(tableName)
    if err != nil {
        tableBinlogPos, err = GetBinlogCoordinatesFromDumpfile(GetDumpFilePath(tableName))
        if err != nil {
            return fmt.Errorf("failed to parse binlog coordinates from dump file: %w", err)
        }
        err = WriteDumpfilePosition(tableName) // for safety
		if err != nil {
			return fmt.Errorf("failed to write dump file position: %w", err)
		}
    }

    mainBinlogPos, err := GetBinlogCoordinates("main")
    if err != nil {
        return fmt.Errorf("failed to get binlog coordinates: %w", err)
    }


    if GetNewerBinlogPosition(mainBinlogPos, tableBinlogPos) == mainBinlogPos  && mainBinlogPos != tableBinlogPos{
        // Main binlog is newer, so we need to sync the dump file with the main binlog
		MainLogger.Debug("Main binlog is newer than dump file. Syncing dump file with main binlog...")
        err = SyncTableUntileDestination(tableName, mainBinlogPos, appConfig)
        if err != nil {
            return fmt.Errorf("failed to sync table until destination: %w", err)
        }
    }

    if GetNewerBinlogPosition(mainBinlogPos, tableBinlogPos) == tableBinlogPos  && mainBinlogPos != tableBinlogPos{
		MainLogger.Debug("Dump file is newer than main binlog. Syncing main binlog with dump file...")
        err = SyncMainBinlogWithDumpFile(tableBinlogPos, appConfig)
        if err != nil {
            return fmt.Errorf("failed to sync table until destination: %w", err)
        }
    }
	MainLogger.Debug("Sync completed for table:" + tableName)
    err = SetTableStatus(tableName, "syncing")
    if err != nil {
        return fmt.Errorf("set table status %s: %w", tableName, err)
    }
    return nil
}


func GetBinlogCoordinates(tableName string) (BinlogPosition, error) {
    var filePath string
    if tableName == "main" {
        filePath = GetMainBinlogPositionFilePath()
    }else{
        filePath = GetDumpBinlogPositionFilePath(tableName)
    }
    return ParseBinlogCoordinatesFile(filePath)
}

func SyncTableUntileDestination(tableName string, desBinlogPos BinlogPosition, appConfig *config.Config) error {
	MainLogger.Debug("Syncing table:" + tableName)

    currentBinlogPos, err := ParseBinlogCoordinatesFile(GetDumpBinlogPositionFilePath(tableName))
    if err != nil {
        return fmt.Errorf("failed to parse binlog coordinates from dump file: %w", err)
    }

    return SyncTablesTillDestination([]string{tableName}, desBinlogPos, currentBinlogPos, appConfig)

}

func WriteBinlogPosition(binlogPos BinlogPosition, tableName string) error {
    jsonData, err := json.Marshal(map[string]interface{}{
        "logfile": binlogPos.Logfile,
        "logpos":  binlogPos.Logpos,
    })
    if err != nil {
        return fmt.Errorf("failed to marshal binlog coordinates: %w", err)
    }

    var filePath string
    if tableName == "main" {
        filePath = GetMainBinlogPositionFilePath()
    } else {
        filePath = GetDumpBinlogPositionFilePath(tableName)
    }
    err = os.WriteFile(filePath, jsonData, 0644)
    if err != nil {
        return fmt.Errorf("failed to write binlog coordinates to file: %w", err)
    }

    return nil
}

func bulkInsertBinlogRowsToElastic(rows [][]interface{}, tableStructure []map[string]interface{}, tableName string, appConfig *config.Config) error {
    var values []map[string]interface{} 
    for _, row := range rows {
        var singleRecord = make(map[string]interface{})
        for j, val := range row {
            columnName, err := getColumnNameFromPosition(tableStructure, j)
            if err != nil {
                return fmt.Errorf("Error getting column name from position %d: %w", j, err)
            }
            singleRecord[columnName] = val
        }
        values = append(values, singleRecord)
    }

    err := bulkSendToElastic(tableName, values, appConfig)
    if err != nil {
        return fmt.Errorf("Error sending data to Elastic: %w", err)
    }
    return nil
}

func bulkUpdateBinlogRowsToElastic(rows [][]interface{}, tableStructure []map[string]interface{}, tableName string, appConfig *config.Config) error {
    var values []map[string]interface{} 
    for _, row := range rows {
        var singleRecord = make(map[string]interface{})
        for j, val := range row {
            columnName, err := getColumnNameFromPosition(tableStructure, j)
            if err != nil {
                return fmt.Errorf("Error getting column name from position %d: %w", j, err)
            }
            singleRecord[columnName] = val
        }
        values = append(values, singleRecord)
    }

	err := bulkUpdateToElastic(tableName, values, appConfig)

    return err
}




func bulkDeleteBinlogRowsFromElastic(rows [][]interface{}, tableStructure []map[string]interface{}, tableName string, appConfig *config.Config) error {
    var values []map[string]interface{} 
    for _, row := range rows {
        var singleRecord = make(map[string]interface{})
        for j, val := range row {
            columnName, err := getColumnNameFromPosition(tableStructure, j)
            if err != nil {
                return fmt.Errorf("Error getting column name from position %d: %w", j, err)
            }
            singleRecord[columnName] = val
        }
        values = append(values, singleRecord)
    }
    err := bulkDeleteFromElastic(tableName, values, appConfig)
    if err != nil {
        return fmt.Errorf("Error deleting data from Elastic: %w", err)
    }
    return nil
}

func SyncMainBinlogWithDumpFile(desBinlogPos BinlogPosition, appConfig *config.Config) error {
	MainLogger.Debug("Syncing main loop")
    currentBinlogPos, err := GetBinlogCoordinates("main")
    if err != nil {
        return fmt.Errorf("failed to parse binlog coordinates from dump file: %w", err)
    }

    registeredTables := GetRegisteredTables()
    // filter registeredTables by syncing status
    var synchingTableNames []string
    for name, table := range registeredTables {
        if table.Status == "syncing" {
            synchingTableNames = append(synchingTableNames, name)
        }
    }
    err = SyncTablesTillDestination(synchingTableNames, desBinlogPos, currentBinlogPos, appConfig)

    return err
}

// If both args are equal, the first one will be returned
func GetNewerBinlogPosition(pos1, pos2 BinlogPosition) BinlogPosition {
    if pos1.Logfile < pos2.Logfile {
        return pos2
    } else if pos1.Logfile > pos2.Logfile {
        return pos1
    } else {
        if pos1.Logpos < pos2.Logpos {
            return pos2
        } else if pos1.Logpos > pos2.Logpos {
            return pos1
        } else {
            return pos1 // They are equal
        }
    }
}


func SyncTablesTillDestination(tableNames []string, desBinlogPos, currentBinlogPos BinlogPosition, appConfig *config.Config) error {
	MainLogger.Debug("Syncing: " + strings.Join(tableNames[:], ","))
    cfg := replication.BinlogSyncerConfig {
        ServerID: uint32(appConfig.Database.ServerId),
        Flavor  : appConfig.Database.Driver,
        Host    : appConfig.Database.Host,
        Port    : uint16(appConfig.Database.Port),
        User    : appConfig.Database.Username,
        Password: appConfig.Database.Password,
		Logger  : MainLogger,
    }

    syncer := replication.NewBinlogSyncer(cfg)

    // Start sync with specified binlog file and position
    streamer, _ := syncer.StartSync(mysql.Position{
        Name: currentBinlogPos.Logfile,
        Pos: currentBinlogPos.Logpos,
    })

    for GetNewerBinlogPosition(currentBinlogPos, desBinlogPos) == desBinlogPos  && desBinlogPos != currentBinlogPos{
		err := processBinlogEvent(streamer, &currentBinlogPos, tableNames, appConfig)
		if err != nil {
			return err
		}
    }

    return nil
}

func processBinlogEvent(streamer *replication.BinlogStreamer, currentBinlogPos *BinlogPosition, tableNames []string, appConfig *config.Config) error {
	if currentBinlogPos == nil {
		return errors.New("nil pointer to currentBinlogPos")
	}
    ev, _ := streamer.GetEvent(context.Background())
	if ev == nil {
		return errors.New("nil pointer to event")
	}
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
            WriteBinlogPosition(*currentBinlogPos, "main") // Update the position after rotation
            return nil
        }
        schemaName := string(e.Table.Schema) // Get schema name

		MainLogger.Debug(fmt.Sprintf("ROW EVENT for %s.%s\n", schemaName, eventTableName))

        switch ev.Header.EventType {
        case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			MainLogger.Debug("  ➡️ INSERT:")
            tableStructure, _ := getTableStructure(getDumpTableStructureFilePath(eventTableName))
            bulkInsertBinlogRowsToElastic(e.Rows, tableStructure, eventTableName, appConfig)
        case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			MainLogger.Debug("  🔄 UPDATE:")
			MainLogger.Debug(string(e.ColumnBitmap2))
            // For UPDATE events, e.Rows contains pairs of [before-image, after-image]
            // The length of e.Rows will be N*2, where N is the number of updated rows.
            tableStructure, _ := getTableStructure(getDumpTableStructureFilePath(eventTableName))
            var afterDocs [][]interface{}
            for i := 0; i < len(e.Rows); i += 2 {
                afterValues := e.Rows[i+1]
                afterDocs = append(afterDocs, afterValues)
            }
			err := bulkUpdateBinlogRowsToElastic(afterDocs, tableStructure, eventTableName, appConfig)
			if err != nil {
				return err
			}
        case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			MainLogger.Debug("  🗑️ DELETE:")
            tableStructure, _ := getTableStructure(getDumpTableStructureFilePath(eventTableName))
            bulkDeleteBinlogRowsFromElastic(e.Rows, tableStructure, eventTableName, appConfig)
        }
    case *replication.QueryEvent:
    // DDL changes, etc.
    default:
    }
    WriteBinlogPosition(*currentBinlogPos, "main") // Update the position after rotation
    return nil
}
