package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/k1-end/mysql-elastic-go/internal/api"
	"github.com/k1-end/mysql-elastic-go/internal/config"
	"github.com/k1-end/mysql-elastic-go/internal/database"
	"github.com/k1-end/mysql-elastic-go/internal/logger"
	syncerpack "github.com/k1-end/mysql-elastic-go/internal/syncer"
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
	esClient, err := getElasticClient(appConfig)
    if err != nil {
		MainLogger.Error(err.Error())
		panic(err)
    }

	syncer, err := database.GetDatabaseSyncer(appConfig, MainLogger)
    if err != nil {
		MainLogger.Error(err.Error())
		panic(err)
    }

	err = initializeTables(appConfig, esClient, syncer)
	if err != nil {
		MainLogger.Error(err.Error())
	}

    go runTheSyncer(appConfig, esClient, syncer)

	api.Serve(MainLogger)
}

func initializeTables(appConfig *config.Config, esClient *elasticsearch.Client, syncer *replication.BinlogSyncer) error{
    
    registeredTables := GetRegisteredTables()
    // Is there any registered tables?
	if len(registeredTables) == 0 {
		return errors.New("No registered tables.\n--- End ---")
	}

	MainLogger.Info("Processing table")

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
            SendDataToElasticFromDumpfile(table.Name, esClient)
			table.Status = GetRegisteredTables()[table.Name].Status
		}

		if table.Status == "moved" {
			MainLogger.Debug("Syncing with the main loop for table: " + table.Name)
			tableBinlogPos, err := GetBinlogCoordinatesFromDumpfile(GetDumpFilePath(table.Name))
			if err != nil {
				return fmt.Errorf("failed to parse binlog coordinates from dump file: %w", err)
			}
			err = WriteDumpfilePosition(table.Name) // for safety
			if err != nil {
				return fmt.Errorf("failed to write dump file position: %w", err)
			}

			mainBinlogPos, err := syncerpack.GetStoredBinlogCoordinates("main")
			if err != nil {
				return fmt.Errorf("failed to get binlog coordinates: %w", err)
			}

			newerBinlog := GetNewerBinlogPosition(&mainBinlogPos, &tableBinlogPos)

			if newerBinlog == &mainBinlogPos{
				// Main binlog is newer, so we need to sync the dump file with the main binlog
				MainLogger.Debug("Main binlog is newer than dump file. Syncing dump file with main binlog...")

				err = SyncTablesTillDestination([]string{table.Name}, mainBinlogPos, tableBinlogPos, esClient, syncer)
				if err != nil {
					return fmt.Errorf("failed to sync table until destination: %w", err)
				}
			} else if newerBinlog == &tableBinlogPos{
				MainLogger.Debug("Dump file is newer than main binlog. Syncing main binlog with dump file...")
				err = SyncMainBinlogTillPosition(tableBinlogPos, esClient, syncer)
				if err != nil {
					return fmt.Errorf("failed to sync table until destination: %w", err)
				}
			}
			MainLogger.Debug("Sync completed for table:" + table.Name)
			err = SetTableStatus(table.Name, "syncing")
			if err != nil {
				return fmt.Errorf("set table status %s: %w", table.Name, err)
			}

			table.Status = GetRegisteredTables()[table.Name].Status
		}
    }
	MainLogger.Info("Finished Processing table")
	return nil
}


func runTheSyncer(appConfig *config.Config, esClient *elasticsearch.Client, syncer *replication.BinlogSyncer) {
    registeredTables := GetRegisteredTables()
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
            initializeTables(appConfig, esClient, syncer)
        default:

			ev, err := streamer.GetEvent(context.Background())
			if err != nil {
				MainLogger.Error(err.Error())
				panic(err)
			}
			if ev == nil {
				MainLogger.Error("nil pointer to event")
			}
            err = processBinlogEvent(ev, &currentBinlogPos, tableNames, esClient)
			if err != nil {
				MainLogger.Error(err.Error())
			}
        }
    }
}

func convertBinlogRowsToArrayOfMaps(rows [][]interface{}, tableStructure []map[string]interface{}) ([]map[string]interface{}, error) {
    var values []map[string]interface{} 
    for _, row := range rows {
        var singleRecord = make(map[string]interface{})
        for j, val := range row {
            columnName, err := getColumnNameFromPosition(tableStructure, j)
            if err != nil {
                return nil, fmt.Errorf("Error getting column name from position %d: %w", j, err)
            }
            singleRecord[columnName] = val
        }
        values = append(values, singleRecord)
    }
	return values, nil
}

func SyncMainBinlogTillPosition(desBinlogPos syncerpack.BinlogPosition, esClient *elasticsearch.Client, syncer *replication.BinlogSyncer) error {
	MainLogger.Debug("Syncing main loop")
    currentBinlogPos, err := syncerpack.GetStoredBinlogCoordinates("main")
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
    err = SyncTablesTillDestination(synchingTableNames, desBinlogPos, currentBinlogPos, esClient, syncer)

    return err
}

// If both args are equal, nil will be returned
func GetNewerBinlogPosition(pos1, pos2 *syncerpack.BinlogPosition) *syncerpack.BinlogPosition {
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
            return nil // They are equal
        }
    }
}


func SyncTablesTillDestination(tableNames []string, desBinlogPos, currentBinlogPos syncerpack.BinlogPosition, esClient *elasticsearch.Client, syncer *replication.BinlogSyncer) error {
	MainLogger.Debug("Syncing: " + strings.Join(tableNames[:], ","))

    // Start sync with specified binlog file and position
    streamer, _ := syncer.StartSync(mysql.Position{
        Name: currentBinlogPos.Logfile,
        Pos: currentBinlogPos.Logpos,
    })

    for GetNewerBinlogPosition(&currentBinlogPos, &desBinlogPos) == &desBinlogPos {
		ev, err := streamer.GetEvent(context.Background())
		if err != nil {
			MainLogger.Error(err.Error())
			return err
		}
		if ev == nil {
			MainLogger.Error("nil pointer to event")
			return err
		}
		err = processBinlogEvent(ev, &currentBinlogPos, tableNames, esClient)
		if err != nil {
			MainLogger.Error(err.Error())
			return err
		}
    }

    return nil
}

func processBinlogEvent(ev *replication.BinlogEvent, currentBinlogPos *syncerpack.BinlogPosition, tableNames []string, esClient *elasticsearch.Client) error {
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
            tableStructure, _ := getTableStructure(getDumpTableStructureFilePath(eventTableName))
			records, err := convertBinlogRowsToArrayOfMaps(e.Rows, tableStructure)
			if err != nil {
				MainLogger.Error(err.Error())
				return err
			}
			err = bulkSendToElastic(eventTableName, records, esClient)
			if err != nil {
				MainLogger.Error(err.Error())
				return fmt.Errorf("Error sending data to Elastic: %w", err)
			}
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

			records, err := convertBinlogRowsToArrayOfMaps(e.Rows, tableStructure)
			if err != nil {
				MainLogger.Error(err.Error())
				return err
			}
			err = bulkUpdateToElastic(eventTableName, records, esClient)
			if err != nil {
				MainLogger.Error(err.Error())
				return err
			}
        case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			MainLogger.Debug("  🗑️ DELETE:")
            tableStructure, _ := getTableStructure(getDumpTableStructureFilePath(eventTableName))
			records, err:= convertBinlogRowsToArrayOfMaps(e.Rows, tableStructure)
			if err != nil {
				MainLogger.Error(err.Error())
				return err
			}
			err = bulkDeleteFromElastic(eventTableName, records, esClient)
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
