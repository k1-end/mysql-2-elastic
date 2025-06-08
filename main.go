package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"slices"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)



func main()  {
    registeredTables := GetRegisteredTables()
    // Is there any registered tables?
	if len(registeredTables) == 0 {
		fmt.Println("No registered tables.\n--- End ---")
		return
	}

    fmt.Println("Processing table")
	processTables(registeredTables)
    fmt.Println("Finished Processing table")
}

func processTables(registeredTables map[string]RegisteredTable) {
    for _, table := range registeredTables {
        switch table.Status {
        case "created":
            fmt.Println("Status: Created")
            ClearIncompleteDumpedData(table.Name)
            InitialDump(table.Name)
            SendDataToElasticFromDumpfile(table.Name)
            SyncWithTheMainLoop(table.Name)
        case "dumping":
			fmt.Println("Status: Dumping")
            ClearIncompleteDumpedData(table.Name)
            InitialDump(table.Name)
            SendDataToElasticFromDumpfile(table.Name)
            SyncWithTheMainLoop(table.Name)
		case "dumped":
			fmt.Println("Status: Dumped")
            SendDataToElasticFromDumpfile(table.Name)
            SyncWithTheMainLoop(table.Name)
		case "moving":
			fmt.Println("Status: Moving")
            SendDataToElasticFromDumpfile(table.Name)
            SyncWithTheMainLoop(table.Name)
		case "moved":
			fmt.Println("Status: Moved")
            err := SyncWithTheMainLoop(table.Name)
            if err != nil {
                log.Fatalf("Error syncing with the main loop for table %s: %v", table.Name, err)
            }
		case "syncing":
            continue
		default:
            log.Fatalf("Unknown status for table %s: %s\n", table.Name, table.Status)
		}
    }
}

func SyncWithTheMainLoop(tableName string) error{
    fmt.Println("Syncing with the main loop for table:", tableName)
    tableBinlogPos, err := GetBinlogCoordinates(tableName)
    if err != nil {
        tableBinlogPos, err = GetBinlogCoordinatesFromDumpfile(GetDumpFilePath(tableName))
        if err != nil {
            return fmt.Errorf("failed to parse binlog coordinates from dump file: %w", err)
        }
        WriteDumpfilePosition(tableName) // for safety
    }

    mainBinlogPos, err := GetBinlogCoordinates("main")
    if err != nil {
        return fmt.Errorf("failed to get binlog coordinates: %w", err)
    }

    if mainBinlogPos.Logfile > tableBinlogPos.Logfile || (mainBinlogPos.Logfile == tableBinlogPos.Logfile && mainBinlogPos.Logpos > tableBinlogPos.Logpos) {
        // Main binlog is newer, so we need to sync the dump file with the main binlog
        fmt.Println("Main binlog is newer than dump file. Syncing dump file with main binlog...")
        SyncTableUntileDestination(tableName, mainBinlogPos)
    }

    if mainBinlogPos.Logfile < tableBinlogPos.Logfile || (mainBinlogPos.Logfile == tableBinlogPos.Logfile && mainBinlogPos.Logpos < tableBinlogPos.Logpos) {
        fmt.Println("Dump file is newer than main binlog. Syncing main binlog with dump file...")
        SyncMainBinlogWithDumpFile(tableBinlogPos)
    }
    fmt.Println("Sync completed for table:", tableName)
    SetTableStatus(tableName, "syncing")
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

func SyncTableUntileDestination(tableName string, desBinlogPos BinlogPosition) error {
    fmt.Println("Syncing table:", tableName)
    cfg := replication.BinlogSyncerConfig {
        ServerID: 100,
        Flavor:   "mysql",
        Host:     "127.0.0.1",
        Port:     3310,
        User:     "admin",
        Password: "password",
    }

    syncer := replication.NewBinlogSyncer(cfg)
    currentBinlogPos, err := ParseBinlogCoordinatesFile(GetDumpBinlogPositionFilePath(tableName))
    if err != nil {
        return fmt.Errorf("failed to parse binlog coordinates from dump file: %w", err)
    }

    // Start sync with specified binlog file and position
    streamer, _ := syncer.StartSync(mysql.Position{
        Name: currentBinlogPos.Logfile,
        Pos: currentBinlogPos.Logpos,
    })

    for CompareBinlogPositions(currentBinlogPos, desBinlogPos) < 0 {
        ev, _ := streamer.GetEvent(context.Background())
        //print position get from event
        switch e := ev.Event.(type) {
        case *replication.RotateEvent:
            currentBinlogPos.Logfile = string(e.NextLogName)
            currentBinlogPos.Logpos = uint32(e.Position)
            fmt.Printf("🔄 Binlog rotated to: %s at position %d\n", currentBinlogPos.Logfile, currentBinlogPos.Logpos)
        
        case *replication.RowsEvent:
            // This event contains the row data for INSERT, UPDATE, DELETE
            eventTableName := string(e.Table.Table) // Get table name from the event
            if eventTableName != tableName {
                continue
            }
            schemaName := string(e.Table.Schema) // Get schema name

            fmt.Printf("ROW EVENT for %s.%s\n", schemaName, tableName)

            switch ev.Header.EventType {
            case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
                fmt.Println("  ➡️ INSERT:")
                tableStructure, _ := getTableStructure(getDumpTableStructureFilePath(tableName))
                bulkInsertBinlogRowsToElastic(e.Rows, tableStructure, tableName)
            case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
                fmt.Println("  🔄 UPDATE:")
                fmt.Println(e.ColumnBitmap2)
                // For UPDATE events, e.Rows contains pairs of [before-image, after-image]
                // The length of e.Rows will be N*2, where N is the number of updated rows.
                tableStructure, _ := getTableStructure(getDumpTableStructureFilePath(tableName))
                var afterDocs [][]interface{}
                for i := 0; i < len(e.Rows); i += 2 {
                    afterValues := e.Rows[i+1]
                    afterDocs = append(afterDocs, afterValues)
                }
                bulkUpdateBinlogRowsToElastic(afterDocs, tableStructure, tableName)
            case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
                fmt.Println("  🗑️ DELETE:")
                tableStructure, _ := getTableStructure(getDumpTableStructureFilePath(tableName))
                bulkDeleteBinlogRowsFromElastic(e.Rows, tableStructure, tableName)
            }
        case *replication.QueryEvent:
            // DDL changes, etc.
        default:
        }
        WriteBinlogPosition(currentBinlogPos, tableName) // Update the position after rotation
    }

    return nil
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

func bulkInsertBinlogRowsToElastic(rows [][]interface{}, tableStructure []map[string]interface{}, tableName string) error {
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

    err := bulkSendToElastic(tableName, values)
    if err != nil {
        return fmt.Errorf("Error sending data to Elastic: %w", err)
    }
    return nil
}

func bulkUpdateBinlogRowsToElastic(rows [][]interface{}, tableStructure []map[string]interface{}, tableName string) error {
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

    bulkUpdateToElastic(tableName, values)

    return nil
}




func bulkDeleteBinlogRowsFromElastic(rows [][]interface{}, tableStructure []map[string]interface{}, tableName string) error {
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
    err := bulkDeleteFromElastic(tableName, values)
    if err != nil {
        return fmt.Errorf("Error deleting data from Elastic: %w", err)
    }
    return nil
}

func SyncMainBinlogWithDumpFile(desBinlogPos BinlogPosition) error {
    fmt.Println("Syncing main loop")
    cfg := replication.BinlogSyncerConfig {
        ServerID: 100,
        Flavor:   "mysql",
        Host:     "127.0.0.1",
        Port:     3310,
        User:     "admin",
        Password: "password",
    }

    syncer := replication.NewBinlogSyncer(cfg)
    currentBinlogPos, err := GetBinlogCoordinates("main")
    if err != nil {
        return fmt.Errorf("failed to parse binlog coordinates from dump file: %w", err)
    }

    // Start sync with specified binlog file and position
    streamer, _ := syncer.StartSync(mysql.Position{
        Name: currentBinlogPos.Logfile,
        Pos: currentBinlogPos.Logpos,
    })

    registeredTables := GetRegisteredTables()
    // filter registeredTables by syncing status
    var synchingTableNames []string
    for name, table := range registeredTables {
        if table.Status == "syncing" {
            synchingTableNames = append(synchingTableNames, name)
        }
    }
            

    for CompareBinlogPositions(currentBinlogPos, desBinlogPos) < 0 {
        ev, _ := streamer.GetEvent(context.Background())
        //print position get from event
        switch e := ev.Event.(type) {
        case *replication.RotateEvent:
            currentBinlogPos.Logfile = string(e.NextLogName)
            currentBinlogPos.Logpos = uint32(e.Position)
            fmt.Printf("🔄 Binlog rotated to: %s at position %d\n", currentBinlogPos.Logfile, currentBinlogPos.Logpos)
        
        case *replication.RowsEvent:
            // This event contains the row data for INSERT, UPDATE, DELETE
            eventTableName := string(e.Table.Table) // Get table name from the event
            if !slices.Contains(synchingTableNames, eventTableName) {
                continue
            }
            schemaName := string(e.Table.Schema) // Get schema name

            fmt.Printf("ROW EVENT for %s.%s\n", schemaName, eventTableName)

            switch ev.Header.EventType {
            case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
                fmt.Println("  ➡️ INSERT:")
                tableStructure, _ := getTableStructure(getDumpTableStructureFilePath(eventTableName))
                bulkInsertBinlogRowsToElastic(e.Rows, tableStructure, eventTableName)
            case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
                fmt.Println("  🔄 UPDATE:")
                fmt.Println(e.ColumnBitmap2)
                // For UPDATE events, e.Rows contains pairs of [before-image, after-image]
                // The length of e.Rows will be N*2, where N is the number of updated rows.
                tableStructure, _ := getTableStructure(getDumpTableStructureFilePath(eventTableName))
                var afterDocs [][]interface{}
                for i := 0; i < len(e.Rows); i += 2 {
                    afterValues := e.Rows[i+1]
                    afterDocs = append(afterDocs, afterValues)
                }
                bulkUpdateBinlogRowsToElastic(afterDocs, tableStructure, eventTableName)
            case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
                fmt.Println("  🗑️ DELETE:")
                tableStructure, _ := getTableStructure(getDumpTableStructureFilePath(eventTableName))
                bulkDeleteBinlogRowsFromElastic(e.Rows, tableStructure, eventTableName)
            }
        case *replication.QueryEvent:
            // DDL changes, etc.
        default:
        }
        WriteBinlogPosition(currentBinlogPos, "main") // Update the position after rotation
    }

    return nil
}

// write a function to compare BinlogPosition
func CompareBinlogPositions(pos1, pos2 BinlogPosition) int {
    if pos1.Logfile < pos2.Logfile {
        return -1
    } else if pos1.Logfile > pos2.Logfile {
        return 1
    } else {
        if pos1.Logpos < pos2.Logpos {
            return -1
        } else if pos1.Logpos > pos2.Logpos {
            return 1
        } else {
            return 0
        }
    }
}
