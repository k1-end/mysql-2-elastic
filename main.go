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


    if GetNewerBinlogPosition(mainBinlogPos, tableBinlogPos) == mainBinlogPos  && mainBinlogPos != tableBinlogPos{
        // Main binlog is newer, so we need to sync the dump file with the main binlog
        fmt.Println("Main binlog is newer than dump file. Syncing dump file with main binlog...")
        err = SyncTableUntileDestination(tableName, mainBinlogPos)
        if err != nil {
            return fmt.Errorf("failed to sync table until destination: %w", err)
        }
    }

    if GetNewerBinlogPosition(mainBinlogPos, tableBinlogPos) == tableBinlogPos  && mainBinlogPos != tableBinlogPos{
        fmt.Println("Dump file is newer than main binlog. Syncing main binlog with dump file...")
        err = SyncMainBinlogWithDumpFile(tableBinlogPos)
        if err != nil {
            return fmt.Errorf("failed to sync table until destination: %w", err)
        }
    }
    fmt.Println("Sync completed for table:", tableName)
    err = SetTableStatus(tableName, "syncing")
    if err != nil {
        return fmt.Errorf("failed to sync table until destination: %w", err)
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

func SyncTableUntileDestination(tableName string, desBinlogPos BinlogPosition) error {
    fmt.Println("Syncing table:", tableName)

    currentBinlogPos, err := ParseBinlogCoordinatesFile(GetDumpBinlogPositionFilePath(tableName))
    if err != nil {
        return fmt.Errorf("failed to parse binlog coordinates from dump file: %w", err)
    }

    return SyncTablesTillDestination([]string{tableName}, desBinlogPos, currentBinlogPos)

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
    SyncTablesTillDestination(synchingTableNames, desBinlogPos, currentBinlogPos)

    return nil
}

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


func SyncTablesTillDestination(tableNames []string, desBinlogPos, currentBinlogPos BinlogPosition) error {
    fmt.Println("Syncing: ", tableNames)
    cfg := replication.BinlogSyncerConfig {
        ServerID: 100,
        Flavor:   "mysql",
        Host:     "127.0.0.1",
        Port:     3310,
        User:     "admin",
        Password: "password",
    }

    syncer := replication.NewBinlogSyncer(cfg)

    // Start sync with specified binlog file and position
    streamer, _ := syncer.StartSync(mysql.Position{
        Name: currentBinlogPos.Logfile,
        Pos: currentBinlogPos.Logpos,
    })

    for GetNewerBinlogPosition(currentBinlogPos, desBinlogPos) == desBinlogPos  && desBinlogPos != currentBinlogPos{
        ev, _ := streamer.GetEvent(context.Background())
        currentBinlogPos.Logpos = uint32(ev.Header.LogPos) // Update the current position from the event header
        //print position get from event
        switch e := ev.Event.(type) {
        case *replication.RotateEvent:
            currentBinlogPos.Logfile = string(e.NextLogName)
            fmt.Printf("🔄 Binlog rotated to: %s at position %d\n", currentBinlogPos.Logfile, currentBinlogPos.Logpos)
        
        case *replication.RowsEvent:
            // This event contains the row data for INSERT, UPDATE, DELETE
            eventTableName := string(e.Table.Table) // Get table name from the event
            if !slices.Contains(tableNames, eventTableName) {
                WriteBinlogPosition(currentBinlogPos, "main") // Update the position after rotation
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
