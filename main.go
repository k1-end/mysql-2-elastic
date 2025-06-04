package main

import (
	"fmt"
	"os"
)

// "context"
// "fmt"
// "time"
//
// "github.com/go-mysql-org/go-mysql/mysql"
// "github.com/go-mysql-org/go-mysql/replication"
// _ "github.com/pingcap/tidb/pkg/parser/test_driver" // Required for the parser to work

// const (
// 	queueDir                 = "queue"
// 	processedDir             = "processed"
// 	cleanupInterval          = 5 * time.Minute
// 	retentionPeriod          = 24 * time.Hour
//     registeredTablesFilePath = "data/registered-tables.json"
// )
//
//
// func main() {
//     newerBinlog, _ := CompareBinlogFiles(getDumpBinlogPositionFilePath("orders"), getMainBinlogPositionFilePath())
//     if newerBinlog == getDumpBinlogPositionFilePath("orders") {
//         //Sync main log
//     }else if newerBinlog == getMainBinlogPositionFilePath() {
//         //Sync dump log
//     }
//     cfg := replication.BinlogSyncerConfig {
//         ServerID: 100,
//         Flavor:   "mysql",
//         Host:     "127.0.0.1",
//         Port:     3310,
//         User:     "admin",
//         Password: "password",
//     }
//     syncer := replication.NewBinlogSyncer(cfg)
//
//     // Start sync with specified binlog file and position
//     binlogFile, binlogPos, _ := getMainBinlogCoordinates()
//     streamer, _ := syncer.StartSync(mysql.Position{binlogFile, binlogPos})
//     for {
//         ev, _ := streamer.GetEvent(context.Background())
//         //print position get from event
//         fmt.Printf("Current Binlog Position: %d\n", ev.Header.LogPos)
//         switch e := ev.Event.(type) {
//         case *replication.RotateEvent:
//             binlogFile = string(e.NextLogName)
//             binlogPos = uint32(e.Position)
//             fmt.Printf("🔄 Binlog rotated to: %s at position %d\n", binlogFile, binlogPos)
//         case *replication.TableMapEvent:
//             // Store table metadata. This event precedes RowsEvents and maps a table ID
//             // to a table name. You'll need this to interpret the RowsEvents.
//             // For actual parsing of row data, you would typically fetch the full
//             // table schema (column names, types) here using e.Schema and e.Table.
//             // The go-mysql library might do some of this implicitly if set up with a schema tracker,
//             // or you might need to manage it more explicitly.
//             // For this example, we'll just log it.
//             // You might need to fetch schema information from your DB here if not using a schema cache.
//             // For now, we are just noting it down; a proper implementation would use this to parse rows.
//             fmt.Printf("🗺️ TableMapEvent: TableID: %d, Schema: %s, Table: %s\n", e.TableID, string(e.Schema), string(e.Table))
//         // Here you would typically fetch or load the schema.Table for this e.TableID
//         // For example:
//         // tableInfo, err := schema.NewTable(string(e.Schema), string(e.Table))
//         // if err == nil { // Simplified error handling
//         //    // You'd fetch column definitions from your DB for tableInfo
//         //    tableMap[e.TableID] = tableInfo
//         // }
//
//         case *replication.RowsEvent:
//             // This event contains the row data for INSERT, UPDATE, DELETE
//             tableName := string(e.Table.Table) // Get table name from the event
//             schemaName := string(e.Table.Schema) // Get schema name
//
//             // To properly interpret e.Rows, you need the schema.Table object that
//             // corresponds to e.TableID, which you should have cached from a prior TableMapEvent.
//             // currentTableSchema := tableMap[e.TableID]
//             // if currentTableSchema == nil {
//             // 	fmt.Printf("⚠️ Warning: No schema found for table ID %d (%s.%s). Skipping row data.\n", e.TableID, schemaName, tableName)
//             // 	continue
//             // }
//
//             fmt.Printf("ROW EVENT for %s.%s\n", schemaName, tableName)
//
//             switch ev.Header.EventType {
//             case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
//                 fmt.Println("  ➡️ INSERT:")
//                 for _, row := range e.Rows {
//                     // `row` is a slice of interface{}, representing column values
//                     fmt.Printf("    New Row: %v\n", row)
//                 }
//             case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
//                 fmt.Println("  🔄 UPDATE:")
//                 fmt.Println(e.ColumnBitmap2)
//                 // For UPDATE events, e.Rows contains pairs of [before-image, after-image]
//                 // The length of e.Rows will be N*2, where N is the number of updated rows.
//                 for i := 0; i < len(e.Rows); i += 2 {
//                     beforeValues := e.Rows[i]
//                     afterValues := e.Rows[i+1]
//                     fmt.Printf("    Before: %v\n", beforeValues)
//                     fmt.Printf("    After:  %v\n", afterValues)
//                     // `beforeValues` and `afterValues` are []interface{}
//                     // representing the data in each column of the row.
//                     // The order of values corresponds to the column order in the table.
//                     // You would use the `currentTableSchema.Columns` to map these values to column names and types.
//                 }
//             case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
//                 fmt.Println("  🗑️ DELETE:")
//                 for _, row := range e.Rows {
//                     // `row` is a slice of interface{}, representing column values of the deleted row
//                     fmt.Printf("    Deleted Row: %v\n", row)
//                 }
//             }
//         case *replication.QueryEvent:
//             // DDL changes, etc.
//             fmt.Printf("📜 QueryEvent: Schema: %s, Query: %s\n", string(e.Schema), string(e.Query))
//         case *replication.GTIDEvent:
//         // Handle GTID event if you are using GTID based replication
//         // gtid, _ := mysql.ParseGTIDSet("mysql", string(e.GTID))
//         // fmt.Printf("GTID Event: %s\n", gtid.String())
//         default:
//         // You can print other events if you are interested
//         // fmt.Printf("Received event Type: %s\n", ev.Header.EventType)
//         }
//     }
//
// }
//
// func getMainBinlogCoordinates() (string, uint32, error) {
//     return "binlog.000022", 157, nil
//     // {"logfile":"binlog.000022","logpos":157}
// }

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
        case "dumping":
			fmt.Println("Status: Dumping")
            ClearIncompleteDumpedData(table.Name)
            InitialDump(table.Name)
            SendDataToElasticFromDumpfile(table.Name)
		case "dumped":
			fmt.Println("Status: Dumped")
            SendDataToElasticFromDumpfile(table.Name)
		case "moving":
			fmt.Println("Status: Dumped")
            SendDataToElasticFromDumpfile(table.Name)
		case "syncing":
            continue
		default:
			fmt.Printf("Unknown status for table %s: %s\n", table.Name, table.Status)
            os.Exit(1)
		}
    }
}
