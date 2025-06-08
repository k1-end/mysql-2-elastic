package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
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
        case "dumping":
			fmt.Println("Status: Dumping")
            ClearIncompleteDumpedData(table.Name)
            InitialDump(table.Name)
            SendDataToElasticFromDumpfile(table.Name)
		case "dumped":
			fmt.Println("Status: Dumped")
            SendDataToElasticFromDumpfile(table.Name)
		case "moving":
			fmt.Println("Status: Moving")
            SendDataToElasticFromDumpfile(table.Name)
		case "moved":
			fmt.Println("Status: Moved")
            SyncWithTheMainLoop(table.Name)
		case "syncing":
            continue
		default:
			fmt.Printf("Unknown status for table %s: %s\n", table.Name, table.Status)
            os.Exit(1)
		}
    }
}

func SyncWithTheMainLoop(tableName string) error{
    fmt.Println("Syncing with the main loop for table:", tableName)
    tableBinlogPos, err := ParseBinlogCoordinatesFile(GetDumpBinlogPositionFilePath(tableName))
    if err != nil {
        tableBinlogPos, err = GetBinlogCoordinatesFromDumpfile(GetDumpFilePath(tableName))
        if err != nil {
            return fmt.Errorf("failed to parse binlog coordinates from dump file: %w", err)
        }
        WriteDumpfilePosition(tableName) // for safety
    }

    mainBinlogPos, err := GetMainBinlogCoordinates()
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
    return nil
}


func GetMainBinlogCoordinates() (BinlogPosition, error) {
	data, err := os.ReadFile(GetMainBinlogPositionFilePath())

	if err != nil && !os.IsNotExist(err) {
		return BinlogPosition{}, fmt.Errorf("error reading file %s: %w", GetMainBinlogPositionFilePath(), err)
	}

	empty := len(strings.TrimSpace(string(data))) == 0 || os.IsNotExist(err)

	if empty {
		return BinlogPosition{}, fmt.Errorf("both files are empty or do not exist")
	}

	var pos BinlogPosition
	if err := json.Unmarshal(data, &pos); err != nil {
		return BinlogPosition{}, fmt.Errorf("error unmarshalling JSON from %s: %w", GetMainBinlogPositionFilePath(), err)
	}

	return pos, nil
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
                bulkInsertToElastic(e.Rows, tableStructure, tableName)
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
                bulkUpdateToElastic(afterDocs, tableStructure, tableName)
            case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
                fmt.Println("  🗑️ DELETE:")
                tableStructure, _ := getTableStructure(getDumpTableStructureFilePath(tableName))
                bulkDeleteFromElastic(e.Rows, tableStructure, tableName)
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

    err = os.WriteFile(GetDumpBinlogPositionFilePath(tableName), jsonData, 0644)
    if err != nil {
        return fmt.Errorf("failed to write binlog coordinates to file: %w", err)
    }

    return nil
}


func WriteMainBinlogPosition(binlogPos BinlogPosition) error {
    jsonData, err := json.Marshal(map[string]interface{}{
        "logfile": binlogPos.Logfile,
        "logpos":  binlogPos.Logpos,
    })
    if err != nil {
        return fmt.Errorf("failed to marshal binlog coordinates: %w", err)
    }

    filePath := GetMainBinlogPositionFilePath()

    err = os.WriteFile(filePath, jsonData, 0644)
    if err != nil {
        return fmt.Errorf("failed to write binlog coordinates to file: %w", err)
    }

    return nil
}


func bulkInsertToElastic(rows [][]interface{}, tableStructure []map[string]interface{}, tableName string) error {
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

func bulkUpdateToElastic(rows [][]interface{}, tableStructure []map[string]interface{}, tableName string) error {
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


    cfg := elasticsearch.Config{
        Addresses: []string{
            "http://localhost:9200",
        },
        Username: "elastic",
        Password: "elastic",
    }
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the Elasticsearch client: %s", err)
	}

    _, err = es.Info()
	if err != nil {
		log.Fatalf("Error getting client info: %s", err)
	}

    // --- Build the NDJSON request body ---
	// Each operation requires two lines:
	// 1. Action and metadata (e.g., {"update": {"_id": "document_id"}})
	// 2. Document source (e.g., {"doc": {"field": "value"}})

    var bulkBody strings.Builder
	var successfulUpdates int
	var failedUpdates int


    for _, doc := range values {

        var id string
        switch v := doc["id"].(type) {
        case string:
            // If the ID is a string, use it directly
            id = doc["id"].(string)
        case int:
            // If the ID is an int, convert it to string
            id = fmt.Sprintf("%d", v)
        case int64:
            // If the ID is an int, convert it to string
            id = fmt.Sprintf("%d", v)
        default:
            fmt.Printf("Unexpected type for ID: %T\n", v)
            os.Exit(1)
            
        }

        meta := map[string]interface{}{
            "update": map[string]interface{}{
                "_index": tableName,
                "_id":    id,
            },
        }
        metaBytes, err := json.Marshal(meta)
        if err != nil {
            log.Fatalf("Error marshaling meta1: %s", err)
        }
        bulkBody.Write(metaBytes)
        bulkBody.WriteString("\n")

        docBytes, err := json.Marshal(map[string]interface{}{
            "doc": doc,
        })
        if err != nil {
            log.Fatalf("Error marshaling docUpdate1: %s", err)
        }
        bulkBody.Write(docBytes)
        bulkBody.WriteString("\n")
		if err != nil {
			log.Fatalf("Unexpected error adding item '%s' to BulkIndexer: %s", id, err)
            os.Exit(1)
		}
    }

    res, err := es.Bulk(
		bytes.NewReader([]byte(bulkBody.String())),
		es.Bulk.WithContext(context.Background()),
	)
    fmt.Println(bulkBody.String())

    defer res.Body.Close()

    if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
		}
		log.Fatalf("Elasticsearch returned an error [%s]: %s", res.Status(), e["error"])
	}

    var bulkRes map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&bulkRes); err != nil {
		log.Fatalf("Error parsing the bulk response: %s", err)
	}

	fmt.Println("\n--- Bulk Operation Response ---")
	if errors, ok := bulkRes["errors"].(bool); ok && errors {
		fmt.Println("Warning: Some items failed in the bulk request!")
		if items, ok := bulkRes["items"].([]interface{}); ok {
			for i, item := range items {
				itemMap := item.(map[string]interface{})
				if updateStatus, ok := itemMap["update"].(map[string]interface{}); ok {
					if status, ok := updateStatus["status"].(float64); ok && status >= 400 {
						failedUpdates++
						errorInfo := updateStatus["error"].(map[string]interface{})
						fmt.Printf("  Item %d (ID: %s) FAILED: %s - %s\n", i+1, updateStatus["_id"], errorInfo["type"], errorInfo["reason"])
					} else {
						successfulUpdates++
						fmt.Printf("  Item %d (ID: %s) SUCCESS: Version %d\n", i+1, updateStatus["_id"], int(updateStatus["_version"].(float64)))
					}
				}
			}
		}
	} else {
		fmt.Println("All items in the bulk request were successful!")
		if items, ok := bulkRes["items"].([]interface{}); ok {
			for i, item := range items {
				itemMap := item.(map[string]interface{})
				if updateStatus, ok := itemMap["update"].(map[string]interface{}); ok {
					successfulUpdates++
					fmt.Printf("  Item %d (ID: %s) SUCCESS: Version %d\n", i+1, updateStatus["_id"], int(updateStatus["_version"].(float64)))
				}
			}
		}
	}

    fmt.Printf("\n--- Bulk Update Summary ---\n")
	fmt.Printf("Total documents attempted: %d\n", len(values))
	fmt.Printf("Successful updates: %d\n", successfulUpdates)
	fmt.Printf("Failed updates: %d\n", failedUpdates)

	if failedUpdates == 0 {
		fmt.Println("All documents updated successfully!")
	} else {
		fmt.Println("Some documents failed to update. Check logs above.")
	}

    return nil
}




func bulkDeleteFromElastic(rows [][]interface{}, tableStructure []map[string]interface{}, tableName string) error {

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
    cfg := elasticsearch.Config{
        Addresses: []string{
            "http://localhost:9200",
        },
        Username: "elastic",
        Password: "elastic",
    }
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the Elasticsearch client: %s", err)
	}

	// Ping to ensure connection
	res, err := es.Info()
	if err != nil {
		log.Fatalf("Error getting client info: %s", err)
	}
	defer res.Body.Close()
	fmt.Println("Successfully connected to Elasticsearch!")

	// --- Build the NDJSON request body ---
	// Each delete operation requires one line:
	//  {"delete": {"_index": "index_name", "_id": "document_id"}}

	var bulkBody strings.Builder
	var successfulDeletes int
	var failedDeletes int

    for _, doc := range values {
        var id string
        switch v := doc["id"].(type) {
        case string:
            // If the ID is a string, use it directly
            id = doc["id"].(string)
        case int:
            // If the ID is an int, convert it to string
            id = fmt.Sprintf("%d", v)
        case int64:
            // If the ID is an int, convert it to string
            id = fmt.Sprintf("%d", v)
        default:
            fmt.Printf("Unexpected type for ID: %T\n", v)
            os.Exit(1)
            
        }
        meta := map[string]interface{}{
            "delete": map[string]interface{}{
                "_index": tableName,
                "_id":    id,
            },
        }
        metaBytes, err := json.Marshal(meta)
        if err != nil {
            log.Fatalf("Error marshaling meta1: %s", err)
        }
        bulkBody.Write(metaBytes)
        bulkBody.WriteString("\n")
    }



	// --- Send the bulk request ---
	fmt.Printf("Sending bulk delete request for documents ...\n")

	// Refresh the index immediately after the bulk operation for searchability (optional, for testing)
	refresh := "wait_for" // or "true" for immediate refresh, or "" for default

	res, err = es.Bulk(
		bytes.NewReader([]byte(bulkBody.String())),
		es.Bulk.WithContext(context.Background()),
		es.Bulk.WithRefresh(refresh), // Apply refresh setting
	)
	if err != nil {
		log.Fatalf("FATAL ERROR: %s", err)
	}
	defer res.Body.Close()

	// --- Process the response synchronously ---
	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
		}
		log.Fatalf("Elasticsearch returned an error [%s]: %s", res.Status(), e["error"])
	}

	var bulkRes map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&bulkRes); err != nil {
		log.Fatalf("Error parsing the bulk response: %s", err)
	}

	fmt.Println("\n--- Bulk Delete Operation Response ---")
	if errors, ok := bulkRes["errors"].(bool); ok && errors {
		fmt.Println("Warning: Some items failed in the bulk request!")
		if items, ok := bulkRes["items"].([]interface{}); ok {
			for i, item := range items {
				itemMap := item.(map[string]interface{})
				if deleteStatus, ok := itemMap["delete"].(map[string]interface{}); ok {
					if status, ok := deleteStatus["status"].(float64); ok && status >= 400 {
						failedDeletes++
						errorInfo := deleteStatus["error"].(map[string]interface{})
						fmt.Printf("  Item %d (ID: %s) FAILED: %s - %s\n", i+1, deleteStatus["_id"], errorInfo["type"], errorInfo["reason"])
					} else {
						successfulDeletes++
						fmt.Printf("  Item %d (ID: %s) SUCCESS: Version %d\n", i+1, deleteStatus["_id"], int(deleteStatus["_version"].(float64)))
					}
				}
			}
		}
	} else {
		fmt.Println("All items in the bulk request were successful!")
		if items, ok := bulkRes["items"].([]interface{}); ok {
			for i, item := range items {
				itemMap := item.(map[string]interface{})
				if deleteStatus, ok := itemMap["delete"].(map[string]interface{}); ok {
					successfulDeletes++
					fmt.Printf("  Item %d (ID: %s) SUCCESS: Version %d\n", i+1, deleteStatus["_id"], int(deleteStatus["_version"].(float64)))
				}
			}
		}
	}

	fmt.Printf("\n--- Bulk Delete Summary ---\n")
	fmt.Printf("Total documents attempted to delete: %d\n", len(values))
	fmt.Printf("Successful deletes: %d\n", successfulDeletes)
	fmt.Printf("Failed deletes: %d\n", failedDeletes)

	if failedDeletes == 0 {
		fmt.Println("All documents deleted successfully!")
	} else {
		fmt.Println("Some documents failed to delete. Check logs above.")
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
    currentBinlogPos, err := GetMainBinlogCoordinates()
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
                bulkInsertToElastic(e.Rows, tableStructure, eventTableName)
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
                bulkUpdateToElastic(afterDocs, tableStructure, eventTableName)
            case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
                fmt.Println("  🗑️ DELETE:")
                tableStructure, _ := getTableStructure(getDumpTableStructureFilePath(eventTableName))
                bulkDeleteFromElastic(e.Rows, tableStructure, eventTableName)
            }
        case *replication.QueryEvent:
            // DDL changes, etc.
        default:
        }
        WriteMainBinlogPosition(currentBinlogPos) // Update the position after rotation
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
