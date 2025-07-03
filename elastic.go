package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func bulkSendToElastic(indexName string, documents []map[string]interface{}) error{
    if len(documents) == 0 {
        return fmt.Errorf("no documents to index")
    }
    var buf bytes.Buffer
    for _, doc := range documents {
        // Prepare the metadata for the bulk operation (index action)
        meta := map[string]interface{}{
            "index": map[string]interface{}{
                "_index": indexName,
            },
        }
        // If your document has an "id" field, you can use it for the document ID in Elasticsearch
        if docID, ok := doc["id"]; ok {
            meta["index"].(map[string]interface{})["_id"] = fmt.Sprintf("%v", docID)
        }

        metaBytes, err := json.Marshal(meta)
        if err != nil {
            MainLogger.Debug(fmt.Sprintf("Error marshaling bulk metadata: %s", err))
            continue // Or handle error more robustly
        }
        buf.Write(metaBytes)
        buf.WriteByte('\n')

        // Prepare the document source
        docBytes, err := json.Marshal(doc)
        if err != nil {
            MainLogger.Debug(fmt.Sprintf("Error marshaling document: %s for doc: %+v", err, doc))
            continue // Or handle error more robustly
        }
        buf.Write(docBytes)
        buf.WriteByte('\n')
    }

    // Create the Bulk request
    req := esapi.BulkRequest{
        Index: indexName,
        Body:  &buf,
        // Refresh: "true", // Uncomment if you want to make documents searchable immediately (slower indexing)
    }

    cfg := elasticsearch.Config{
        Addresses: []string{
            AppConfiguration.Elastic.Address,
        },
        Username: AppConfiguration.Elastic.Username,
        Password: AppConfiguration.Elastic.Password,
    }
    es, err := elasticsearch.NewClient(cfg)
    
    if err != nil {
        MainLogger.Error(fmt.Sprintf("Error creating the Elasticsearch client: %s", err))
		panic(err)
    }

    // Ping the Elasticsearch server to verify connection (optional)
    res, err := es.Info()
    if err != nil {
        MainLogger.Error(fmt.Sprintf("Error getting Elasticsearch info: %s", err))
		panic(err)
    }
    defer res.Body.Close()
	MainLogger.Debug("Elasticsearch Info:" + res.Status())
    ctx := context.Background()

    // Perform the bulk request
    res, err = req.Do(ctx, es)
    if err != nil {
        return fmt.Errorf("error performing bulk request: %w", err)
    }
    defer res.Body.Close()

    if res.IsError() {
        var raw map[string]interface{}
        if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
            return fmt.Errorf("failure to parse response body: %s", err)
        }
        return fmt.Errorf("bulk request failed: [%s] %s: %s",
            res.Status(),
            raw["error"].(map[string]interface{})["type"],
            raw["error"].(map[string]interface{})["reason"],
            )
    }

    // You can optionally parse the response to check for individual item errors
    // For brevity, this example doesn't parse the full bulk response items.
	MainLogger.Debug("Bulk indexing successful or partially successful. Response Status:" + res.Status())
    // To see the full response, you can read res.Body
    // responseBody, _ := io.ReadAll(res.Body)
    // log.Printf("Full response: %s", string(responseBody))

    return nil
}

func bulkUpdateToElastic(indexName string, documents []map[string]interface{}) error{
    if len(documents) == 0 {
        return fmt.Errorf("no documents to index")
    }
    cfg := elasticsearch.Config{
        Addresses: []string{
            AppConfiguration.Elastic.Address,
        },
        Username: AppConfiguration.Elastic.Username,
        Password: AppConfiguration.Elastic.Password,
    }
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		MainLogger.Error(fmt.Sprintf("Error creating the Elasticsearch client: %s", err))
		panic(err)
	}

    _, err = es.Info()
	if err != nil {
		MainLogger.Error(fmt.Sprintf("Error getting client info: %s", err))
		panic(err)
	}

    // --- Build the NDJSON request body ---
	// Each operation requires two lines:
	// 1. Action and metadata (e.g., {"update": {"_id": "document_id"}})
	// 2. Document source (e.g., {"doc": {"field": "value"}})

    var bulkBody strings.Builder
	var successfulUpdates int
	var failedUpdates int


    for _, doc := range documents {

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
			return errors.New(fmt.Sprintf("Unexpected type for ID: %T\n", v))
        }

        meta := map[string]interface{}{
            "update": map[string]interface{}{
                "_index": indexName,
                "_id":    id,
            },
        }
        metaBytes, err := json.Marshal(meta)
        if err != nil {
            MainLogger.Error(fmt.Sprintf("Error marshaling meta1: %s", err))
			panic(err)
        }
        bulkBody.Write(metaBytes)
        bulkBody.WriteString("\n")

        docBytes, err := json.Marshal(map[string]interface{}{
            "doc": doc,
        })
        if err != nil {
            MainLogger.Error(fmt.Sprintf("Error marshaling docUpdate1: %s", err))
			panic(err)
        }
        bulkBody.Write(docBytes)
        bulkBody.WriteString("\n")
		if err != nil {
			MainLogger.Error(fmt.Sprintf("Unexpected error adding item '%s' to BulkIndexer: %s", id, err))
			panic(err)
		}
    }

    res, err := es.Bulk(
		bytes.NewReader([]byte(bulkBody.String())),
		es.Bulk.WithContext(context.Background()),
	)
	MainLogger.Debug(bulkBody.String())

    defer res.Body.Close()

    if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			MainLogger.Error(fmt.Sprintf("Error parsing the response body: %s", err))
		}
		MainLogger.Error(fmt.Sprintf("Elasticsearch returned an error [%s]: %s", res.Status(), e["error"]))
		panic(err)
	}

    var bulkRes map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&bulkRes); err != nil {
		MainLogger.Error("Error parsing the bulk response: %s", err)
		panic(err)
	}

	MainLogger.Debug("\n--- Bulk Operation Response ---")
	if errors, ok := bulkRes["errors"].(bool); ok && errors {
		MainLogger.Debug("Warning: Some items failed in the bulk request!")
		if items, ok := bulkRes["items"].([]interface{}); ok {
			for i, item := range items {
				itemMap := item.(map[string]interface{})
				if updateStatus, ok := itemMap["update"].(map[string]interface{}); ok {
					if status, ok := updateStatus["status"].(float64); ok && status >= 400 {
						failedUpdates++
						errorInfo := updateStatus["error"].(map[string]interface{})
						MainLogger.Debug(fmt.Sprintf("  Item %d (ID: %s) FAILED: %s - %s\n", i+1, updateStatus["_id"], errorInfo["type"], errorInfo["reason"]))
					} else {
						successfulUpdates++
						MainLogger.Debug(fmt.Sprintf("  Item %d (ID: %s) SUCCESS: Version %d\n", i+1, updateStatus["_id"], int(updateStatus["_version"].(float64))))
					}
				}
			}
		}
	} else {
		MainLogger.Debug("All items in the bulk request were successful!")
		if items, ok := bulkRes["items"].([]interface{}); ok {
			for i, item := range items {
				itemMap := item.(map[string]interface{})
				if updateStatus, ok := itemMap["update"].(map[string]interface{}); ok {
					successfulUpdates++
					MainLogger.Debug(fmt.Sprintf("  Item %d (ID: %s) SUCCESS: Version %d\n", i+1, updateStatus["_id"], int(updateStatus["_version"].(float64))))
				}
			}
		}
	}

	MainLogger.Debug("\n--- Bulk Update Summary ---\n")
	MainLogger.Debug(fmt.Sprintf("Total documents attempted: %d\n", len(documents)))
	MainLogger.Debug(fmt.Sprintf("Successful updates: %d\n", successfulUpdates))
	MainLogger.Debug(fmt.Sprintf("Failed updates: %d\n", failedUpdates))

	if failedUpdates == 0 {
		MainLogger.Debug("All documents updated successfully!")
	} else {
		MainLogger.Debug("Some documents failed to update. Check logs above.")
	}
    return nil
}


func bulkDeleteFromElastic(indexName string, documents []map[string]interface{}) error {

    cfg := elasticsearch.Config{
        Addresses: []string{
            AppConfiguration.Elastic.Address,
        },
        Username: AppConfiguration.Elastic.Username,
        Password: AppConfiguration.Elastic.Password,
    }
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		MainLogger.Error("Error creating the Elasticsearch client: %s", err)
		panic(err)
	}

	// Ping to ensure connection
	res, err := es.Info()
	if err != nil {
		MainLogger.Error(fmt.Sprintf("Error getting client info: %s", err))
		panic(err)
	}
	defer res.Body.Close()
	MainLogger.Debug("Successfully connected to Elasticsearch!")

	// --- Build the NDJSON request body ---
	// Each delete operation requires one line:
	//  {"delete": {"_index": "index_name", "_id": "document_id"}}

	var bulkBody strings.Builder
	var successfulDeletes int
	var failedDeletes int

    for _, doc := range documents {
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
            MainLogger.Error(fmt.Sprintf("Unexpected type for ID: %T\n", v))
			panic(err)
            
        }
        meta := map[string]interface{}{
            "delete": map[string]interface{}{
                "_index": indexName,
                "_id":    id,
            },
        }
        metaBytes, err := json.Marshal(meta)
        if err != nil {
            MainLogger.Error(fmt.Sprintf("Error marshaling meta1: %s", err))
			panic(err)
        }
        bulkBody.Write(metaBytes)
        bulkBody.WriteString("\n")
    }

	// --- Send the bulk request ---
	MainLogger.Debug("Sending bulk delete request for documents ...\n")

	// Refresh the index immediately after the bulk operation for searchability (optional, for testing)
	refresh := "wait_for" // or "true" for immediate refresh, or "" for default

	res, err = es.Bulk(
		bytes.NewReader([]byte(bulkBody.String())),
		es.Bulk.WithContext(context.Background()),
		es.Bulk.WithRefresh(refresh), // Apply refresh setting
	)
	if err != nil {
		MainLogger.Error(fmt.Sprintf("FATAL ERROR: %s", err))
		panic(err)
	}
	defer res.Body.Close()

	// --- Process the response synchronously ---
	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			MainLogger.Error(fmt.Sprintf("Error parsing the response body: %s", err))
			panic(err)
		}
		MainLogger.Error(fmt.Sprintf("Elasticsearch returned an error [%s]: %s", res.Status(), e["error"]))
		panic(err)
	}

	var bulkRes map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&bulkRes); err != nil {
		MainLogger.Error(fmt.Sprintf("Error parsing the bulk response: %s", err))
		panic(err)
	}

	MainLogger.Debug("\n--- Bulk Delete Operation Response ---")
	if errors, ok := bulkRes["errors"].(bool); ok && errors {
		MainLogger.Debug("Warning: Some items failed in the bulk request!")
		if items, ok := bulkRes["items"].([]interface{}); ok {
			for i, item := range items {
				itemMap := item.(map[string]interface{})
				if deleteStatus, ok := itemMap["delete"].(map[string]interface{}); ok {
					if status, ok := deleteStatus["status"].(float64); ok && status >= 400 {
						failedDeletes++
						errorInfo := deleteStatus["error"].(map[string]interface{})
						MainLogger.Debug(fmt.Sprintf("  Item %d (ID: %s) FAILED: %s - %s\n", i+1, deleteStatus["_id"], errorInfo["type"], errorInfo["reason"]))
					} else {
						successfulDeletes++
						MainLogger.Debug(fmt.Sprintf("  Item %d (ID: %s) SUCCESS: Version %d\n", i+1, deleteStatus["_id"], int(deleteStatus["_version"].(float64))))
					}
				}
			}
		}
	} else {
		MainLogger.Debug("All items in the bulk request were successful!")
		if items, ok := bulkRes["items"].([]interface{}); ok {
			for i, item := range items {
				itemMap := item.(map[string]interface{})
				if deleteStatus, ok := itemMap["delete"].(map[string]interface{}); ok {
					successfulDeletes++
					MainLogger.Debug(fmt.Sprintf("  Item %d (ID: %s) SUCCESS: Version %d\n", i+1, deleteStatus["_id"], int(deleteStatus["_version"].(float64))))
				}
			}
		}
	}

	MainLogger.Debug(fmt.Sprintf("\n--- Bulk Delete Summary ---\n"))
	MainLogger.Debug(fmt.Sprintf("Total documents attempted to delete: %d\n", len(documents)))
	MainLogger.Debug(fmt.Sprintf("Successful deletes: %d\n", successfulDeletes))
	MainLogger.Debug(fmt.Sprintf("Failed deletes: %d\n", failedDeletes))

	if failedDeletes == 0 {
		MainLogger.Debug("All documents deleted successfully!")
	} else {
		MainLogger.Debug("Some documents failed to delete. Check logs above.")
	}
    return nil
}
