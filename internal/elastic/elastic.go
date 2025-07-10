package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/k1-end/mysql-2-elastic/internal/config"
)

func GetElasticClient(appConfig *config.Config) (*elasticsearch.Client, error){

    cfg := elasticsearch.Config{
        Addresses: []string{
            appConfig.Elastic.Address,
        },
        Username: appConfig.Elastic.Username,
        Password: appConfig.Elastic.Password,
    }
    es, err := elasticsearch.NewClient(cfg)
    
    if err != nil {
		return nil, fmt.Errorf("Error creating the Elasticsearch client: %s", err)
    }

    res, err := es.Info()
    if err != nil {
		return nil, fmt.Errorf("Error getting Elasticsearch info: %s", err)
    }
    defer res.Body.Close()
	return es, nil
}

func BulkSendToElastic(indexName string, documents []map[string]any, esClient *elasticsearch.Client, logger *slog.Logger) error{
    if len(documents) == 0 {
        return fmt.Errorf("no documents to index")
    }
    var buf bytes.Buffer
    for _, doc := range documents {
        // Prepare the metadata for the bulk operation (index action)
        meta := map[string]any{
            "index": map[string]any{
                "_index": indexName,
            },
        }
        // If your document has an "id" field, you can use it for the document ID in Elasticsearch
        if docID, ok := doc["id"]; ok {
            meta["index"].(map[string]any)["_id"] = fmt.Sprintf("%v", docID)
        }

        metaBytes, err := json.Marshal(meta)
        if err != nil {
            logger.Debug(fmt.Sprintf("Error marshaling bulk metadata: %s", err))
            continue // Or handle error more robustly
        }
        buf.Write(metaBytes)
        buf.WriteByte('\n')

        // Prepare the document source
        docBytes, err := json.Marshal(doc)
        if err != nil {
            logger.Debug(fmt.Sprintf("Error marshaling document: %s for doc: %+v", err, doc))
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

    ctx := context.Background()

    // Perform the bulk request
	res, err := req.Do(ctx, esClient)
    if err != nil {
        return fmt.Errorf("error performing bulk request: %w", err)
    }
    defer res.Body.Close()

    if res.IsError() {
        var raw map[string]any
        if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
            return fmt.Errorf("failure to parse response body: %s", err)
        }
        return fmt.Errorf("bulk request failed: [%s] %s: %s",
            res.Status(),
            raw["error"].(map[string]any)["type"],
            raw["error"].(map[string]any)["reason"],
            )
    }

    // You can optionally parse the response to check for individual item errors
    // For brevity, this example doesn't parse the full bulk response items.
	logger.Debug("Bulk indexing successful or partially successful. Response Status:" + res.Status())
    // To see the full response, you can read res.Body
    // responseBody, _ := io.ReadAll(res.Body)
    // log.Printf("Full response: %s", string(responseBody))

    return nil
}

func BulkUpdateToElastic(indexName string, documents []map[string]any, esClient *elasticsearch.Client, logger *slog.Logger) error{
    if len(documents) == 0 {
        return fmt.Errorf("no documents to index")
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

        meta := map[string]any{
            "update": map[string]any{
                "_index": indexName,
                "_id":    id,
            },
        }
        metaBytes, err := json.Marshal(meta)
        if err != nil {
            logger.Error(fmt.Sprintf("Error marshaling meta1: %s", err))
			panic(err)
        }
        bulkBody.Write(metaBytes)
        bulkBody.WriteString("\n")

        docBytes, err := json.Marshal(map[string]any{
            "doc": doc,
        })
        if err != nil {
            logger.Error(fmt.Sprintf("Error marshaling docUpdate1: %s", err))
			panic(err)
        }
        bulkBody.Write(docBytes)
        bulkBody.WriteString("\n")
		if err != nil {
			logger.Error(fmt.Sprintf("Unexpected error adding item '%s' to BulkIndexer: %s", id, err))
			panic(err)
		}
    }

    res, err := esClient.Bulk(
		bytes.NewReader([]byte(bulkBody.String())),
		esClient.Bulk.WithContext(context.Background()),
	)
	logger.Debug(bulkBody.String())

    defer res.Body.Close()

    if res.IsError() {
		var e map[string]any
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			logger.Error(fmt.Sprintf("Error parsing the response body: %s", err))
		}
		logger.Error(fmt.Sprintf("Elasticsearch returned an error [%s]: %s", res.Status(), e["error"]))
		panic(err)
	}

    var bulkRes map[string]any
	if err := json.NewDecoder(res.Body).Decode(&bulkRes); err != nil {
		logger.Error("Error parsing the bulk response: %s", err)
		panic(err)
	}

	logger.Debug("\n--- Bulk Operation Response ---")
	if errors, ok := bulkRes["errors"].(bool); ok && errors {
		logger.Debug("Warning: Some items failed in the bulk request!")
		if items, ok := bulkRes["items"].([]any); ok {
			for i, item := range items {
				itemMap := item.(map[string]any)
				if updateStatus, ok := itemMap["update"].(map[string]any); ok {
					if status, ok := updateStatus["status"].(float64); ok && status >= 400 {
						failedUpdates++
						errorInfo := updateStatus["error"].(map[string]any)
						logger.Debug(fmt.Sprintf("  Item %d (ID: %s) FAILED: %s - %s\n", i+1, updateStatus["_id"], errorInfo["type"], errorInfo["reason"]))
					} else {
						successfulUpdates++
						logger.Debug(fmt.Sprintf("  Item %d (ID: %s) SUCCESS: Version %d\n", i+1, updateStatus["_id"], int(updateStatus["_version"].(float64))))
					}
				}
			}
		}
	} else {
		logger.Debug("All items in the bulk request were successful!")
		if items, ok := bulkRes["items"].([]any); ok {
			for i, item := range items {
				itemMap := item.(map[string]any)
				if updateStatus, ok := itemMap["update"].(map[string]any); ok {
					successfulUpdates++
					logger.Debug(fmt.Sprintf("  Item %d (ID: %s) SUCCESS: Version %d\n", i+1, updateStatus["_id"], int(updateStatus["_version"].(float64))))
				}
			}
		}
	}

	logger.Debug("\n--- Bulk Update Summary ---\n")
	logger.Debug(fmt.Sprintf("Total documents attempted: %d\n", len(documents)))
	logger.Debug(fmt.Sprintf("Successful updates: %d\n", successfulUpdates))
	logger.Debug(fmt.Sprintf("Failed updates: %d\n", failedUpdates))

	if failedUpdates == 0 {
		logger.Debug("All documents updated successfully!")
	} else {
		logger.Debug("Some documents failed to update. Check logs above.")
	}
    return nil
}


func BulkDeleteFromElastic(indexName string, documents []map[string]any, esClient *elasticsearch.Client, logger *slog.Logger) error {

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
            logger.Error(fmt.Sprintf("Unexpected type for ID: %T\n", v))
			panic(nil)
            
        }
        meta := map[string]any{
            "delete": map[string]any{
                "_index": indexName,
                "_id":    id,
            },
        }
        metaBytes, err := json.Marshal(meta)
        if err != nil {
            logger.Error(fmt.Sprintf("Error marshaling meta1: %s", err))
			panic(err)
        }
        bulkBody.Write(metaBytes)
        bulkBody.WriteString("\n")
    }

	// --- Send the bulk request ---
	logger.Debug("Sending bulk delete request for documents ...\n")

	// Refresh the index immediately after the bulk operation for searchability (optional, for testing)
	refresh := "wait_for" // or "true" for immediate refresh, or "" for default

	res, err := esClient.Bulk(
		bytes.NewReader([]byte(bulkBody.String())),
		esClient.Bulk.WithContext(context.Background()),
		esClient.Bulk.WithRefresh(refresh), // Apply refresh setting
	)
	if err != nil {
		logger.Error(fmt.Sprintf("FATAL ERROR: %s", err))
		panic(err)
	}
	defer res.Body.Close()

	// --- Process the response synchronously ---
	if res.IsError() {
		var e map[string]any
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			logger.Error(fmt.Sprintf("Error parsing the response body: %s", err))
			panic(err)
		}
		logger.Error(fmt.Sprintf("Elasticsearch returned an error [%s]: %s", res.Status(), e["error"]))
		panic(err)
	}

	var bulkRes map[string]any
	if err := json.NewDecoder(res.Body).Decode(&bulkRes); err != nil {
		logger.Error(fmt.Sprintf("Error parsing the bulk response: %s", err))
		panic(err)
	}

	logger.Debug("\n--- Bulk Delete Operation Response ---")
	if errors, ok := bulkRes["errors"].(bool); ok && errors {
		logger.Debug("Warning: Some items failed in the bulk request!")
		if items, ok := bulkRes["items"].([]any); ok {
			for i, item := range items {
				itemMap := item.(map[string]any)
				if deleteStatus, ok := itemMap["delete"].(map[string]any); ok {
					if status, ok := deleteStatus["status"].(float64); ok && status >= 400 {
						failedDeletes++
						errorInfo := deleteStatus["error"].(map[string]any)
						logger.Debug(fmt.Sprintf("  Item %d (ID: %s) FAILED: %s - %s\n", i+1, deleteStatus["_id"], errorInfo["type"], errorInfo["reason"]))
					} else {
						successfulDeletes++
						logger.Debug(fmt.Sprintf("  Item %d (ID: %s) SUCCESS: Version %d\n", i+1, deleteStatus["_id"], int(deleteStatus["_version"].(float64))))
					}
				}
			}
		}
	} else {
		logger.Debug("All items in the bulk request were successful!")
		if items, ok := bulkRes["items"].([]any); ok {
			for i, item := range items {
				itemMap := item.(map[string]any)
				if deleteStatus, ok := itemMap["delete"].(map[string]any); ok {
					successfulDeletes++
					logger.Debug(fmt.Sprintf("  Item %d (ID: %s) SUCCESS: Version %d\n", i+1, deleteStatus["_id"], int(deleteStatus["_version"].(float64))))
				}
			}
		}
	}

	logger.Debug(fmt.Sprintf("\n--- Bulk Delete Summary ---\n"))
	logger.Debug(fmt.Sprintf("Total documents attempted to delete: %d\n", len(documents)))
	logger.Debug(fmt.Sprintf("Successful deletes: %d\n", successfulDeletes))
	logger.Debug(fmt.Sprintf("Failed deletes: %d\n", failedDeletes))

	if failedDeletes == 0 {
		logger.Debug("All documents deleted successfully!")
	} else {
		logger.Debug("Some documents failed to delete. Check logs above.")
	}
    return nil
}
