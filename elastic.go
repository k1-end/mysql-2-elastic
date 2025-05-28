package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func bulkSendToElastic(documents []map[string]interface{}) error{
    indexName := "orders"
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
        // if docID, ok := doc["id"]; ok {
        //     meta["index"].(map[string]interface{})["_id"] = fmt.Sprintf("%v", docID)
        // }

        metaBytes, err := json.Marshal(meta)
        if err != nil {
            log.Printf("Error marshaling bulk metadata: %s", err)
            continue // Or handle error more robustly
        }
        buf.Write(metaBytes)
        buf.WriteByte('\n')

        // Prepare the document source
        docBytes, err := json.Marshal(doc)
        if err != nil {
            log.Printf("Error marshaling document: %s for doc: %+v", err, doc)
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
            "http://localhost:9200", // Default address, change if needed
        },
        Username: "elastic", // If you have security enabled
        Password: "elastic",
        // For Cloud ID:
        // CloudID: "<YOUR_CLOUD_ID>",
        // APIKey:  "<YOUR_API_KEY>",
    }
    es, err := elasticsearch.NewClient(cfg)
    if err != nil {
        log.Fatalf("Error creating the Elasticsearch client: %s", err)
    }

    // Ping the Elasticsearch server to verify connection (optional)
    res, err := es.Info()
    if err != nil {
        log.Fatalf("Error getting Elasticsearch info: %s", err)
    }
    defer res.Body.Close()
    log.Println("Elasticsearch Info:", res.Status())
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
    log.Println("Bulk indexing successful or partially successful. Response Status:", res.Status())
    // To see the full response, you can read res.Body
    // responseBody, _ := io.ReadAll(res.Body)
    // log.Printf("Full response: %s", string(responseBody))

    return nil
}
