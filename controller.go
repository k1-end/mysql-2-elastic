package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

func pushNewTable(tableName string) error {
	if !isServerFree() {
		return fmt.Errorf("Server is busy right now. Try again later.")
	}

	registeredTables := GetRegisteredTables()
	registeredTables[tableName] = RegisteredTable{
		Name:   tableName,
		Status: "created",
	}

	jsonData, _ := json.Marshal(registeredTables)
	os.WriteFile(registeredTablesFilePath, jsonData, 0644)
	setServerStatus("busy")
	return nil
}

func addTable(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("got /add-table request\n")

	hasTableName := r.URL.Query().Has("table_name")
	if !hasTableName {
		http.Error(w, "table_name is required", http.StatusUnprocessableEntity)
		return
	}

	tableName := r.URL.Query().Get("table_name")
	if tableExists(tableName) {
		http.Error(w, "Table already exists", http.StatusConflict)
		return
	}

	err := pushNewTable(tableName)
	if err != nil {
		http.Error(w, "There was an error pushing the new table: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	io.WriteString(w, "Table added successfully")
	w.WriteHeader(http.StatusOK)
	return
}

func dumpTable(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("got /dump-table request\n")

	hasTableName := r.URL.Query().Has("table_name")
	if !hasTableName {
		http.Error(w, "table_name is required", http.StatusUnprocessableEntity)
		return
	}

	tableName := r.URL.Query().Get("table_name")
	if !tableExists(tableName) {
		http.Error(w, "Table does not exists", http.StatusConflict)
		return
	}

	go InitialDump(tableName)
	io.WriteString(w, "Table dump started successfully")
	w.WriteHeader(http.StatusOK)
	return
}

func getTable(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("got /get-table request\n")
	hasTableName := r.URL.Query().Has("table_name")
	if !hasTableName {
		http.Error(w, "table_name is required", http.StatusUnprocessableEntity)
		return
	}
	tableName := r.URL.Query().Get("table_name")
	if !tableExists(tableName) {
		http.Error(w, "Table does not exists", http.StatusConflict)
		return
	}
	// return table info
	registeredTables := GetRegisteredTables()
	table, exists := registeredTables[tableName]
	if !exists {
		http.Error(w, "Table does not exists", http.StatusConflict)
		return
	}
	jsonData, _ := json.Marshal(table)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonData)
	return
}

func getAllTable(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("got /get-all-table request\n")
	registeredTables := GetRegisteredTables()
	jsonData, _ := json.Marshal(registeredTables)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, string(jsonData))
	return
}

//    http.HandleFunc("/add-table", addTable)
//    http.HandleFunc("/dump-table", dumpTable)
//    http.HandleFunc("/get-table", getTable)
//    http.HandleFunc("/get-all-table", getAllTable)
//
// _ = http.ListenAndServe(":3333", nil)
