package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	tablepack "github.com/k1-end/mysql-2-elastic/internal/table"
	"github.com/k1-end/mysql-2-elastic/internal/util"
)

func rootHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Hello, World!")
    // RestartChannel<- true
}

func pushNewTable(tableName string) error {
	if !util.IsServerFree() {
		return fmt.Errorf("Server is busy right now. Try again later.")
	}

	//TODO: write a method in table package for this
	// registeredTables := tablepack.GetRegisteredTables()
	// registeredTables[tableName] = tablepack.RegisteredTable{
	// 	Name:   tableName,
	// 	Status: "created",
	// }
	//
	// jsonData, _ := json.Marshal(registeredTables)
	// os.WriteFile(registeredTablesFilePath, jsonData, 0644)
	util.SetServerStatus("busy")
	return nil
}

func addTable(w http.ResponseWriter, r *http.Request) {

	hasTableName := r.URL.Query().Has("table_name")
	if !hasTableName {
		http.Error(w, "table_name is required", http.StatusUnprocessableEntity)
		return
	}

	tableName := r.URL.Query().Get("table_name")
	if tablepack.TableExists(tableName) {
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
	hasTableName := r.URL.Query().Has("table_name")
	if !hasTableName {
		http.Error(w, "table_name is required", http.StatusUnprocessableEntity)
		return
	}

	tableName := r.URL.Query().Get("table_name")
	if !tablepack.TableExists(tableName) {
		http.Error(w, "Table does not exists", http.StatusConflict)
		return
	}

	// go InitialDump(tableName, ap)
	io.WriteString(w, "Table dump started successfully")
	w.WriteHeader(http.StatusOK)
	return
}

func getTable(w http.ResponseWriter, r *http.Request) {
	hasTableName := r.URL.Query().Has("table_name")
	if !hasTableName {
		http.Error(w, "table_name is required", http.StatusUnprocessableEntity)
		return
	}
	tableName := r.URL.Query().Get("table_name")
	if !tablepack.TableExists(tableName) {
		http.Error(w, "Table does not exists", http.StatusConflict)
		return
	}
	// return table info
	registeredTables := tablepack.GetRegisteredTables()
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
	registeredTables := tablepack.GetRegisteredTables()
	jsonData, _ := json.Marshal(registeredTables)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, string(jsonData))
	return
}
