package table


const (
    registeredTablesFilePath = "data/registered-tables.json"
)

type RegisteredTable struct {
    Name string `json:"name"`
    Status string `json:"status"`
	Columns *[]ColumnInfo `json:"columns"`
}

type ColumnInfo struct {
	Name string `json:"name"`
    Type string `json:"type"`
    Position int `json:"positions"`
}
