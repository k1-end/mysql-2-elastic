package syncer

// BinlogPosition represents the structure of your JSON data
type BinlogPosition struct {
	Logfile string `json:"logfile"`
	Logpos  uint32  `json:"logpos"`
}

func GetMainBinlogPositionFilePath() (string) {
   return "data/main-binlog-position.json" 
}

func GetTableBinlogPositionFilePath(tableName string) (string) {
   return "data/dumps/" + tableName + "/" + tableName + "-dump-binlog-position.json" 
}
