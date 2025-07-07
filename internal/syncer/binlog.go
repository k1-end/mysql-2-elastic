package syncer

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"log/slog"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/k1-end/mysql-elastic-go/internal/config"
)

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

func ParseBinlogCoordinatesFile(filePath string) (BinlogPosition, error) {
	data, err := os.ReadFile(filePath)

	// Check for read errors (excluding file not exists for emptiness check)
	if err != nil && !os.IsNotExist(err) {
        return BinlogPosition{}, fmt.Errorf("error reading file %s: %w", filePath, err)
	}

	empty := len(strings.TrimSpace(string(data))) == 0 || os.IsNotExist(err)

	if empty{
		return BinlogPosition{}, fmt.Errorf("file is empty or does not exist")
	}

	var pos BinlogPosition
	if err := json.Unmarshal(data, &pos); err != nil {
		return BinlogPosition{}, fmt.Errorf("error unmarshalling JSON from %s: %w", filePath, err)
	}
    return pos, nil
}

func GetStoredBinlogCoordinates(tableName string) (BinlogPosition, error) {
    var filePath string
    if tableName == "main" {
        filePath = GetMainBinlogPositionFilePath()
    }else{
        filePath = GetTableBinlogPositionFilePath(tableName)
    }
    return ParseBinlogCoordinatesFile(filePath)
}

func WriteBinlogPosition(binlogPos BinlogPosition, tableName string) error {
    jsonData, err := json.Marshal(map[string]any{
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
        filePath = GetTableBinlogPositionFilePath(tableName)
    }
    err = os.WriteFile(filePath, jsonData, 0644)
    if err != nil {
        return fmt.Errorf("failed to write binlog coordinates to file: %w", err)
    }

    return nil
}

// If both args are equal, nil will be returned
func GetNewerBinlogPosition(pos1, pos2 *BinlogPosition) *BinlogPosition {
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
            return nil // They are equal
        }
    }
}

func GetDatabaseSyncer(appConfig *config.Config, logger *slog.Logger) (*replication.BinlogSyncer, error) {

    cfg := replication.BinlogSyncerConfig {
        ServerID: uint32(appConfig.Database.ServerId),
        Flavor  : appConfig.Database.Driver,
        Host    : appConfig.Database.Host,
        Port    : uint16(appConfig.Database.Port),
        User    : appConfig.Database.Username,
        Password: appConfig.Database.Password,
		Logger  : logger,
    }

    syncer := replication.NewBinlogSyncer(cfg)
	return syncer, nil
}

