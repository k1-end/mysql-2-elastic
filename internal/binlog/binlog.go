package binlog

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/k1-end/mysql-2-elastic/internal/config"
	"github.com/k1-end/mysql-2-elastic/internal/table"
)

const binlogPositionFilePath = "data/main-binlog-position.json"

// BinlogCompare indicates which of two binlog positions is newer.
type BinlogCompare int

const (
	Pos1Newer BinlogCompare = iota
	Pos2Newer
	Equal
)

func parseBinlogFile(filePath string) (table.BinlogPosition, error) {
	data, err := os.ReadFile(filePath)
	if err != nil && !os.IsNotExist(err) {
		return table.BinlogPosition{}, fmt.Errorf("error reading binlog file %s: %w", filePath, err)
	}

	if os.IsNotExist(err) || len(strings.TrimSpace(string(data))) == 0 {
		return table.BinlogPosition{}, fmt.Errorf("binlog file is empty or does not exist: %s", filePath)
	}

	var pos table.BinlogPosition
	if err := json.Unmarshal(data, &pos); err != nil {
		return table.BinlogPosition{}, fmt.Errorf("error unmarshalling binlog JSON from %s: %w", filePath, err)
	}
	return pos, nil
}

// GetStoredBinlogPosition reads the main binlog position from disk.
func GetStoredBinlogPosition() (table.BinlogPosition, error) {
	return parseBinlogFile(binlogPositionFilePath)
}

// WriteBinlogPosition persists the binlog position to disk.
func WriteBinlogPosition(pos table.BinlogPosition) error {
	data, err := json.Marshal(pos)
	if err != nil {
		return fmt.Errorf("failed to marshal binlog position: %w", err)
	}
	if err := os.WriteFile(binlogPositionFilePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write binlog position: %w", err)
	}
	return nil
}

// CompareBinlogPositions returns which position is newer, or Equal.
func CompareBinlogPositions(a, b table.BinlogPosition) BinlogCompare {
	if a.Logfile < b.Logfile {
		return Pos2Newer
	}
	if a.Logfile > b.Logfile {
		return Pos1Newer
	}
	if a.Logpos < b.Logpos {
		return Pos2Newer
	}
	if a.Logpos > b.Logpos {
		return Pos1Newer
	}
	return Equal
}

// Initialize creates a default binlog position file if one does not exist.
func Initialize() error {
	data, err := os.ReadFile(binlogPositionFilePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error reading binlog file %s: %w", binlogPositionFilePath, err)
	}

	if os.IsNotExist(err) || len(strings.TrimSpace(string(data))) == 0 {
		return WriteBinlogPosition(table.BinlogPosition{Logfile: "binlog.000000", Logpos: 0})
	}
	return nil
}

// NewSyncer creates a binlog syncer connected to MySQL.
func NewSyncer(cfg *config.Config, logger *slog.Logger) (*replication.BinlogSyncer, error) {
	syncerCfg := replication.BinlogSyncerConfig{
		ServerID: uint32(cfg.Database.ServerId),
		Flavor:   cfg.Database.Driver,
		Host:     cfg.Database.Host,
		Port:     uint16(cfg.Database.Port),
		User:     cfg.Database.Username,
		Password: cfg.Database.Password,
		Logger:   logger,
	}
	return replication.NewBinlogSyncer(syncerCfg), nil
}
