package database

import (
	"log/slog"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/k1-end/mysql-elastic-go/internal/config"
)

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

