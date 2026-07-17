package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/k1-end/mysql-2-elastic/internal/binlog"
	"github.com/k1-end/mysql-2-elastic/internal/config"
	"github.com/k1-end/mysql-2-elastic/internal/es"
	"github.com/k1-end/mysql-2-elastic/internal/logger"
	"github.com/k1-end/mysql-2-elastic/internal/pipeline"
	"github.com/k1-end/mysql-2-elastic/internal/storage/filesystem"
	"github.com/k1-end/mysql-2-elastic/internal/util"
)

func main() {
	log := logger.NewLogger(slog.LevelInfo)

	cfg, err := config.LoadConfig()
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	esClient, err := es.NewClient(cfg)
	if err != nil {
		log.Error("connect to elasticsearch", "err", err)
		os.Exit(1)
	}

	syncer, err := binlog.NewSyncer(cfg, log)
	if err != nil {
		log.Error("create syncer", "err", err)
		os.Exit(1)
	}

	if err := binlog.Initialize(); err != nil {
		log.Error("initialize binlog", "err", err)
		os.Exit(1)
	}

	if err := util.CreateDirectoryIfNotExists("data/dumps"); err != nil {
		log.Error("create data/dumps directory", "err", err)
		os.Exit(1)
	}

	store, err := filesystem.NewFileStorage(cfg.Database.Tables)
	if err != nil {
		log.Error("initialize storage", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := pipeline.InitializeTables(ctx, cfg, esClient, syncer, store, log); err != nil {
		log.Error("initialize tables", "err", err)
		os.Exit(1)
	}

	pipeline.Run(ctx, cfg, esClient, syncer, store, log)
}
