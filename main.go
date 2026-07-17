package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/k1-end/mysql-2-elastic/internal/binlog"
	"github.com/k1-end/mysql-2-elastic/internal/config"
	"github.com/k1-end/mysql-2-elastic/internal/handler"
	"github.com/k1-end/mysql-2-elastic/internal/handler/elasticsearch"
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

	registry := handler.NewRegistry()

	for name, handlerCfg := range cfg.Handlers {
		handlerMap, ok := handlerCfg.(map[string]any)
		if !ok {
			log.Error("invalid handler config", "handler", name)
			continue
		}
		switch name {
		case "elasticsearch":
			h, err := elasticsearch.New(handlerMap, log)
			if err != nil {
				log.Error("failed to initialize elasticsearch handler", "err", err)
				os.Exit(1)
			}
			registry.Register(h)
		default:
			log.Warn("unknown handler in config, skipping", "handler", name)
		}
	}

	var activeHandlers []string
	for name := range cfg.Handlers {
		activeHandlers = append(activeHandlers, name)
	}
	registry.Activate(activeHandlers)

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

	if err := pipeline.InitializeTables(ctx, cfg, registry, syncer, store, log); err != nil {
		log.Error("initialize tables", "err", err)
		os.Exit(1)
	}

	pipeline.Run(ctx, cfg, registry, syncer, store, log)
	registry.CloseAll(log)
}
