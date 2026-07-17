package pipeline

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/k1-end/mysql-2-elastic/internal/binlog"
	"github.com/k1-end/mysql-2-elastic/internal/config"
	"github.com/k1-end/mysql-2-elastic/internal/dump"
	"github.com/k1-end/mysql-2-elastic/internal/handler"
	"github.com/k1-end/mysql-2-elastic/internal/storage"
	"github.com/k1-end/mysql-2-elastic/internal/table"
)

// InitializeTables walks each registered table through the sync lifecycle state machine
// until it reaches the Syncing status.
func InitializeTables(
	ctx context.Context,
	cfg *config.Config,
	registry *handler.Registry,
	syncer *replication.BinlogSyncer,
	store storage.TableStorage,
	log *slog.Logger,
) error {
	tables, err := store.GetRegisteredTables()
	if err != nil {
		return fmt.Errorf("failed to get registered tables: %w", err)
	}
	if len(tables) == 0 {
		return errors.New("no registered tables")
	}

	log.Info("processing tables")

	for _, t := range tables {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := initializeTable(ctx, cfg, t, registry, syncer, store, log); err != nil {
			return fmt.Errorf("table %q: %w", t.Name, err)
		}
	}
	log.Info("finished processing tables")
	return nil
}

func initializeTable(
	ctx context.Context,
	cfg *config.Config,
	t table.RegisteredTable,
	registry *handler.Registry,
	syncer *replication.BinlogSyncer,
	store storage.TableStorage,
	log *slog.Logger,
) error {
	dumpPath, err := store.GetDumpFilePath(t.Name)
	if err != nil {
		return err
	}

	// Phase 1: Create/dumping → dump
	if t.Status == table.Created || t.Status == table.Dumping {
		if err := dump.ClearIncompleteDump(dumpPath); err != nil {
			return fmt.Errorf("clear incomplete dump: %w", err)
		}
		if err := store.SetTableStatus(t.Name, table.Created); err != nil {
			return err
		}
		if err := store.SetTableStatus(t.Name, table.Dumping); err != nil {
			return err
		}
		if err := dump.InitialDump(t.Name, cfg, log, store); err != nil {
			return fmt.Errorf("initial dump: %w", err)
		}
		if err := store.SetTableStatus(t.Name, table.Dumped); err != nil {
			return err
		}
		t.Status, err = store.GetTableStatus(t.Name)
		if err != nil {
			return err
		}
	}

	// Phase 2: Dumped → initialized handlers
	if t.Status == table.Dumped {
		colsInfo, err := dump.GetTableColsInfoFromDumpFile(dumpPath)
		if err != nil {
			return fmt.Errorf("parse columns from dump: %w", err)
		}
		if err := store.SetTableColsInfo(t.Name, colsInfo); err != nil {
			return err
		}
		t.Columns = &colsInfo

		for _, h := range registry.GetActive() {
			if err := h.OnTableInit(ctx, t.Name, colsInfo); err != nil {
				log.Error("handler table init failed", "handler", h.Name(), "table", t.Name, "error", err)
			}
		}
		if err := store.SetTableStatus(t.Name, table.Initialized); err != nil {
			return err
		}
		t.Status, err = store.GetTableStatus(t.Name)
		if err != nil {
			return err
		}
	}

	// Phase 3: Initialized → moving → moved (send dump data to handlers)
	if t.Status == table.Initialized || t.Status == table.Moving {
		if err := store.SetTableStatus(t.Name, table.Moving); err != nil {
			return err
		}
		if t.DumpReadProgress == nil {
			if err := store.SetDumpReadProgress(t.Name, 0); err != nil {
				return err
			}
		}
		t, err = store.GetTable(t.Name)
		if err != nil {
			return err
		}
		if t.Status != table.Moving {
			return fmt.Errorf("table status is not moving after set")
		}
		if err := SendDumpToHandlers(t, registry, store); err != nil {
			return fmt.Errorf("send dump to handlers: %w", err)
		}
		if err := store.SetTableStatus(t.Name, table.Moved); err != nil {
			return err
		}
		t.Status, err = store.GetTableStatus(t.Name)
		if err != nil {
			return err
		}
	}

	// Phase 4: Moved → syncing (catch up binlog)
	if t.Status == table.Moved {
		log.Debug("syncing with main loop", "table", t.Name)

		dumpBinlogPos, err := dump.GetBinlogCoordinates(dumpPath)
		if err != nil {
			return fmt.Errorf("parse binlog coordinates from dump: %w", err)
		}
		if err := store.SetTableBinlogPos(t.Name, dumpBinlogPos); err != nil {
			return err
		}
		t, err = store.GetTable(t.Name)
		if err != nil {
			return err
		}

		mainPos, err := binlog.GetStoredBinlogPosition()
		if err != nil {
			return fmt.Errorf("get stored binlog position: %w", err)
		}

		if mainPos.Logfile == "binlog.000000" && mainPos.Logpos == 0 {
			if err := binlog.WriteBinlogPosition(*t.BinlogPos); err != nil {
				return err
			}
		}
		if t.BinlogPos == nil {
			return fmt.Errorf("nil binlog position for table %q", t.Name)
		}

		switch binlog.CompareBinlogPositions(mainPos, *t.BinlogPos) {
		case binlog.Pos1Newer:
			log.Debug("main binlog is newer, catching up dump file to main binlog", "table", t.Name)
			if err := SyncCatchup([]string{t.Name}, mainPos, *t.BinlogPos, registry, syncer, store, log); err != nil {
				return fmt.Errorf("sync catchup: %w", err)
			}
		case binlog.Pos2Newer:
			log.Debug("dump binlog is newer, catching up main binlog to dump file", "table", t.Name)
			tables, err := store.GetRegisteredTables()
			if err != nil {
				return err
			}
			var syncingNames []string
			for name, tbl := range tables {
				if tbl.Status == table.Syncing {
					syncingNames = append(syncingNames, name)
				}
			}
			if err := SyncCatchup(syncingNames, *t.BinlogPos, mainPos, registry, syncer, store, log); err != nil {
				return fmt.Errorf("sync catchup: %w", err)
			}
		}

		log.Debug("sync completed", "table", t.Name)
		if err := store.SetTableStatus(t.Name, table.Syncing); err != nil {
			return err
		}
	}

	return nil
}

// Run enters the real-time binlog sync loop. It blocks until ctx is cancelled.
func Run(
	ctx context.Context,
	cfg *config.Config,
	registry *handler.Registry,
	syncer *replication.BinlogSyncer,
	store storage.TableStorage,
	log *slog.Logger,
) {
	tables, err := store.GetRegisteredTables()
	if err != nil {
		log.Error("failed to get registered tables", "err", err)
		return
	}
	var tableNames []string
	for name, t := range tables {
		if t.Status == table.Syncing {
			tableNames = append(tableNames, name)
		}
	}
	if len(tableNames) == 0 {
		log.Warn("no tables in syncing status")
		return
	}
	log.Debug("syncing tables", "tables", strings.Join(tableNames, ", "))

	currentPos, err := binlog.GetStoredBinlogPosition()
	if err != nil {
		log.Error("failed to get stored binlog position", "err", err)
		return
	}

	streamer, err := syncer.StartSync(mysql.Position{
		Name: currentPos.Logfile,
		Pos:  currentPos.Logpos,
	})
	if err != nil {
		log.Error("failed to start sync", "err", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			log.Info("shutdown signal received, stopping syncer")
			syncer.Close()
			return
		default:
			ev, err := streamer.GetEvent(ctx)
			if err != nil {
				if ctx.Err() != nil {
					syncer.Close()
					return
				}
				log.Error("failed to get binlog event", "err", err)
				return
			}
			if ev == nil {
				continue
			}
			if err := ProcessBinlogEvent(ev, &currentPos, tableNames, registry, store, log); err != nil {
				log.Error("error processing binlog event", "err", err)
			}
		}
	}
}

// ProcessBinlogEvent handles a single binlog event.
func ProcessBinlogEvent(
	ev *replication.BinlogEvent,
	pos *table.BinlogPosition,
	tableNames []string,
	registry *handler.Registry,
	store storage.TableStorage,
	log *slog.Logger,
) error {
	pos.Logpos = uint32(ev.Header.LogPos)

	switch e := ev.Event.(type) {
	case *replication.RotateEvent:
		pos.Logfile = string(e.NextLogName)
		log.Debug("binlog rotated", "file", pos.Logfile, "pos", pos.Logpos)

	case *replication.RowsEvent:
		tableName := string(e.Table.Table)
		if !slices.Contains(tableNames, tableName) {
			return binlog.WriteBinlogPosition(*pos)
		}
		schema := string(e.Table.Schema)
		log.Debug("row event", "schema", schema, "table", tableName)

		switch ev.Header.EventType {
		case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			if err := handleInsert(tableName, e.Rows, registry, store, log); err != nil {
				return err
			}
		case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			if err := handleUpdate(tableName, e.Rows, registry, store, log); err != nil {
				return err
			}
		case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			if err := handleDelete(tableName, e.Rows, registry, store, log); err != nil {
				return err
			}
		}
	}
	return binlog.WriteBinlogPosition(*pos)
}

func handleInsert(tableName string, rows [][]any, registry *handler.Registry, store storage.TableStorage, log *slog.Logger) error {
	t, err := store.GetTable(tableName)
	if err != nil {
		return err
	}
	dbRecords, err := convertRows(rows, *t.Columns)
	if err != nil {
		return fmt.Errorf("convert insert rows: %w", err)
	}
	records := toHandlerRecords(dbRecords)
	event := handler.Event{
		Operation:  handler.OpInsert,
		Table:      tableName,
		Records:    records,
		ColumnInfo: *t.Columns,
	}
	for _, h := range registry.GetActive() {
		if err := h.HandleEvent(context.Background(), event); err != nil {
			log.Error("handler failed", "handler", h.Name(), "table", tableName, "operation", "insert", "error", err)
		}
	}
	return nil
}

func handleUpdate(tableName string, rows [][]any, registry *handler.Registry, store storage.TableStorage, log *slog.Logger) error {
	var afterImages [][]any
	for i := 0; i+1 < len(rows); i += 2 {
		afterImages = append(afterImages, rows[i+1])
	}
	t, err := store.GetTable(tableName)
	if err != nil {
		return err
	}
	dbRecords, err := convertRows(afterImages, *t.Columns)
	if err != nil {
		return fmt.Errorf("convert update rows: %w", err)
	}
	records := toHandlerRecords(dbRecords)
	event := handler.Event{
		Operation:  handler.OpUpdate,
		Table:      tableName,
		Records:    records,
		ColumnInfo: *t.Columns,
	}
	for _, h := range registry.GetActive() {
		if err := h.HandleEvent(context.Background(), event); err != nil {
			log.Error("handler failed", "handler", h.Name(), "table", tableName, "operation", "update", "error", err)
		}
	}
	return nil
}

func handleDelete(tableName string, rows [][]any, registry *handler.Registry, store storage.TableStorage, log *slog.Logger) error {
	t, err := store.GetTable(tableName)
	if err != nil {
		return err
	}
	dbRecords, err := convertRows(rows, *t.Columns)
	if err != nil {
		return fmt.Errorf("convert delete rows: %w", err)
	}
	records := toHandlerRecords(dbRecords)
	event := handler.Event{
		Operation:  handler.OpDelete,
		Table:      tableName,
		Records:    records,
		ColumnInfo: *t.Columns,
	}
	for _, h := range registry.GetActive() {
		if err := h.HandleEvent(context.Background(), event); err != nil {
			log.Error("handler failed", "handler", h.Name(), "table", tableName, "operation", "delete", "error", err)
		}
	}
	return nil
}

func convertRows(rows [][]any, cols []table.ColumnInfo) ([]table.DbRecord, error) {
	colMap := table.ColumnMap(cols)
	var records []table.DbRecord
	for _, row := range rows {
		rec := table.DbRecord{ColValues: make(map[string]any)}
		for j, val := range row {
			col, ok := colMap[j]
			if !ok {
				return nil, fmt.Errorf("no column at position %d", j)
			}
			rec.ColValues[col.Name] = val
			if col.IsInPrimaryKey {
				rec.PrimaryKey += fmt.Sprintf("%v", val)
			}
		}
		records = append(records, rec)
	}
	return records, nil
}

func toHandlerRecords(dbRecords []table.DbRecord) []handler.Record {
	records := make([]handler.Record, len(dbRecords))
	for i, r := range dbRecords {
		records[i] = handler.Record{
			PrimaryKey: r.PrimaryKey,
			Columns:    r.ColValues,
		}
	}
	return records
}

// SyncCatchup reads binlog events from currentPos until the destination is reached,
// sending each relevant event to all active handlers.
func SyncCatchup(
	tableNames []string,
	currentPos, desPos table.BinlogPosition,
	registry *handler.Registry,
	mainSyncer *replication.BinlogSyncer,
	store storage.TableStorage,
	log *slog.Logger,
) error {
	log.Debug("catching up", "tables", strings.Join(tableNames, ", "))

	streamer, err := mainSyncer.StartSync(mysql.Position{
		Name: currentPos.Logfile,
		Pos:  currentPos.Logpos,
	})
	if err != nil {
		return fmt.Errorf("failed to start catchup sync: %w", err)
	}

	for binlog.CompareBinlogPositions(currentPos, desPos) == binlog.Pos2Newer {
		ev, err := streamer.GetEvent(context.Background())
		if err != nil {
			return fmt.Errorf("failed to get event during catchup: %w", err)
		}
		if ev == nil {
			return fmt.Errorf("nil event during catchup")
		}
		if err := ProcessBinlogEvent(ev, &currentPos, tableNames, registry, store, log); err != nil {
			return fmt.Errorf("error processing event during catchup: %w", err)
		}
	}
	return nil
}

// SendDumpToHandlers reads a SQL dump file line-by-line, parsing INSERT statements
// and bulk-sending them to all active handlers.
func SendDumpToHandlers(t table.RegisteredTable, registry *handler.Registry, store storage.TableStorage) error {
	dumpPath, err := store.GetDumpFilePath(t.Name)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(dumpPath, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open dump file: %w", err)
	}
	defer f.Close()

	if t.DumpReadProgress == nil {
		return fmt.Errorf("DumpReadProgress is nil for table %q", t.Name)
	}
	if _, err := f.Seek(int64(*t.DumpReadProgress), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek in dump file: %w", err)
	}

	scanner := bufio.NewScanner(f)
	var stmt bytes.Buffer
	inInsert := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, "INSERT INTO") && strings.HasSuffix(line, ";") {
			stmt.WriteString(line)
			inInsert = false
		} else if strings.HasPrefix(line, "INSERT INTO") {
			inInsert = true
			stmt.WriteString(" " + line)
		} else if inInsert {
			stmt.WriteString(" " + line)
			if strings.HasSuffix(line, ";") {
				inInsert = false
			}
		}

		*t.DumpReadProgress += len(scanner.Bytes()) + 2

		if !inInsert {
			insertSQL := stmt.String()
			if len(insertSQL) > 0 {
				dbRecords, err := dump.ParseInsertStatements(insertSQL, *t.Columns, nil)
				if err != nil {
					return fmt.Errorf("parse insert statement: %w", err)
				}
				records := toHandlerRecords(dbRecords)
				event := handler.Event{
					Operation:  handler.OpInsert,
					Table:      t.Name,
					Records:    records,
					ColumnInfo: *t.Columns,
				}
				for _, h := range registry.GetActive() {
					if err := h.HandleEvent(context.Background(), event); err != nil {
						// Log but continue — other handlers should still run
						fmt.Printf("handler %s failed for table %s: %v\n", h.Name(), t.Name, err)
					}
				}
			}
			if err := store.SetDumpReadProgress(t.Name, *t.DumpReadProgress); err != nil {
				return err
			}
			stmt.Reset()
		}
	}
	return scanner.Err()
}
