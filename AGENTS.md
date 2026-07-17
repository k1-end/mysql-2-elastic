# mysql-2-elastic

MySQL-to-Elasticsearch real-time synchronizer using binlog replication.

## Docker Usage (IMPORTANT — read before running any Docker command)

All Go commands run inside Docker (`golang:1.23.4`) so no local Go installation is needed.

**Rule: Always use `make` targets instead of raw `docker run` commands.** The Makefile includes required bind mounts and environment variables.

If you must run a raw `docker run`, you **always** need this bind mount for the Go module cache:

```
-v $HOME/go/pkg/mod:/go/pkg/mod
```

Full example:
```bash
docker run --rm --network=host \
  -v $HOME/go/pkg/mod:/go/pkg/mod \
  -v $(pwd):/app -w /app golang:1.23.4 \
  go vet ./...
```

Without this mount, `go mod tidy` and builds will re-download all dependencies every time and may fail with checksum mismatches.

## Build and Run

```bash
make build   # builds ./mysql-2-elastic binary
make tidy    # go mod tidy
make vet     # go vet ./...
```

Requires `config.json` in the project root (copy from `config.json.example`). The binary also depends on `mysqldump` (mariadb-dump) being installed on the host for initial data dumps.

## Tests

Tests are **integration-only** — they connect to a live MySQL and Elasticsearch instance. There are no unit tests.

```bash
# Requires running MySQL + Elasticsearch from .test-environment/docker-compose.yml
docker run --rm --network=host \
  -v $HOME/go/pkg/mod:/go/pkg/mod \
  -v $(pwd):/app -w /app golang:1.23.4 \
  go test -v -run TestSync ./...
docker run --rm --network=host \
  -v $HOME/go/pkg/mod:/go/pkg/mod \
  -v $(pwd):/app -w /app golang:1.23.4 \
  go test -v -run TestSeed ./...
```

The `seed.bash` script loads `world.sql` into MySQL for test data. Both test files live at the project root (`*_test.go`).

## Test Environment

`.test-environment/docker-compose.yml` spins up:
- MySQL 8.0 on port **3311**
- Elasticsearch 7.8.1 on port **9206**
- Kibana on port **5606**

The `main` service uses a pre-built base image `base-go-1` (built from `Dockerfile-base`). You must build it first:

```bash
docker build -f Dockerfile-base -t base-go-1 .
```

## Architecture

Single-binary Go app. Entry point is `main.go` (~60 lines) — a thin orchestrator.

**Execution flow:**
1. Load `config.json` via Viper
2. Connect to Elasticsearch
3. Initialize binlog syncer
4. For each registered table: dump from MySQL → send to ES → catch up binlog → enter real-time sync loop
5. `pipeline.Run` blocks on binlog events until context is cancelled (SIGINT/SIGTERM)

**Internal packages:**
| Package | Purpose |
|---|---|
| `config` | Viper-based config loading from `config.json` |
| `database` | MySQL schema/row helpers |
| `dump` | SQL dump creation, INSERT parsing, dump-to-ES transfer |
| `es` | Elasticsearch bulk operations (single `bulkOp` function) |
| `binlog` | Binlog position tracking, syncer initialization |
| `pipeline` | Table lifecycle orchestration, event processing, sync loop |
| `logger` | slog-based logging (configurable level) |
| `storage` + `storage/filesystem` | File-based state persistence (table status, binlog positions, dump progress) |
| `table` | Data types: `BinlogPosition`, `RegisteredTable`, `ColumnInfo`, `DbRecord` |
| `util` | Misc helpers (directory creation, server status) |

**Key file:** `internal/storage/storage.go` defines the `TableStorage` interface — the filesystem implementation is the only one.

## Dependency Graph

```
main → config, binlog, es, pipeline, storage/filesystem, logger, util
pipeline → binlog, config, database, dump, es, storage, table
dump → config, storage, table, database, util
es → config, table
database → table
storage → table
storage/filesystem → storage, table
table → (no internal deps — leaf package)
binlog → config, table
logger → (stdlib only)
util → (stdlib only)
```

No circular dependencies. `table` is the leaf package everything imports.

## Gotchas

- **`config.json` is gitignored** — never commit it. Always create from `config.json.example`.
- **`data/` directory is gitignored** — created at runtime for dump files and internal state.
- **Docker images use regional mirrors** (`hub.hamdocker.ir`, `docker.arvancloud.ir`) — may fail outside Iran.
- **CI only builds Docker** — no lint, test, or typecheck in CI. The workflow triggers only on commits containing "docker build" in the message.
- Tests require the full docker-compose stack running; they cannot run standalone.
