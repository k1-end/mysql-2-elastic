# mysql-2-elastic

MySQL-to-Elasticsearch real-time synchronizer using binlog replication.

## Build and Run

```bash
go build -o mysql-2-elastic main.go
./mysql-2-elastic
```

Requires `config.json` in the project root (copy from `config.json.example`). The binary also depends on `mysqldump` (mariadb-dump) being installed on the host for initial data dumps.

## Tests

Tests are **integration-only** — they connect to a live MySQL and Elasticsearch instance. There are no unit tests.

```bash
# Requires running MySQL + Elasticsearch from .test-environment/docker-compose.yml
go test -v -run TestSync ./...
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

Single-binary Go app. Entry point is `main.go` — there is no CLI framework wired up yet (`cmd/root.go` defines a cobra root command but `main.go` does not use it).

**Execution flow:**
1. Load `config.json` via Viper
2. Connect to Elasticsearch
3. Initialize binlog syncer
4. For each registered table: dump from MySQL → send to ES → catch up binlog → enter real-time sync loop
5. `runTheSyncer` blocks on binlog events indefinitely

**Internal packages:**
| Package | Purpose |
|---|---|
| `config` | Viper-based config loading from `config.json` |
| `database` | MySQL schema/row helpers |
| `dumpFile` | SQL dump creation and INSERT statement parsing |
| `elastic` | Elasticsearch bulk operations |
| `logger` | slog-based logging |
| `storage` + `storage/filesystem` | File-based state persistence (table status, binlog positions, dump progress) |
| `syncer` | Binlog position tracking and syncer initialization |
| `table` | Data types for table metadata and records |
| `util` | Misc helpers |

**Key file:** `internal/storage/storage.go` defines the `TableStorage` interface — the filesystem implementation is the only one.

## Gotchas

- **`config.json` is gitignored** — never commit it. Always create from `config.json.example`.
- **`data/` directory is gitignored** — created at runtime for dump files and internal state.
- **Docker images use regional mirrors** (`hub.hamdocker.ir`, `docker.arvancloud.ir`) — may fail outside Iran.
- **CI only builds Docker** — no lint, test, or typecheck in CI. The workflow triggers only on commits containing "docker build" in the message.
- Tests require the full docker-compose stack running; they cannot run standalone.
- The `cmd/` package (cobra CLI) is scaffolding — unused by the actual entry point.
