# MySQL to Elasticsearch Synchronizer

-----

This project is a result of my passion for low level technologies and provides a solution for synchronizing data from a MySQL database to an Elasticsearch cluster. It leverages MySQL's binary log (binlog) to capture real-time changes (inserts, updates, deletes) and also supports initial data dumping for full synchronization. Designed with **minimum DevOps overhead** in mind, it aims for straightforward deployment and operation. It has a long way to go, and I'm committed to make it useable in real world scenarios.

## Features

  * **Real-time Synchronization**: Utilizes MySQL's binlog to propagate changes to targets in near real-time.
  * **Initial Data Dump**: Performs an initial dump of specified MySQL tables to populate target storage systems.
  * **Pluggable Handler Architecture**: Event-driven design with pluggable handlers for different target systems. Currently includes Elasticsearch handler, with framework for adding webhooks, additional databases, and custom destinations.
  * **State Persistence**: Abstracted storage layer with filesystem implementation for tracking synchronization state, binlog positions, and table metadata.
  * **Resilience**: Designed to resume synchronization from the last known binlog position, ensuring data consistency even after restarts, minimizing manual intervention.
  * **7-State Table Lifecycle**: Robust state management (Created → Dumping → Dumped → Initialized → Moving → Moved → Syncing) ensuring reliable data transfer and recovery.
  * **Handler Registry**: Centralized lifecycle management for multiple active handlers, enabling flexible deployment scenarios.
  * **Minimum DevOps Overhead**:
      * **Self-contained**: Deploys as a single executable with no complex external dependencies beyond MySQL and target systems.
      * **Automated State Management**: Automatically manages binlog positions and dump progress internally, reducing the need for external state management systems.
      * **Simplified Deployment**: Just build and run. Can be easily containerized or run as a system service.
  * **Error Handling and Logging**: Comprehensive logging provides insights into the synchronization process and helps in troubleshooting.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

Before you begin, ensure you have the following installed:

  * **Docker**: Required for building and running the application. Go commands run inside Docker (`golang:1.23.4`) so no local Go installation needed.
  * **MySQL Database**: With binary logging enabled.
  * **Target Systems**: Currently supports Elasticsearch 7.x+ (additional handlers can be added).

### Installation

**Note**: This project uses Docker for development. Always use `make` targets instead of raw `docker run` commands to ensure proper bind mounts and environment variables.

1.  **Clone the Repository**:

    ```bash
    git clone https://github.com/k1-end/mysql-2-elastic.git
    cd mysql-2-elastic
    ```

2.  **Build Base Docker Image** (required for test environment):

    ```bash
    docker build -f Dockerfile-base -t base-go-1 .
    ```

3.  **Build the Application**:

    ```bash
    make build
    ```

    This builds the `./mysql-2-elastic` binary using the Docker-based build process.

4.  **Clean up Dependencies**:

    ```bash
    make tidy
    ```

5.  **Run Code Analysis**:

    ```bash
    make vet
    ```

### Configuration

The application uses a configuration file (`config.json`) to set up connections and synchronization rules. Copy `config.json.example` and modify it for your environment:

```json
{
  "database": {
    "driver": "mysql",
    "host": "127.0.0.1",
    "port": 3306,
    "name": "world",
    "username": "root",
    "password": "password",
    "server_id": 100
  },
  "handlers": {
    "elasticsearch": {
      "address": "http://127.0.0.1:9200",
      "username": "elastic",
      "password": "password"
    }
  },
  "active_handlers": ["elasticsearch"],
  "mysqldump_path": "/usr/bin/mariadb-dump"
}
```

**Configuration Parameters**:

- `database`: MySQL connection configuration
  - `server_id`: Unique identifier for this synchronizer instance (required for binlog replication)
- `handlers`: Configuration for each available handler (currently `elasticsearch`)
- `active_handlers`: Array of handler names to enable (supports multiple active handlers)
- `mysqldump_path`: Path to mariadb-dump or mysqldump binary for initial data export

**Important MySQL Configuration**:

Ensure your MySQL server has binary logging enabled and a unique `server_id`. You can check this in your `my.cnf` or `my.ini` file:

```ini
[mysqld]
log_bin = mysql-bin
server_id = 1
binlog_format = ROW # Important for capturing row-level changes
binlog_row_image = FULL # Recommended for complete row data
```

After modifying `my.cnf` (or `my.ini`), restart your MySQL server.

### Running the Application

Once configured, you can run the application:

```bash
./mysql-2-elastic
```

The application will:
1. Initialize the handler registry with configured handlers
2. Connect to MySQL and start the binlog syncer
3. For each registered table: perform initial dump → transfer to active handlers → catch up binlog → enter real-time sync loop
4. Block on binlog events until interrupted (SIGINT/SIGTERM)

The `data/` directory will be created at runtime for dump files and internal state persistence.

## How it Works

### Architecture Overview

The application follows an event-driven architecture with a pluggable handler pattern:

1. **Handler Interface**: `internal/handler/` defines the `Handler` interface for target system operations (initialize, bulk write, update, delete)
2. **Handler Registry**: `internal/handler/registry.go` manages handler lifecycle (initialization, health checks, graceful shutdown)
3. **Storage Abstraction**: `internal/storage/` provides a pluggable storage layer for state persistence (filesystem implementation included)
4. **Pipeline Orchestration**: `internal/pipeline/` manages the 7-state table lifecycle and coordinates handlers

### 7-State Table Lifecycle

Each registered table progresses through these states:

1. **Created**: Table registered in configuration, ready for processing
2. **Dumping**: MySQL data export in progress using mysqldump
3. **Dumped**: Dump file created successfully
4. **Initialized**: Handler metadata prepared (ES indices created, etc.)
5. **Moving**: Bulk data transfer from dump file to active handlers
6. **Moved**: Initial data fully transferred, ready for binlog catch-up
7. **Syncing**: Real-time binlog synchronization active

### Execution Flow

**Initialization**:
- Load configuration via Viper
- Initialize handler registry with configured handlers
- Connect to storage backend for state persistence
- Start MySQL binlog syncer with last known position

**Table Processing** (`pipeline.Run`):
- Iterate through registered tables
- For tables in `Created`/`Dumping` state: Clear incomplete data, perform `InitialDump` to local file
- For tables in `Dumped`/`Moving` state: Transfer dump data to active handlers, store binlog coordinates
- For tables in `Moved` state: Compare dump binlog position with current position, perform catch-up sync via `SyncTablesTillDestination`
- Transition tables to `Syncing` state for real-time processing

**Real-time Synchronization**:
- Main loop continuously reads MySQL binlog events from last synchronized position
- **`RotateEvent`**: Handle binlog file rotation, update current binlog filename
- **`RowsEvent`**: Process row changes (INSERT/UPDATE/DELETE)
  - Extract and convert row data to handler-compatible format
  - Dispatch to active handlers via bulk operations for efficiency
- Persist current binlog position periodically for seamless recovery

**State Persistence**:
- `data/dumps/`: Temporary storage for MySQL dump files during initial transfer
- Storage backend: Tracks table metadata, synchronization status, dump progress, column information, and last binlog position
- File-based approach simplifies deployment and eliminates external dependencies

## Project Structure

  * `main.go`: Entry point (~60 lines) — thin orchestrator that loads config, initializes handlers, and starts the pipeline
  * `internal/handler/`: Core handler interface defining target system operations (Initialize, BulkWrite, Update, Delete)
  * `internal/handler/elasticsearch/`: Elasticsearch handler implementation with bulk operations
  * `internal/handler/registry.go`: Handler lifecycle management — initialization, health checks, graceful shutdown
  * `internal/pipeline/`: Table lifecycle orchestration and event processing (renamed from syncer)
  * `internal/config/`: Viper-based configuration loading from `config.json`
  * `internal/binlog/`: Binlog position tracking and syncer initialization
  * `internal/database/`: MySQL schema and row data helpers
  * `internal/dump/`: SQL dump creation, INSERT parsing, dump-to-handler transfer (renamed from dumpFile)
  * `internal/es/`: Elasticsearch client and bulk operations (renamed from elastic)
  * `internal/storage/`: Storage interface definition for state persistence
  * `internal/storage/filesystem/`: Filesystem implementation of storage interface
  * `internal/table/`: Core data types (BinlogPosition, RegisteredTable, ColumnInfo, DbRecord)
  * `internal/logger/`: Slog-based logging with configurable level
  * `internal/util/`: General utility functions (directory creation, server status)
  * `data/`: Runtime directory (gitignored) for dump files and internal state
  * `.test-environment/`: Docker Compose setup for integration testing (MySQL 8.0, Elasticsearch 7.8.1, Kibana)

## Development

### Docker-Based Development

All Go commands run inside Docker (`golang:1.23.4`) so no local Go installation is needed. The Makefile includes required bind mounts and environment variables.

**Important Rules**:
- Always use `make` targets instead of raw `docker run` commands
- If you must run raw Docker commands, include the Go module cache mount: `-v $HOME/go/pkg/mod:/go/pkg/mod`

### Makefile Commands

```bash
make build    # Build ./mysql-2-elastic binary
make tidy     # Run go mod tidy
make vet      # Run go vet ./...
```

### Testing

Tests are **integration-only** — they connect to live MySQL and Elasticsearch instances. There are no unit tests.

**Test Environment Setup**:

1. Build the base image:
   ```bash
   docker build -f Dockerfile-base -t base-go-1 .
   ```

2. Start the test stack:
   ```bash
   cd .test-environment
   docker-compose up -d
   ```

This spins up:
- MySQL 8.0 on port **3311**
- Elasticsearch 7.8.1 on port **9206**
- Kibana on port **5606**

**Running Tests**:

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

### Configuration

- `config.json` is gitignored — never commit it. Always create from `config.json.example`.
- `data/` directory is gitignored — created at runtime for dump files and internal state.

### Gotchas

- Docker images use regional mirrors (`hub.hamdocker.ir`, `docker.arvancloud.ir`) — may fail outside Iran
- CI only builds Docker — no lint, test, or typecheck in CI
- Tests require the full docker-compose stack running; they cannot run standalone
- The binary also depends on `mysqldump` (mariadb-dump) being installed on the host for initial data dumps

-----

## Future Steps and Plan

I am committed to continuously improving this synchronizer. Here are my future plans:

### Handler Architecture Enhancements
  * **Webhook Handler**: Add HTTP webhook handler for real-time event notifications to external systems
  * **Database Handlers**: Implement handlers for PostgreSQL, MongoDB, and other database systems
  * **Message Queue Handlers**: Add handlers for Kafka, RabbitMQ, and other message queues
  * **Custom Handler SDK**: Provide SDK and documentation for implementing custom handlers

### Core Improvements
  * **Integration and stress tests for binlog events**
  * **HTTP API**: RESTful API for monitoring and management (status, metrics, table lifecycle control)
  * **Enhanced Error Handling and Alerts**: Implement more sophisticated error handling mechanisms, including retry policies for transient errors and integration with alerting systems (e.g., Prometheus, Grafana) to notify operators of critical issues.
  * **Schema Evolution Handling**: Automatically detect and adapt to schema changes in MySQL tables (e.g., adding/removing columns, changing data types) and propagate these changes to target handlers with minimal or no downtime.
  * **Performance Optimizations**: Further optimize data processing and bulk indexing for very high-volume scenarios. This could involve parallel processing of dump files or more fine-grained batching for binlog events.
  * **Support for More Complex Data Types**: Improve handling of specific MySQL data types that might require special mapping or transformation before being sent to handlers (e.g., JSON columns, spatial data).

### Storage and State Management
  * **Additional Storage Backends**: Implement storage backends for Redis, PostgreSQL, and other databases for state persistence
  * **Distributed State**: Support for distributed deployments with shared state storage
  * **State Migration**: Tools for migrating state between storage backends

### Operations and Monitoring
  * **Dynamic Table Management**: Allow for dynamically adding or removing tables from synchronization without requiring a full application restart. This would involve a more robust control plane for managing the synchronization lifecycle.
  * **Configuration Reloading**: Implement hot reloading of configuration changes, so updates to tables or connection details don't require stopping and starting the synchronizer.
  * **Metrics and Monitoring**: Expose internal metrics (e.g., binlog lag, number of processed events, handler indexing rates) via a standardized interface (e.g., Prometheus endpoint) for better observability.
  * **Health Checks**: Comprehensive health checks for MySQL connectivity, handler availability, and synchronization lag

-----

Feel free to open issues or submit pull requests.
