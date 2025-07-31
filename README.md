# MySQL to Elasticsearch Synchronizer

-----

This project is a result of my passion for low level technologies and provides a solution for synchronizing data from a MySQL database to an Elasticsearch cluster. It leverages MySQL's binary log (binlog) to capture real-time changes (inserts, updates, deletes) and also supports initial data dumping for full synchronization. Designed with **minimum DevOps overhead** in mind, it aims for straightforward deployment and operation. It has a long way to go, and I'm committed to make it useable in real world scenarios.

## Features

  * **Real-time Synchronization**: Utilizes MySQL's binlog to propagate changes to Elasticsearch in near real-time.
  * **Initial Data Dump**: Performs an initial dump of specified MySQL tables to populate Elasticsearch indices.
  * **Resilience**: Designed to resume synchronization from the last known binlog position, ensuring data consistency even after restarts, minimizing manual intervention.
  * **Configurable**: Easy to configure MySQL and Elasticsearch connection details, as well as the tables to synchronize, using a simple JSON file.
  * **Minimum DevOps Overhead**:
      * **Self-contained**: Deploys as a single executable with no complex external dependencies beyond MySQL and Elasticsearch.
      * **Automated State Management**: Automatically manages binlog positions and dump progress internally, reducing the need for external state management systems.
      * **Simplified Deployment**: Just build and run. Can be easily containerized or run as a system service.
  * **Error Handling and Logging**: Comprehensive logging provides insights into the synchronization process and helps in troubleshooting.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

Before you begin, ensure you have the following installed:

  * **Go (Golang)**: Version 1.18 or higher.
  * **MySQL Database**: With binary logging enabled.
  * **Elasticsearch Cluster**: Running and accessible.

### Installation

1.  **Clone the Repository**:

    ```bash
    git clone https://github.com/k1-end/mysql-2-elastic.git
    cd mysql-2-elastic
    ```

2.  **Download Dependencies**:

    ```bash
    go mod tidy
    ```

3.  **Build the Application**:

    ```bash
    go build -o mysql-2-elastic main.go
    ```

### Configuration

The application uses a configuration file (e.g., `config.json` or environment variables) to set up connections and synchronization rules. A sample JSON configuration might look like this:

```json
// config.json (example)
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
  "mysqldump_path": "/usr/bin/mariadb-dump",
  "world_database_path": "world.sql", // This is just for testing
  "elastic": {
      "address": "http://127.0.0.1:9200",
      "username": "elastic",
      "password": "password"
  }
}
```

**Important MySQL Configuration**:

Ensure your MySQL server has binary logging enabled and a unique `server_id`. You can check this in your `my.cnf` or `my.ini` file:

```ini
[mysqld]
log_bin = mysql-bin
server_id = 1
binlog_format = ROW # Important for capturing row-level changes
```

After modifying `my.cnf` (or `my.ini`), restart your MySQL server.

### Running the Application

Once configured, you can run the application:

```bash
./mysql-2-elastic
```

The application will first perform an initial dump of the configured tables (if they haven't been fully synchronized yet) and then start tailing the MySQL binlog for real-time updates.

## How it Works

1.  **Initialization**:

      * Loads configuration for MySQL and Elasticsearch.
      * Connects to Elasticsearch.
      * Initializes a MySQL binlog syncer.
      * Sets up a file-based storage for tracking synchronization progress and table metadata (e.g., dump progress, binlog positions, column information) – **all handled internally without external databases or services.**

2.  **Table Processing (`initializeTables` function)**:

      * Iterates through registered tables.
      * If a table is `created` or `dumping`, it clears any incomplete dump data and performs an `InitialDump` from MySQL to a local file.
      * If a table is `dumped` or `moving`, it reads data from the local dump file and sends it to Elasticsearch. It also stores the binlog coordinates captured during the dump.
      * Once a table's initial dump is complete and moved to Elasticsearch, its status changes to `moved`.
      * Finally, for tables with status `moved`, it compares the binlog position from the dump with the current main binlog position. It then performs a `SyncTablesTillDestination` to catch up any events that occurred between the dump and the current binlog position, ensuring all changes are applied. The table status is then set to `syncing`.

3.  **Real-time Synchronization (`runTheSyncer` function)**:

      * The main loop continuously reads events from the MySQL binlog starting from the last known synchronized position.
      * **`RotateEvent`**: Handles binlog file rotation, updating the current binlog filename.
      * **`RowsEvent`**: Processes `WRITE_ROWS_EVENT` (INSERT), `UPDATE_ROWS_EVENT`, and `DELETE_ROWS_EVENT`.
          * It extracts row data and converts it into a format suitable for Elasticsearch.
          * It then uses Elasticsearch's Bulk API to efficiently send, update, or delete documents in the corresponding Elasticsearch index.
      * The current binlog position is periodically persisted to ensure seamless recovery.

4.  **Data Storage (`data/dumps` and internal storage)**:

      * The `data/dumps` directory is used to store temporary dump files during the initial data transfer.
      * Internal storage (likely `internal/storage/filesystem`) is used to persist metadata about registered tables, their synchronization status, dump progress, column information, and the last synchronized binlog position. **This file-based approach simplifies deployment and eliminates the need for an additional database for the synchronizer's own state.**

## Project Structure

  * `main.go`: The main entry point of the application, orchestrating the initial dump and continuous binlog synchronization.
  * `internal/config`: Handles application configuration loading.
  * `internal/database`: Contains utilities for interacting with MySQL database schema and data.
  * `internal/dumpFile`: Manages the process of dumping initial data from MySQL and parsing dump files.
  * `internal/elastic`: Provides functionalities for interacting with Elasticsearch, including bulk operations.
  * `internal/logger`: Configures and manages application logging.
  * `internal/storage`: Defines interfaces for persisting application state (e.g., `TableStorage`).
  * `internal/storage/filesystem`: Implements file-system based storage for application state.
  * `internal/syncer`: Manages the MySQL binlog synchronization logic and position tracking.
  * `internal/table`: Defines data structures for representing tables and their metadata.
  * `internal/util`: Contains general utility functions.

-----

## Future Steps and Plan

I am committed to continuously improving this synchronizer. Here are my future plans:

  * **Integration and stress tests for binlog events**
  * **HTTP API**
  * **Enhanced Error Handling and Alerts**: Implement more sophisticated error handling mechanisms, including retry policies for transient errors and integration with alerting systems (e.g., Prometheus, Grafana) to notify operators of critical issues.
  * **Schema Evolution Handling**: Automatically detect and adapt to schema changes in MySQL tables (e.g., adding/removing columns, changing data types) and propagate these changes to Elasticsearch mappings with minimal or no downtime.
  * **Performance Optimizations**: Further optimize data processing and bulk indexing for very high-volume scenarios. This could involve parallel processing of dump files or more fine-grained batching for binlog events.
  * **Support for More Complex Data Types**: Improve handling of specific MySQL data types that might require special mapping or transformation before being sent to Elasticsearch (e.g., JSON columns, spatial data).
  * **Dynamic Table Management**: Allow for dynamically adding or removing tables from synchronization without requiring a full application restart. This would involve a more robust control plane for managing the synchronization lifecycle.
  * **Configuration Reloading**: Implement hot reloading of configuration changes, so updates to tables or connection details don't require stopping and starting the synchronizer.
  * **Metrics and Monitoring**: Expose internal metrics (e.g., binlog lag, number of processed events, Elasticsearch indexing rates) via a standardized interface (e.g., Prometheus endpoint) for better observability.

-----

Feel free to open issues or submit pull requests.
