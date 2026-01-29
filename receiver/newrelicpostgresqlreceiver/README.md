# New Relic PostgreSQL Receiver

The New Relic PostgreSQL Receiver is an OpenTelemetry receiver that collects metrics from PostgreSQL databases.

## Features

### Basic Capabilities

- PostgreSQL database connection support
- SSL/TLS connection encryption
- Certificate-based authentication
- Connection pooling configuration

### Environment Compatibility

**Supported PostgreSQL Versions:**

- PostgreSQL 9.2 and later versions
- All columns are supported in PostgreSQL 9.2+
- Note: `blk_read_time` and `blk_write_time` metrics require `track_io_timing = on` (available since PostgreSQL 9.2)

**Supported Environments:**

- ✅ Amazon RDS for PostgreSQL
- ✅ Amazon Aurora PostgreSQL
- ✅ Azure Database for PostgreSQL
- ✅ Google Cloud SQL for PostgreSQL
- ✅ Self-hosted PostgreSQL instances

All cloud providers fully support the `pg_stat_database` view and related functions used by this receiver.

## Configuration

### Basic Configuration

```yaml
receivers:
  newrelicpostgresqlreceiver:
    hostname: "localhost"
    port: "5432"
    username: "monitoring_user"
    password: "secure_password"
    database: "postgres"
    collection_interval: 15s
```

### Full Configuration Options

```yaml
receivers:
  newrelicpostgresqlreceiver:
    # Connection settings
    hostname: "postgres-server.example.com" # Required: PostgreSQL hostname
    port: "5432" # Port number (default: 5432)
    username: "monitor_user" # Required: PostgreSQL username
    password: "secure_password" # PostgreSQL password
    database: "postgres" # Required: Database name to connect to

    # SSL Configuration
    ssl_mode: "disable" # SSL mode: disable, require, verify-ca, verify-full
    ssl_cert: "/path/to/client-cert.pem" # Path to SSL client certificate
    ssl_key: "/path/to/client-key.pem" # Path to SSL client key
    ssl_root_cert: "/path/to/ca-cert.pem" # Path to SSL root certificate
    ssl_password: "" # SSL certificate password (if encrypted)

    # Collection settings
    collection_interval: 15s # How often to collect metrics
    timeout: 30s # Query timeout duration
```

### Per-Table Metrics Configuration (Optional)

**⚠️ WARNING: HIGH CARDINALITY - Opt-in Feature**

The receiver supports collecting per-table vacuum and analyze metrics from `pg_stat_user_tables`. This feature is **disabled by default** to prevent cardinality explosion.

#### Why is this opt-in?

Per-table metrics can create high cardinality:
- 1000 tables × 8 metrics = 8,000 unique time series
- Each time series consumes memory, storage, and processing resources
- Can significantly impact New Relic costs and performance

#### Configuration

To enable per-table metrics, add the `relations` configuration:

```yaml
receivers:
  newrelicpostgresqlreceiver:
    hostname: "localhost"
    port: "5432"
    username: "postgres"
    password: "password"
    database: "postgres"

    # Per-table metrics (OPTIONAL - HIGH CARDINALITY!)
    relations:
      schemas: [public, myapp]  # Defaults to ["public"] if not specified
      tables: [users, orders, payments, inventory]  # Specify exact table names
```

#### Available Per-Table Metrics (PostgreSQL 9.6+)

**Vacuum/Analyze Age Metrics (Gauge):**
- `postgresql.last_vacuum_age` - Seconds since last manual VACUUM
- `postgresql.last_autovacuum_age` - Seconds since last autovacuum
- `postgresql.last_analyze_age` - Seconds since last manual ANALYZE
- `postgresql.last_autoanalyze_age` - Seconds since last autoanalyze

**Vacuum/Analyze Count Metrics (Cumulative Sum):**
- `postgresql.vacuumed` - Number of manual vacuum operations
- `postgresql.autovacuumed` - Number of autovacuum operations
- `postgresql.analyzed` - Number of manual analyze operations
- `postgresql.autoanalyzed` - Number of autoanalyze operations

Each metric includes attributes: `schema_name` and `table_name`

#### Performance Guidelines

| Configuration | Tables | Metrics per Interval | Recommended For |
|--------------|--------|---------------------|-----------------|
| Disabled (default) | 0 | 0 | Most users |
| Small (3-5 tables) | 5 | 40 | Critical tables only |
| Medium (10-20 tables) | 20 | 160 | Important schemas |
| Large (50+ tables) | 50+ | 400+ | Advanced monitoring (costly) |

**Best Practices:**
- Only monitor critical tables (authentication, transactions, etc.)
- Avoid monitoring system tables or temporary tables
- Start small and add tables as needed
- Monitor your New Relic metric cardinality dashboard

**Note:** Cumulative metrics are automatically converted to delta using the `cumulativetodelta` processor for better New Relic compatibility.

### ANALYZE Progress Metrics (PostgreSQL 13+)

The receiver automatically collects real-time progress metrics for running ANALYZE operations. These metrics are **enabled by default** and require no configuration.

#### Why are these safe by default?

ANALYZE progress metrics are naturally low-cardinality:
- Only tracks **actively running** ANALYZE operations (typically 0-2 concurrent operations)
- Metrics automatically disappear when ANALYZE completes
- No cardinality explosion risk (unlike per-table statistics which track ALL tables)

#### Available ANALYZE Progress Metrics (Gauge)

**Sample Block Progress:**
- `postgresql.analyze.sample_blks_total` - Total number of blocks to sample
- `postgresql.analyze.sample_blks_scanned` - Number of blocks scanned so far

**Extended Statistics Progress:**
- `postgresql.analyze.ext_stats_total` - Total extended statistics to compute
- `postgresql.analyze.ext_stats_computed` - Number of extended statistics computed

**Partitioned Table Progress:**
- `postgresql.analyze.child_tables_total` - Total child tables to analyze
- `postgresql.analyze.child_tables_done` - Number of child tables analyzed

Each metric includes attributes: `database_name`, `schema_name`, and `table_name`

#### Use Cases

- **Monitor long-running ANALYZE operations** on large tables
- **Track progress** of manual ANALYZE commands
- **Identify bottlenecks** in partitioned table analysis
- **Alert on stuck ANALYZE** operations that don't make progress

#### Example: Monitor ANALYZE Progress in New Relic

```sql
FROM Metric
SELECT latest(postgresql.analyze.sample_blks_scanned) / latest(postgresql.analyze.sample_blks_total) * 100 AS 'Progress %'
WHERE metricName LIKE 'postgresql.analyze%'
FACET table_name
TIMESERIES
```

### CLUSTER/VACUUM FULL Progress Metrics (PostgreSQL 12+)

The receiver automatically collects real-time progress metrics for running CLUSTER and VACUUM FULL operations. These metrics are **enabled by default** and require no configuration.

#### Why are these safe by default?

CLUSTER/VACUUM FULL progress metrics are naturally low-cardinality:
- Only tracks **actively running** CLUSTER or VACUUM FULL operations (typically 0-1 concurrent operations)
- Metrics automatically disappear when the operation completes
- No cardinality explosion risk (unlike per-table statistics which track ALL tables)

#### Available CLUSTER/VACUUM FULL Progress Metrics (Gauge)

**Heap Block Progress:**
- `postgresql.cluster_vacuum.heap_blks_total` - Total number of heap blocks to scan
- `postgresql.cluster_vacuum.heap_blks_scanned` - Number of heap blocks scanned so far

**Tuple Progress:**
- `postgresql.cluster_vacuum.heap_tuples_scanned` - Number of heap tuples scanned
- `postgresql.cluster_vacuum.heap_tuples_written` - Number of heap tuples written to new heap

Each metric includes attributes: `command` (CLUSTER or VACUUM FULL), `database_name`, `schema_name`, and `table_name`

#### Use Cases

- **Monitor long-running CLUSTER operations** on large tables
- **Track progress** of manual VACUUM FULL commands
- **Identify bottlenecks** in table reorganization
- **Alert on stuck operations** that don't make progress
- **Compare performance** between CLUSTER and VACUUM FULL

#### Example: Monitor CLUSTER/VACUUM FULL Progress in New Relic

```sql
FROM Metric
SELECT latest(postgresql.cluster_vacuum.heap_blks_scanned) / latest(postgresql.cluster_vacuum.heap_blks_total) * 100 AS 'Progress %'
WHERE metricName LIKE 'postgresql.cluster_vacuum%'
FACET table_name, command
TIMESERIES
```

### CREATE INDEX Progress Metrics (PostgreSQL 12+)

The receiver automatically collects real-time progress metrics for running CREATE INDEX operations. These metrics are **enabled by default** and require no configuration.

#### Why are these safe by default?

CREATE INDEX progress metrics are naturally low-cardinality:
- Only tracks **actively running** CREATE INDEX operations (typically 0-2 concurrent operations)
- Metrics automatically disappear when the index creation completes
- No cardinality explosion risk (unlike per-table statistics which track ALL tables)

#### Available CREATE INDEX Progress Metrics (Gauge)

**Locker Progress (for concurrent index builds):**
- `postgresql.create_index.lockers_total` - Total number of lockers to wait for (concurrent builds only)
- `postgresql.create_index.lockers_done` - Number of lockers processed

**Block Progress:**
- `postgresql.create_index.blocks_total` - Total number of blocks to be processed
- `postgresql.create_index.blocks_done` - Number of blocks processed

**Tuple Progress:**
- `postgresql.create_index.tuples_total` - Total number of tuples to be indexed
- `postgresql.create_index.tuples_done` - Number of tuples indexed

**Partition Progress (for partitioned tables):**
- `postgresql.create_index.partitions_total` - Total number of partitions to be indexed
- `postgresql.create_index.partitions_done` - Number of partitions indexed

Each metric includes attributes: `database_name`, `index_name`, `schema_name`, and `table_name`

#### Use Cases

- **Monitor index build progress** for large tables
- **Track REINDEX operations** on critical indexes
- **Identify slow index builds** that may impact performance
- **Alert on stuck CREATE INDEX CONCURRENTLY** operations
- **Optimize concurrent index creation** strategies

#### Example: Monitor CREATE INDEX Progress in New Relic

```sql
FROM Metric
SELECT latest(postgresql.create_index.tuples_done) / latest(postgresql.create_index.tuples_total) * 100 AS 'Progress %'
WHERE metricName LIKE 'postgresql.create_index%'
FACET table_name, index_name
TIMESERIES
```

### VACUUM Progress Metrics (PostgreSQL 12+)

The receiver automatically collects real-time progress metrics for running VACUUM operations. These metrics are **enabled by default** and require no configuration.

#### Why are these safe by default?

VACUUM progress metrics are naturally low-cardinality:
- Only tracks **actively running** VACUUM operations (typically 0-3 concurrent operations)
- Metrics automatically disappear when the vacuum completes
- No cardinality explosion risk (unlike per-table statistics which track ALL tables)

#### Available VACUUM Progress Metrics (Gauge)

**Heap Block Progress:**
- `postgresql.vacuum.heap_blks_total` - Total number of heap blocks in the table
- `postgresql.vacuum.heap_blks_scanned` - Number of heap blocks scanned so far
- `postgresql.vacuum.heap_blks_vacuumed` - Number of heap blocks vacuumed so far

**Index Vacuum Progress:**
- `postgresql.vacuum.index_vacuum_count` - Number of completed index vacuum cycles

**Dead Tuple Statistics:**
- `postgresql.vacuum.max_dead_tuples` - Maximum number of dead tuples before index vacuum is required
- `postgresql.vacuum.num_dead_tuples` - Current number of dead tuples collected

Each metric includes attributes: `database_name`, `schema_name`, and `table_name`

#### Use Cases

- **Monitor vacuum progress** for large tables
- **Track autovacuum operations** to ensure tables are being maintained
- **Identify slow vacuum operations** that may impact performance
- **Alert on stuck VACUUM** operations that don't make progress
- **Optimize vacuum_cost_delay settings** based on observed progress

#### Example: Monitor VACUUM Progress in New Relic

```sql
FROM Metric
SELECT latest(postgresql.vacuum.heap_blks_scanned) / latest(postgresql.vacuum.heap_blks_total) * 100 AS 'Progress %',
       latest(postgresql.vacuum.num_dead_tuples) AS 'Dead Tuples',
       latest(postgresql.vacuum.index_vacuum_count) AS 'Index Vacuum Cycles'
WHERE metricName LIKE 'postgresql.vacuum%'
FACET table_name
TIMESERIES
```

## Prerequisites

### Database User Permissions

The monitoring user needs the following minimum permissions:

```sql
-- Create monitoring user
CREATE USER monitoring_user WITH PASSWORD 'secure_password';

-- Grant connect permission
GRANT CONNECT ON DATABASE postgres TO monitoring_user;

-- Grant usage on schema
GRANT USAGE ON SCHEMA public TO monitoring_user;
```

## Example Configuration

### Monitoring with SSL

```yaml
receivers:
  newrelicpostgresqlreceiver:
    hostname: "postgres.example.com"
    port: "5432"
    username: "monitor_user"
    password: "${env:PG_PASSWORD}"
    database: "production"
    ssl_mode: "verify-full"
    ssl_root_cert: "/etc/ssl/certs/ca-bundle.crt"
    collection_interval: 30s
```

## Development

To generate internal metadata code:

```bash
cd receiver/newrelicpostgresqlreceiver
make generate
```

To run tests:

```bash
make test
```

## Support

For issues, questions, or contributions, please refer to the OpenTelemetry Collector Contrib repository.
