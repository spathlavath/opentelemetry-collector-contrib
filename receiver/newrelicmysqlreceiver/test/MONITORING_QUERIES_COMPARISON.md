# MySQL Receiver - Query Performance Monitoring

This document provides MySQL queries for the New Relic MySQL receiver, modeled after the New Relic Oracle receiver pattern. Each query retrieves essential metrics without unnecessary details.

---

## Query 1: Slow Query Metrics

**Purpose**: Retrieve query performance metrics from historical aggregates

**Oracle Equivalent**: `V$SQLAREA` query in `GetSlowQueriesSQL()`

**Collection Interval**: 60 seconds (configurable)

### MySQL Query

```sql
SELECT
    UTC_TIMESTAMP() AS collection_timestamp,
    COALESCE(SCHEMA_NAME, 'NULL') AS database_name,
    DIGEST AS query_id,
    CASE
        WHEN CHAR_LENGTH(DIGEST_TEXT) > 4000 THEN CONCAT(LEFT(DIGEST_TEXT, 3997), '...')
        ELSE DIGEST_TEXT
    END AS query_text,
    COUNT_STAR AS execution_count,
    ROUND((SUM_CPU_TIME / NULLIF(COUNT_STAR, 0)) / 1000000, 3) AS avg_cpu_time_ms,
    ROUND((SUM_TIMER_WAIT / NULLIF(COUNT_STAR, 0)) / 1000000, 3) AS avg_elapsed_time_ms,
    ROUND((SUM_LOCK_TIME / NULLIF(COUNT_STAR, 0)) / 1000000, 3) AS avg_lock_time_ms,
    ROUND(SUM_ROWS_EXAMINED / NULLIF(COUNT_STAR, 0), 2) AS avg_rows_examined,
    ROUND(SUM_ROWS_AFFECTED / NULLIF(COUNT_STAR, 0), 2) AS avg_disk_writes,
    ROUND(SUM_TIMER_WAIT / 1000000, 3) AS total_elapsed_time_ms,
    DATE_FORMAT(CONVERT_TZ(LAST_SEEN, @@session.time_zone, '+00:00'), '%Y-%m-%dT%H:%i:%sZ') AS last_active_time
FROM
    performance_schema.events_statements_summary_by_digest
WHERE
    CONVERT_TZ(LAST_SEEN, @@session.time_zone, '+00:00') >= UTC_TIMESTAMP() - INTERVAL ? SECOND
    AND SCHEMA_NAME IS NOT NULL
    AND SCHEMA_NAME NOT IN ('performance_schema', 'information_schema', 'mysql', 'sys')
    AND DIGEST_TEXT NOT LIKE '%performance_schema%'
    AND DIGEST_TEXT NOT LIKE '%information_schema%'
    AND DIGEST_TEXT NOT LIKE '%events_statements_%'
    AND DIGEST_TEXT NOT LIKE '%events_waits_%'
ORDER BY
    avg_elapsed_time_ms DESC
LIMIT 100;
```

### Metrics Retrieved

| Metric | MySQL Source | Oracle Equivalent | New Relic MySQL | Datadog MySQL |
|--------|--------------|-------------------|-----------------|---------------|
| `collection_timestamp` | `UTC_TIMESTAMP()` | `SYSTIMESTAMP` | `collection_timestamp` | N/A |
| `database_name` | `SCHEMA_NAME` | `COALESCE(p.name, d.name)` | `database_name` | `schema_name` |
| `query_id` | `DIGEST` | `sql_id` | `query_id` | `digest` |
| `query_text` | `DIGEST_TEXT` | `sql_text` | `query_text` | `digest_text` |
| `execution_count` | `COUNT_STAR` | `executions` | `execution_count` | `count_star` |
| `avg_cpu_time_ms` | `SUM_CPU_TIME/COUNT_STAR` | `cpu_time/executions` | `avg_cpu_time_ms` | N/A |
| `avg_elapsed_time_ms` | `SUM_TIMER_WAIT/COUNT_STAR` | `elapsed_time/executions` | `avg_elapsed_time_ms` | `avg_timer_wait` |
| `avg_lock_time_ms` | `SUM_LOCK_TIME/COUNT_STAR` | `concurrency_wait_time/executions` | N/A | `avg_lock_time` |
| `avg_rows_examined` | `SUM_ROWS_EXAMINED/COUNT_STAR` | `buffer_gets/executions` | `avg_disk_reads` | `avg_rows_examined` |
| `avg_disk_writes` | `SUM_ROWS_AFFECTED/COUNT_STAR` | `direct_writes/executions` | `avg_disk_writes` | `avg_rows_affected` |
| `total_elapsed_time_ms` | `SUM_TIMER_WAIT` | `elapsed_time` | N/A | `sum_timer_wait` |
| `last_active_time` | `LAST_SEEN` | `last_active_time` | `last_execution_timestamp` | `last_seen` |

### Implementation Notes

```go
func GetSlowQueriesSQL(intervalSeconds int) string {
    return fmt.Sprintf(`
        SELECT
            UTC_TIMESTAMP() AS collection_timestamp,
            COALESCE(SCHEMA_NAME, 'NULL') AS database_name,
            DIGEST AS query_id,
            CASE
                WHEN CHAR_LENGTH(DIGEST_TEXT) > 4000 THEN CONCAT(LEFT(DIGEST_TEXT, 3997), '...')
                ELSE DIGEST_TEXT
            END AS query_text,
            COUNT_STAR AS execution_count,
            ROUND((SUM_CPU_TIME / NULLIF(COUNT_STAR, 0)) / 1000000, 3) AS avg_cpu_time_ms,
            ROUND((SUM_TIMER_WAIT / NULLIF(COUNT_STAR, 0)) / 1000000, 3) AS avg_elapsed_time_ms,
            ROUND((SUM_LOCK_TIME / NULLIF(COUNT_STAR, 0)) / 1000000, 3) AS avg_lock_time_ms,
            ROUND(SUM_ROWS_EXAMINED / NULLIF(COUNT_STAR, 0), 2) AS avg_rows_examined,
            ROUND(SUM_ROWS_AFFECTED / NULLIF(COUNT_STAR, 0), 2) AS avg_disk_writes,
            ROUND(SUM_TIMER_WAIT / 1000000, 3) AS total_elapsed_time_ms,
            DATE_FORMAT(CONVERT_TZ(LAST_SEEN, @@session.time_zone, '+00:00'), '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS last_active_time
        FROM
            performance_schema.events_statements_summary_by_digest
        WHERE
            CONVERT_TZ(LAST_SEEN, @@session.time_zone, '+00:00') >= UTC_TIMESTAMP() - INTERVAL %d SECOND
            AND SCHEMA_NAME IS NOT NULL
            AND SCHEMA_NAME NOT IN ('performance_schema', 'information_schema', 'mysql', 'sys')
            AND DIGEST_TEXT NOT LIKE '%%performance_schema%%'
            AND DIGEST_TEXT NOT LIKE '%%information_schema%%'
            AND DIGEST_TEXT NOT LIKE '%%events_statements_%%'
            AND DIGEST_TEXT NOT LIKE '%%events_waits_%%'
        ORDER BY
            avg_elapsed_time_ms DESC
        LIMIT 100`, intervalSeconds)
}
```

**Key Differences from Oracle**:
- **Time conversion**: Picoseconds → microseconds (divide by 1000000 vs 1000)
- **Schema filtering**: MySQL uses `SCHEMA_NAME NOT IN (...)` vs Oracle uses `username NOT IN (...)`
- **Cumulative metrics**: Both require delta calculation between collections

---

## Query 2: Wait Event Metrics

**Purpose**: Retrieve wait event statistics for currently executing queries

**Oracle Equivalent**: `V$SESSION_WAIT` + `V$ACTIVE_SESSION_HISTORY`

**Collection Interval**: 10-30 seconds

### MySQL Query

```sql
WITH wait_events_aggregated AS (
    SELECT
        w.THREAD_ID,
        esc.DIGEST AS query_id,
        esc.CURRENT_SCHEMA AS database_name,
        CASE
            WHEN w.EVENT_NAME LIKE 'wait/io/file%' THEN 'File I/O'
            WHEN w.EVENT_NAME LIKE 'wait/io/socket%' THEN 'Network I/O'
            WHEN w.EVENT_NAME LIKE 'wait/io/table%' THEN 'Table I/O'
            WHEN w.EVENT_NAME LIKE 'wait/lock%' THEN 'Lock'
            WHEN w.EVENT_NAME LIKE 'wait/synch%' THEN 'Synchronization'
            ELSE 'Other'
        END AS wait_category,
        w.EVENT_NAME AS wait_event_name,
        SUM(w.TIMER_WAIT) AS total_wait_time,
        COUNT(*) AS wait_count
    FROM performance_schema.events_waits_current w
    INNER JOIN performance_schema.events_statements_current esc 
        ON w.THREAD_ID = esc.THREAD_ID
    WHERE w.EVENT_NAME != 'idle'
      AND esc.DIGEST IS NOT NULL
    GROUP BY w.THREAD_ID, esc.DIGEST, esc.CURRENT_SCHEMA, wait_category, w.EVENT_NAME
)
SELECT
    UTC_TIMESTAMP() AS collection_timestamp,
    database_name,
    query_id,
    wait_category,
    wait_event_name,
    ROUND(total_wait_time / 1000000, 3) AS total_wait_time_ms,
    wait_count AS wait_event_count,
    ROUND((total_wait_time / NULLIF(wait_count, 0)) / 1000000, 3) AS avg_wait_time_ms
FROM wait_events_aggregated
ORDER BY total_wait_time_ms DESC
LIMIT 100;
```

### Metrics Retrieved

| Metric | MySQL Source | Oracle Equivalent | New Relic MySQL | Datadog MySQL |
|--------|--------------|-------------------|-----------------|---------------|
| `collection_timestamp` | `UTC_TIMESTAMP()` | `SYSTIMESTAMP` | `collection_timestamp` | N/A |
| `database_name` | `CURRENT_SCHEMA` | `schema_name` | `database_name` | `processlist_db` |
| `query_id` | `DIGEST` | `sql_id` | `query_id` | `digest` |
| `wait_category` | `CASE WHEN EVENT_NAME...` | `wait_class` | `wait_category` | N/A |
| `wait_event_name` | `EVENT_NAME` | `event` | `wait_event_name` | `event_name` |
| `total_wait_time_ms` | `SUM(TIMER_WAIT)` | `time_waited_micro` | `total_wait_time_ms` | `timer_wait` |
| `wait_event_count` | `COUNT(*)` | `total_waits` | `wait_event_count` | N/A |
| `avg_wait_time_ms` | `total_wait_time/wait_count` | `average_wait` | `avg_wait_time_ms` | N/A |

### Implementation Notes

```go
func GetWaitEventsSQL() string {
    return `
        WITH wait_events_aggregated AS (
            SELECT
                w.THREAD_ID,
                esc.DIGEST AS query_id,
                esc.CURRENT_SCHEMA AS database_name,
                CASE
                    WHEN w.EVENT_NAME LIKE 'wait/io/file%%' THEN 'File I/O'
                    WHEN w.EVENT_NAME LIKE 'wait/io/socket%%' THEN 'Network I/O'
                    WHEN w.EVENT_NAME LIKE 'wait/io/table%%' THEN 'Table I/O'
                    WHEN w.EVENT_NAME LIKE 'wait/lock%%' THEN 'Lock'
                    WHEN w.EVENT_NAME LIKE 'wait/synch%%' THEN 'Synchronization'
                    ELSE 'Other'
                END AS wait_category,
                w.EVENT_NAME AS wait_event_name,
                SUM(w.TIMER_WAIT) AS total_wait_time,
                COUNT(*) AS wait_count
            FROM performance_schema.events_waits_current w
            INNER JOIN performance_schema.events_statements_current esc 
                ON w.THREAD_ID = esc.THREAD_ID
            WHERE w.EVENT_NAME != 'idle'
              AND esc.DIGEST IS NOT NULL
            GROUP BY w.THREAD_ID, esc.DIGEST, esc.CURRENT_SCHEMA, wait_category, w.EVENT_NAME
        )
        SELECT
            UTC_TIMESTAMP() AS collection_timestamp,
            database_name,
            query_id,
            wait_category,
            wait_event_name,
            ROUND(total_wait_time / 1000000, 3) AS total_wait_time_ms,
            wait_count AS wait_event_count,
            ROUND((total_wait_time / NULLIF(wait_count, 0)) / 1000000, 3) AS avg_wait_time_ms
        FROM wait_events_aggregated
        ORDER BY total_wait_time_ms DESC
        LIMIT 100`
}
```

**Key Differences from Oracle**:
- **Wait classification**: MySQL uses EVENT_NAME patterns vs Oracle's predefined `wait_class`
- **Granularity**: MySQL provides real-time current waits vs Oracle's ASH sampling
- **Categories**: MySQL wait categories map to Oracle wait classes (I/O, Lock, Synchronization)

---

## Query 3: Blocking Sessions

**Purpose**: Identify blocked transactions and their blockers

**Oracle Equivalent**: `V$SESSION.BLOCKING_SESSION` query in Oracle receiver

**Collection Interval**: 10 seconds

### MySQL Query

```sql
SELECT
    UTC_TIMESTAMP() AS collection_timestamp,
    
    -- Blocked transaction
    r.trx_id AS blocked_txn_id,
    r.trx_mysql_thread_id AS blocked_session_id,
    wt.PROCESSLIST_USER AS blocked_user,
    wt.PROCESSLIST_HOST AS blocked_host,
    wt.PROCESSLIST_DB AS database_name,
    r.trx_query AS blocked_query,
    DATE_FORMAT(CONVERT_TZ(r.trx_started, @@session.time_zone, '+00:00'), '%Y-%m-%dT%H:%i:%sZ') AS blocked_txn_start_time,
    TIMESTAMPDIFF(SECOND, r.trx_wait_started, UTC_TIMESTAMP()) AS blocked_wait_duration_seconds,
    
    -- Blocking transaction
    b.trx_id AS blocking_txn_id,
    b.trx_mysql_thread_id AS blocking_session_id,
    bt.PROCESSLIST_USER AS blocking_user,
    bt.PROCESSLIST_HOST AS blocking_host,
    b.trx_query AS blocking_query,
    DATE_FORMAT(CONVERT_TZ(b.trx_started, @@session.time_zone, '+00:00'), '%Y-%m-%dT%H:%i:%sZ') AS blocking_txn_start_time,
    
    -- Lock details
    w.requesting_engine_lock_id,
    w.blocking_engine_lock_id,
    dl_blocked.OBJECT_SCHEMA AS locked_schema,
    dl_blocked.OBJECT_NAME AS locked_table,
    dl_blocked.INDEX_NAME AS locked_index,
    dl_blocked.LOCK_TYPE,
    dl_blocked.LOCK_MODE AS blocked_lock_mode,
    dl_blocking.LOCK_MODE AS blocking_lock_mode
    
FROM performance_schema.data_lock_waits w

-- Blocked transaction
INNER JOIN information_schema.innodb_trx r 
    ON r.trx_id = w.requesting_engine_transaction_id
INNER JOIN performance_schema.threads wt 
    ON r.trx_mysql_thread_id = wt.PROCESSLIST_ID

-- Blocking transaction
INNER JOIN information_schema.innodb_trx b 
    ON b.trx_id = w.blocking_engine_transaction_id
INNER JOIN performance_schema.threads bt 
    ON b.trx_mysql_thread_id = bt.PROCESSLIST_ID

-- Lock details
LEFT JOIN performance_schema.data_locks dl_blocked 
    ON w.requesting_engine_lock_id = dl_blocked.ENGINE_LOCK_ID
LEFT JOIN performance_schema.data_locks dl_blocking 
    ON w.blocking_engine_lock_id = dl_blocking.ENGINE_LOCK_ID

ORDER BY blocked_wait_duration_seconds DESC
LIMIT 50;
```

### Metrics Retrieved

| Metric | MySQL Source | Oracle Equivalent | New Relic MySQL | Datadog MySQL |
|--------|--------------|-------------------|-----------------|---------------|
| `collection_timestamp` | `UTC_TIMESTAMP()` | `SYSTIMESTAMP` | `collection_timestamp` | N/A |
| `blocked_txn_id` | `trx_id` | `xid` | `blocked_txn_id` | N/A |
| `blocked_session_id` | `trx_mysql_thread_id` | `sid` | `blocked_thread_id` | `processlist_id` |
| `blocked_user` | `PROCESSLIST_USER` | `username` | N/A | `processlist_user` |
| `blocked_host` | `PROCESSLIST_HOST` | `machine` | `blocked_host` | `processlist_host` |
| `database_name` | `PROCESSLIST_DB` | `schema_name` | `database_name` | `processlist_db` |
| `blocked_query` | `trx_query` | `sql_text` | `blocked_query` | N/A |
| `blocked_wait_duration_seconds` | `TIMESTAMPDIFF(SECOND, trx_wait_started, NOW)` | `seconds_in_wait` | N/A | N/A |
| `blocking_txn_id` | `blocking trx_id` | `blocking_xid` | `blocking_txn_id` | N/A |
| `blocking_session_id` | `blocking trx_mysql_thread_id` | `blocking_session` | `blocking_thread_id` | `blocking_processlist_id` |
| `blocking_user` | `blocking PROCESSLIST_USER` | `blocking_username` | N/A | N/A |
| `blocking_query` | `blocking trx_query` | `blocking_sql_text` | `blocking_query` | N/A |
| `locked_table` | `OBJECT_NAME` | `object_name` | N/A | N/A |
| `lock_type` | `LOCK_TYPE` | `lock_type` | N/A | N/A |

### Implementation Notes

```go
func GetBlockingSessionsSQL() string {
    return `
        SELECT
            UTC_TIMESTAMP() AS collection_timestamp,
            r.trx_id AS blocked_txn_id,
            r.trx_mysql_thread_id AS blocked_session_id,
            wt.PROCESSLIST_USER AS blocked_user,
            wt.PROCESSLIST_HOST AS blocked_host,
            wt.PROCESSLIST_DB AS database_name,
            r.trx_query AS blocked_query,
            DATE_FORMAT(CONVERT_TZ(r.trx_started, @@session.time_zone, '+00:00'), '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS blocked_txn_start_time,
            TIMESTAMPDIFF(SECOND, r.trx_wait_started, UTC_TIMESTAMP()) AS blocked_wait_duration_seconds,
            b.trx_id AS blocking_txn_id,
            b.trx_mysql_thread_id AS blocking_session_id,
            bt.PROCESSLIST_USER AS blocking_user,
            bt.PROCESSLIST_HOST AS blocking_host,
            b.trx_query AS blocking_query,
            DATE_FORMAT(CONVERT_TZ(b.trx_started, @@session.time_zone, '+00:00'), '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS blocking_txn_start_time,
            w.requesting_engine_lock_id,
            w.blocking_engine_lock_id,
            dl_blocked.OBJECT_SCHEMA AS locked_schema,
            dl_blocked.OBJECT_NAME AS locked_table,
            dl_blocked.INDEX_NAME AS locked_index,
            dl_blocked.LOCK_TYPE,
            dl_blocked.LOCK_MODE AS blocked_lock_mode,
            dl_blocking.LOCK_MODE AS blocking_lock_mode
        FROM performance_schema.data_lock_waits w
        INNER JOIN information_schema.innodb_trx r 
            ON r.trx_id = w.requesting_engine_transaction_id
        INNER JOIN performance_schema.threads wt 
            ON r.trx_mysql_thread_id = wt.PROCESSLIST_ID
        INNER JOIN information_schema.innodb_trx b 
            ON b.trx_id = w.blocking_engine_transaction_id
        INNER JOIN performance_schema.threads bt 
            ON b.trx_mysql_thread_id = bt.PROCESSLIST_ID
        LEFT JOIN performance_schema.data_locks dl_blocked 
            ON w.requesting_engine_lock_id = dl_blocked.ENGINE_LOCK_ID
        LEFT JOIN performance_schema.data_locks dl_blocking 
            ON w.blocking_engine_lock_id = dl_blocking.ENGINE_LOCK_ID
        ORDER BY blocked_wait_duration_seconds DESC
        LIMIT 50`
}
```

**Key Differences from Oracle**:
- **Lock tables**: MySQL uses `data_lock_waits` + `innodb_trx` vs Oracle's `V$LOCK` + `V$SESSION`
- **Wait duration**: MySQL provides explicit `trx_wait_started` timestamp vs Oracle's `seconds_in_wait`
- **Lock modes**: MySQL shows both blocked and blocking lock modes for conflict analysis

---

## Performance Schema Configuration

### Required Settings

```sql
-- Enable Performance Schema (add to my.cnf)
[mysqld]
performance_schema = ON

-- Statement instrumentation
UPDATE performance_schema.setup_instruments 
SET ENABLED = 'YES', TIMED = 'YES' 
WHERE NAME LIKE 'statement/%';

-- Wait event instrumentation
UPDATE performance_schema.setup_instruments 
SET ENABLED = 'YES', TIMED = 'YES' 
WHERE NAME LIKE 'wait/%';

-- Enable statement consumers
UPDATE performance_schema.setup_consumers 
SET ENABLED = 'YES' 
WHERE NAME LIKE '%statements%';

-- Enable wait consumers
UPDATE performance_schema.setup_consumers 
SET ENABLED = 'YES' 
WHERE NAME LIKE '%waits%';

-- Verify configuration
SELECT * FROM performance_schema.setup_instruments 
WHERE NAME LIKE 'statement/%' OR NAME LIKE 'wait/%';
```

### MySQL 8.0+ Requirements

- **Minimum Version**: MySQL 8.0.13 (for `data_lock_waits`)
- **Memory**: Allocate sufficient memory for Performance Schema tables
- **InnoDB**: Required for transaction and lock monitoring

---

## Integration Comparison Summary

### Query Pattern Differences

| Feature | New Relic Oracle | New Relic MySQL (This Doc) | Datadog MySQL | Notes |
|---------|------------------|----------------------------|---------------|-------|
| **Slow Queries** | `V$SQLAREA` aggregates | `events_statements_summary_by_digest` | Same as NR MySQL | Both cumulative, need delta |
| **Wait Events** | `V$SESSION_WAIT` current | `events_waits_current` CTE | Join on `events_waits_current` | MySQL more granular |
| **Blocking** | `V$SESSION.BLOCKING_SESSION` | `data_lock_waits` + `innodb_trx` | `data_lock_waits` join | Similar approach |
| **Time Units** | Microseconds | Picoseconds (÷1000000 for ms) | Picoseconds | MySQL higher precision |
| **Filtering** | Username exclusion | Schema exclusion | Schema exclusion | Different system account patterns |
| **Timezone** | `SYSTIMESTAMP` | `UTC_TIMESTAMP()` + `CONVERT_TZ` | Direct timestamps | MySQL needs TZ conversion |

### Metric Mapping

| Oracle Metric | MySQL Equivalent | Conversion |
|---------------|------------------|------------|
| `cpu_time` | `SUM_CPU_TIME` | Oracle µs → MySQL ps (÷1000000) |
| `elapsed_time` | `SUM_TIMER_WAIT` | Oracle µs → MySQL ps (÷1000000) |
| `disk_reads` | `SUM_ROWS_EXAMINED` | Logical reads ≈ rows examined |
| `direct_writes` | `SUM_ROWS_AFFECTED` | Physical writes ≈ rows affected |
| `buffer_gets` | `SUM_ROWS_EXAMINED` | Buffer gets ≈ rows examined |
| `concurrency_wait_time` | `SUM_LOCK_TIME` | Lock wait time |
| `wait_class` | `wait_category` (derived) | Pattern matching on EVENT_NAME |
| `blocking_session` | `blocking_session_id` | Direct mapping |

---

## Usage Example

```go
package newrelicmysqlreceiver

import (
    "database/sql"
    "fmt"
    "time"
)

type MySQLQueryMonitor struct {
    db               *sql.DB
    intervalSeconds  int
}

func (m *MySQLQueryMonitor) CollectSlowQueries() ([]SlowQueryMetric, error) {
    query := GetSlowQueriesSQL(m.intervalSeconds)
    rows, err := m.db.Query(query)
    if err != nil {
        return nil, fmt.Errorf("failed to execute slow queries: %w", err)
    }
    defer rows.Close()
    
    var metrics []SlowQueryMetric
    for rows.Next() {
        var metric SlowQueryMetric
        err := rows.Scan(
            &metric.CollectionTimestamp,
            &metric.DatabaseName,
            &metric.QueryID,
            &metric.QueryText,
            &metric.ExecutionCount,
            &metric.AvgCPUTimeMs,
            &metric.AvgElapsedTimeMs,
            &metric.AvgLockTimeMs,
            &metric.AvgRowsExamined,
            &metric.AvgDiskWrites,
            &metric.TotalElapsedTimeMs,
            &metric.LastActiveTime,
        )
        if err != nil {
            return nil, fmt.Errorf("failed to scan row: %w", err)
        }
        metrics = append(metrics, metric)
    }
    
    return metrics, nil
}

func (m *MySQLQueryMonitor) CollectWaitEvents() ([]WaitEventMetric, error) {
    query := GetWaitEventsSQL()
    // Similar implementation...
}

func (m *MySQLQueryMonitor) CollectBlockingSessions() ([]BlockingSessionMetric, error) {
    query := GetBlockingSessionsSQL()
    // Similar implementation...
}
```

---

## Resources

- **MySQL Performance Schema**: [Documentation](https://dev.mysql.com/doc/refman/8.4/en/performance-schema.html)
- **New Relic Oracle Receiver**: `receiver/newrelicoraclereceiver/queries/qpm_queries.go`
- **New Relic MySQL Integration**: [GitHub - nri-mysql](https://github.com/newrelic/nri-mysql/blob/master/src/query-performance-monitoring/utils/queries.go)
- **Datadog MySQL Integration**: [GitHub - integrations-core](https://github.com/DataDog/integrations-core/tree/master/mysql)
