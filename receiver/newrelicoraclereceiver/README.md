# New Relic Oracle Receiver

## Overview

The New Relic Oracle Receiver is a comprehensive OpenTelemetry receiver that collects extensive Oracle database metrics, performance data, and telemetry for monitoring Oracle database health, performance, and resource utilization.

## Features

This receiver collects comprehensive Oracle database metrics across multiple categories:

### Core Database Metrics
- **Connection Metrics**: Sessions, active/inactive connections, logons, resource limits
- **Memory Metrics**: SGA, PGA, buffer cache, shared pool statistics
- **Disk I/O Metrics**: Physical reads/writes, disk blocks, I/O timing
- **Performance Metrics**: Transactions, executions, parse counts, CPU usage
- **Tablespace Metrics**: Space usage, offline datafiles, capacity

### Query Performance Monitoring
- **Slow Queries**: Execution count, CPU time, disk reads/writes, elapsed time, rows examined
- **SQL Execution Plans**: Detailed execution plan operations with cost, cardinality, and resource estimates
- **Child Cursors**: Per-cursor execution statistics including CPU time, buffer gets, invalidations
- **Wait Events**: Current wait times for active sessions with detailed wait event information
- **Blocking Queries**: Identifies blocking sessions with wait times and blocker information

### Container Database (CDB) & Pluggable Database (PDB)
- **CDB/PDB Status**: Container status, open mode, restricted status
- **PDB Performance**: Per-PDB metrics including CPU usage, transactions, I/O rates
- **Multi-tenancy Support**: Full support for Oracle 12c+ multitenant architecture

### Oracle RAC (Real Application Clusters)
- **ASM Disk Groups**: Capacity, free space, offline disks
- **Cluster Wait Events**: RAC-specific wait times and event tracking
- **Instance Status**: Per-instance health, uptime, database status
- **Service Configuration**: Service failover, load balancing, transaction guard

### Database Configuration & Health
- **Version Information**: Database version, edition, platform details
- **Data Guard**: Database role, protection mode, replication status
- **System Statistics**: Comprehensive metrics from `GV$SYSMETRIC`

## Configuration

### Basic Configuration

```yaml
receivers:
  newrelicoracledb:
    endpoint: "hostname:1521"
    username: "oracle_user"
    password: "oracle_password"
    service: "XE"
    collection_interval: 60s
```

### Advanced Configuration

```yaml
receivers:
  newrelicoracledb:
    endpoint: "hostname:1521"
    username: "oracle_user"
    password: "oracle_password"
    service: "XE"
    collection_interval: 60s
    initial_delay: 10s
    timeout: 30s
    max_open_connections: 5
    disable_connection_pool: false
    
    # Query Performance Monitoring
    enable_query_monitoring: true
    enable_interval_based_averaging: true
    query_monitoring_response_time_threshold: 0
    query_monitoring_count_threshold: 49
    query_monitoring_interval_seconds: 30
    interval_calculator_cache_ttl_minutes: 15
    
    # PDB Configuration
    pdb_services: ["ALL"]
    
    # Enable/disable specific scrapers (optional, all true by default)
    # enable_session_scraper: true
    # enable_tablespace_scraper: true
    # enable_core_scraper: true
    # enable_pdb_scraper: true
    # enable_system_scraper: true
    # enable_connection_scraper: true
    # enable_container_scraper: true
    # enable_rac_scraper: true
    # enable_database_info_scraper: true
```

### Configuration Parameters

**Note**: The receiver supports two connection methods:
1. **Individual parameters** (recommended): `endpoint`, `username`, `password`, `service`
2. **Connection string**: `datasource` with oracle:// URL format

You must provide either `datasource` OR all individual parameters.

#### Connection Settings
| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| `endpoint` | Oracle database host and port (host:port) | No* | |
| `username` | Oracle database username | No* | |
| `password` | Oracle database password (supports ${env:VAR} syntax) | No* | |
| `service` | Oracle service name or SID | No* | |
| `datasource` | Alternative: Complete connection string (`oracle://user:pass@host:port/service`) | No* | |

*Either all of `endpoint`, `username`, `password`, `service` OR `datasource` must be provided.

#### Collection Settings
| Parameter | Description | Default |
|-----------|-------------|---------|
| `collection_interval` | Interval between metric collections | 60s |
| `initial_delay` | Delay before first collection | 1s |
| `timeout` | Timeout for scrape operations | 30s |
| `max_open_connections` | Maximum database connections | 5 |
| `disable_connection_pool` | Disable connection pooling | false |

#### Query Performance Monitoring Settings
| Parameter | Description | Default |
|-----------|-------------|---------|
| `enable_query_monitoring` | Enable query performance monitoring (slow queries, execution plans, child cursors) | false |
| `enable_interval_based_averaging` | Calculate interval-based (delta) metrics for query performance | false |
| `query_monitoring_response_time_threshold` | Minimum response time in seconds to report slow queries (0 = all queries) | 0 |
| `query_monitoring_count_threshold` | Minimum execution count to report queries | 49 |
| `query_monitoring_interval_seconds` | Collection interval for query monitoring scrapers | 30 |
| `interval_calculator_cache_ttl_minutes` | Cache TTL for interval calculations | 15 |

#### PDB Configuration
| Parameter | Description | Default |
|-----------|-------------|---------|
| `pdb_services` | List of PDB service names to monitor, or ["ALL"] for all PDBs | [] |

#### Scraper Control (Optional)
| Parameter | Description | Default |
|-----------|-------------|---------|
| `enable_session_scraper` | Enable session metrics collection | true |
| `enable_tablespace_scraper` | Enable tablespace metrics collection | true |
| `enable_core_scraper` | Enable core database metrics collection | true |
| `enable_pdb_scraper` | Enable PDB metrics collection | true |
| `enable_system_scraper` | Enable system metrics collection | true |
| `enable_connection_scraper` | Enable connection metrics collection | true |
| `enable_container_scraper` | Enable container/CDB metrics collection | true |
| `enable_rac_scraper` | Enable RAC metrics collection | true |
| `enable_database_info_scraper` | Enable database version/info collection | true |

## Prerequisites

### Oracle Database
- Supported versions: Oracle 11g, 12c, 19c, 21c, 23c
- Supports Standard Edition and Enterprise Edition
- Supports non-CDB and CDB/PDB architectures
- Oracle RAC monitoring requires appropriate cluster configuration

### Required Database Permissions

#### For CDB/PDB Monitoring (Oracle 12c+)

Create a common user with `C##` prefix and grant all required permissions:

```sql
CREATE USER c##<YOUR_DB_USERNAME> IDENTIFIED BY <YOUR_PASSWORD> CONTAINER=ALL;

-- Session and connection grants
GRANT CREATE SESSION TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SET CONTAINER TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT CONNECT TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- Performance and session monitoring views
GRANT SELECT ON SYS.V_$SESSION TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.V_$SYSSTAT TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.V_$SESSTAT TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.V_$STATNAME TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.V_$SYSTEM_EVENT TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- Shared server and dispatcher views
GRANT SELECT ON SYS.V_$SHARED_SERVER TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.V_$DISPATCHER TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.V_$CIRCUIT TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- Resource and locking views
GRANT SELECT ON SYS.V_$RESOURCE_LIMIT TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.V_$LOCK TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- Database configuration and parameters
GRANT SELECT ON SYS.V_$DATABASE TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.V_$PARAMETER TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- SQL performance and execution plans
GRANT SELECT ON SYS.V_$SQLAREA TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.V_$SQL TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.V_$SQL_PLAN TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- PDB views
GRANT SELECT ON SYS.V_$PDBS TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- Global views for RAC and multi-instance monitoring
GRANT SELECT ON SYS.GV_$INSTANCE TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.GV_$SGA TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.GV_$SESSTAT TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.GV_$STATNAME TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.GV_$SYSSTAT TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.GV_$SQLAREA TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- Cache and memory views
GRANT SELECT ON SYS.GV_$LIBRARYCACHE TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.GV_$ROWCACHE TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.GV_$PGASTAT TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- Container and PDB views
GRANT SELECT ON SYS.GV_$CONTAINERS TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.GV_$PDBS TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- System metrics and statistics
GRANT SELECT ON SYS.GV_$CON_SYSMETRIC TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.GV_$SYSMETRIC TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- File and I/O statistics
GRANT SELECT ON SYS.GV_$FILESTAT TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- Wait events and session monitoring
GRANT SELECT ON SYS.GV_$SYSTEM_EVENT TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.GV_$ACTIVE_SERVICES TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.GV_$SESSION TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON SYS.GV_$SESSION_WAIT TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- Rollback segment statistics
GRANT SELECT ON SYS.GV_$ROLLSTAT TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- DBA views for objects and tablespaces
GRANT SELECT ON DBA_OBJECTS TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON DBA_TABLESPACES TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON DBA_TABLESPACE_USAGE_METRICS TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON DBA_DATA_FILES TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON DBA_USERS TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- CDB views for multi-tenant monitoring
GRANT SELECT ON CDB_SERVICES TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON CDB_DATA_FILES TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON CDB_TABLESPACE_USAGE_METRICS TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON CDB_USERS TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON CDB_PDBS TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;

-- General access views
GRANT SELECT ON ALL_USERS TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON ALL_VIEWS TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
GRANT SELECT ON GLOBAL_NAME TO c##<YOUR_DB_USERNAME> CONTAINER=ALL;
```

#### For Non-CDB or PDB-Specific Monitoring (Oracle 11g/12c Standard)

For non-container databases or when connecting directly to a PDB, use a regular user without the `C##` prefix:

```sql
CREATE USER <YOUR_DB_USERNAME> IDENTIFIED BY <YOUR_PASSWORD>;

-- Session and connection grants
GRANT CREATE SESSION TO <YOUR_DB_USERNAME>;
GRANT CONNECT TO <YOUR_DB_USERNAME>;

-- Grant SELECT on the same views as above, but without CONTAINER=ALL
-- Example:
GRANT SELECT ON SYS.V_$SESSION TO <YOUR_DB_USERNAME>;
GRANT SELECT ON SYS.V_$SYSSTAT TO <YOUR_DB_USERNAME>;
-- ... (repeat all SELECT grants from above without CONTAINER=ALL)
```

## Metrics

This receiver emits 100+ metrics across multiple categories. Key metrics include:

### Connection Metrics
- `newrelicoracledb.connection.total_sessions`
- `newrelicoracledb.connection.active_sessions`
- `newrelicoracledb.connection.sessions_by_status`

### Memory Metrics
- `newrelicoracledb.memory.pga_in_use_bytes`
- `newrelicoracledb.memory.pga_allocated_bytes`
- `newrelicoracledb.memory.sga_uga_total_bytes`

### Disk I/O Metrics
- `newrelicoracledb.disk.reads`
- `newrelicoracledb.disk.writes`
- `newrelicoracledb.disk.blocks_read`

### Performance Metrics
- `newrelicoracledb.system.transactions_per_second`
- `newrelicoracledb.system.executions_per_second`
- `newrelicoracledb.system.cpu_usage_per_second`

### Query Performance
- `newrelicoracledb.slow_queries.avg_elapsed_time`
- `newrelicoracledb.slow_queries.execution_count`
- `newrelicoracledb.execution_plan` (with detailed plan steps)
- `newrelicoracledb.child_cursors.cpu_time`

### Wait Events & Blocking
- `newrelicoracledb.wait_events.current_wait_time_ms`
- `newrelicoracledb.blocking_queries.wait_time_ms`

### Tablespace Metrics
- `newrelicoracledb.tablespace.space_used_percentage`
- `newrelicoracledb.tablespace.space_consumed_bytes`

### PDB Metrics (12c+)
- `newrelicoracledb.pdb.cpu_usage_per_second`
- `newrelicoracledb.pdb.transactions_per_second`
- `newrelicoracledb.pdb.session_count`

### RAC Metrics
- `newrelicoracledb.rac.instance.status`
- `newrelicoracledb.rac.wait_time`
- `newrelicoracledb.asm.diskgroup.total_mb`

For a complete list of metrics, see [metadata.yaml](metadata.yaml).

## Resource Attributes

- `host.address`: IP address or hostname of the Oracle server
- `host.port`: Port number of the Oracle server
- `service.name`: Oracle service name
- `database_name`: Oracle database name
- `instance.id`: Oracle instance ID

## Example Configurations

### Complete Production Configuration

This example shows a production-ready configuration with separate receivers for CDB infrastructure monitoring and PDB application monitoring, including query performance tracking and metrics-to-logs conversion:

```yaml
receivers:
  # ============================================
  # Receiver 1: CDB Infrastructure Monitoring
  # ============================================
  newrelicoracledb/cdb:
    endpoint: "oraclehost:1521"
    username: "c##monitor"
    password: "${env:ORACLE_PASSWORD}"
    service: "ORCL"
    collection_interval: 45s
    timeout: 45s
    # Query monitoring disabled for CDB - focus on infrastructure metrics
    enable_query_monitoring: false
    enable_interval_based_averaging: false
    interval_calculator_cache_ttl_minutes: 15
    query_monitoring_interval_seconds: 30

  # ============================================
  # Receiver 2: PDB Application Monitoring
  # ============================================
  newrelicoracledb/pdbs:
    endpoint: "oraclehost:1521"
    username: "monitor"
    password: "${env:ORACLE_PASSWORD}"
    service: "CDB"
    collection_interval: 45s
    timeout: 45s
    # Query monitoring enabled for PDBs - track application performance
    enable_query_monitoring: true
    enable_interval_based_averaging: true
    query_monitoring_response_time_threshold: 0
    query_monitoring_count_threshold: 49
    interval_calculator_cache_ttl_minutes: 10
    query_monitoring_interval_seconds: 30
    # Monitor all PDBs accessible through this service
    pdb_services: ["ALL"]

processors:
  # Transform to clear description and unit fields (optional, reduces data size)
  transform/clear_metadata:
    metric_statements:
      - context: metric
        statements:
          - set(metric.description, "")
          - set(metric.unit, "")

  # Filter to only include execution plan and query details metrics (for logs conversion)
  filter/exec_plan_and_query_details_include:
    metrics:
      include:
        match_type: strict
        metric_names:
          - newrelicoracledb.execution_plan
          - newrelicoracledb.slow_queries.query_details

  # Filter to exclude execution plan and query details metrics (from main metrics pipeline)
  filter/exec_plan_and_query_details_exclude:
    metrics:
      exclude:
        match_type: strict
        metric_names:
          - newrelicoracledb.execution_plan
          - newrelicoracledb.slow_queries.query_details

connectors:
  # Convert metrics to logs for execution plans and query details
  metricsaslogs:
    include_resource_attributes: true
    include_scope_info: true

exporters:
  debug:
    verbosity: detailed
  otlp:
    endpoint: "https://otlp.nr-data.net:4317"
    headers:
      api-key: "${env:NEW_RELIC_LICENSE_KEY}"
    compression: gzip

service:
  pipelines:
    # Main metrics pipeline - send all metrics EXCEPT execution plan and query details
    metrics:
      receivers: [newrelicoracledb/cdb, newrelicoracledb/pdbs]
      processors: [transform/clear_metadata, filter/exec_plan_and_query_details_exclude]
      exporters: [otlp]

    # Metrics to logs pipeline - convert execution plan and query details metrics to logs
    metrics/exec_plan_and_query_details_to_logs:
      receivers: [newrelicoracledb/cdb, newrelicoracledb/pdbs]
      processors: [filter/exec_plan_and_query_details_include]
      exporters: [metricsaslogs]

    # Logs pipeline - receive converted metrics as logs
    logs/newrelicoracledb:
      receivers: [metricsaslogs]
      exporters: [otlp]

  telemetry:
    logs:
      level: debug
    metrics:
      level: detailed
```

## Troubleshooting

### Connection Issues

**Connection Refused**
- Verify Oracle listener is running: `lsnrctl status`
- Check firewall rules allow traffic on Oracle port (default 1521)
- Verify service name with: `SELECT sys_context('USERENV','SERVICE_NAME') FROM DUAL;`

**Authentication Failed**
- Verify credentials: `sqlplus username/password@service`
- Check password hasn't expired
- Ensure account is not locked: `SELECT username, account_status FROM dba_users WHERE username = 'MONITOR';`

**Permission Denied**
```sql
-- Verify grants
SELECT * FROM dba_sys_privs WHERE grantee = 'MONITOR';
SELECT * FROM dba_tab_privs WHERE grantee = 'MONITOR';
```

### Performance Issues

**High Database Load**
- Reduce `collection_interval` (e.g., from 60s to 300s)
- Decrease `max_open_connections` (e.g., to 2)
- Disable expensive scrapers:
  ```yaml
  enable_query_monitoring: false
  enable_pdb_scraper: false
  ```

**Query Timeouts**
- Increase `timeout` value:
  ```yaml
  timeout: 60s  # or higher for slow systems
  ```
- Check database load: `SELECT * FROM v$session WHERE status = 'ACTIVE';`

**Memory Issues**
- Enable connection pooling (ensure `disable_connection_pool: false`)
- Reduce `max_open_connections`

### Debugging

Enable debug logging to see detailed receiver operation:

```yaml
service:
  telemetry:
    logs:
      level: debug
    metrics:
      level: detailed

exporters:
  debug:
    verbosity: detailed  # Add debug exporter to see all collected metrics

service:
  pipelines:
    metrics:
      receivers: [newrelicoracledb/cdb]
      exporters: [debug]  # Use debug exporter instead of or alongside OTLP
```

Check collector logs for error messages:
```bash
# Look for connection errors
grep "newrelicoraclereceiver" collector.log | grep -i error

# Check scraper execution times
grep "scraper completed" collector.log

# View all collected metrics in debug output
grep "Metric #" collector.log

# Check query monitoring activity
grep "query_monitoring" collector.log
```

### Common Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| `ORA-01017: invalid username/password` | Wrong credentials | Verify username and password |
| `ORA-12514: TNS:listener does not currently know of service` | Wrong service name | Check tnsnames.ora or service name |
| `ORA-00942: table or view does not exist` | Missing permissions | Grant SELECT on required views |
| `ORA-28000: the account is locked` | Account locked | Unlock: `ALTER USER monitor ACCOUNT UNLOCK;` |
| `context deadline exceeded` | Query timeout | Increase timeout or reduce collection frequency |
| `failed to connect to database` | Network/firewall issue | Check connectivity and firewall rules |

## Architecture Notes

### Query Performance Monitoring Pipeline

The receiver uses a dual-pipeline approach for query performance data:

1. **Metrics Pipeline**: Collects numeric metrics (execution counts, CPU time, wait times)
2. **Logs Pipeline**: Converts high-cardinality data (SQL text, execution plans) to logs via `metricsaslogs` connector

This architecture prevents cardinality explosion in metrics while preserving full query details in logs.

### Interval-Based Averaging

When `enable_interval_based_averaging: true`:
- Tracks previous scrape values
- Calculates delta metrics showing changes since last collection
- Provides real-time visibility into query performance changes
- Example: `newrelicoracledb.slow_queries.interval_execution_count` shows new executions since last scrape

### PDB Services Configuration

- `pdb_services: ["ALL"]`: Monitors all PDBs accessible through the connected service
- `pdb_services: ["PDB1", "PDB2"]`: Monitors only specified PDBs
- `pdb_services: []`: No PDB-specific filtering (default)

## Configuration Best Practices

1. **Separate CDB and PDB Monitoring**: Use dedicated receivers for CDB (infrastructure) and PDB (application) monitoring
2. **Query Monitoring Placement**: Enable query monitoring only on PDB receivers where application queries execute
3. **Interval-Based Averaging**: Enable `enable_interval_based_averaging` to see delta metrics showing changes since last scrape
4. **Metrics-to-Logs Conversion**: Use the `metricsaslogs` connector to convert high-cardinality metrics (execution plans, query details) to logs
5. **Filter Processors**: Use filters to route different metric types to appropriate pipelines
6. **Environment Variables**: Use `${env:VAR}` syntax for sensitive values like passwords and API keys
7. **Collection Intervals**: 
   - CDB infrastructure: 45-60s
   - Query monitoring: 30-45s
   - High-load systems: 120s or higher
8. **Connection Pooling**: Keep `disable_connection_pool: false` (default) for better performance
9. **Scraper Selection**: Disable unnecessary scrapers to reduce database load and collection time

## Contributing

This receiver is part of the OpenTelemetry Collector Contrib repository. For contribution guidelines, see [CONTRIBUTING.md](../../CONTRIBUTING.md).

## License

Copyright 2025 New Relic Corporation. All rights reserved.

Licensed under the Apache License, Version 2.0
