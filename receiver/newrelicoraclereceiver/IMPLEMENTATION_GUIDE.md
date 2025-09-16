# New Relic Oracle Receiver for OpenTelemetry Collector

## Overview

This receiver (`newrelicoraclereceiver`) provides comprehensive Oracle database monitoring capabilities for the OpenTelemetry Collector, implementing all metrics available in the New Relic Oracle database integration (nri-oracle).

## Features

- **Complete Metric Coverage**: Implements all 100+ metrics from nri-oracle including:
  - SGA (System Global Area) metrics
  - Memory management metrics
  - Disk I/O performance metrics
  - Query execution metrics
  - Database connection metrics
  - Network performance metrics
  - Tablespace usage metrics

- **Oracle Database Support**: Compatible with Oracle 11g, 12c, 18c, 19c, and 21c
- **Flexible Authentication**: Supports username/password authentication
- **Resource Attributes**: Includes instance name, database name, and other identifiers
- **Error Handling**: Robust error handling with detailed logging

## Configuration

### Basic Configuration

```yaml
receivers:
  newrelicoraclereceiver:
    # Option 1: Use data source connection string
    data_source: "oracle://username:password@host:port/service"
    
    # Option 2: Use individual connection parameters
    endpoint: "localhost:1521"
    username: "otel_user"
    password: "otel_password"
    service: "ORCL"
    
    # Collection interval (default: 60s)
    collection_interval: 60s
    
    # Extended metrics (default: false)
    extended_metrics: true
    
    # System metrics source (default: "SYS")
    sys_metrics_source: "SYS"
    
    # Custom metrics configuration
    custom_metrics_config:
      - metric_name: "custom_query_result"
        sql_query: "SELECT COUNT(*) as count FROM user_tables"
        value_column: "count"
        metric_type: "gauge"
```

### Security Configuration

For production environments, use Oracle Wallet or other secure authentication methods:

```yaml
receivers:
  newrelicoraclereceiver:
    data_source: "oracle:///@tnsname"  # Using TNS alias with wallet
```

## Metrics

The receiver exports over 100 metrics across the following categories:

### SGA Metrics
- `newrelic.oracle.sga.hit_ratio` - Buffer cache hit ratio
- `newrelic.oracle.sga.fixed_size_bytes` - Fixed SGA size
- `newrelic.oracle.sga.redo_buffers_bytes` - Redo log buffer size
- And many more...

### Memory Metrics
- `newrelic.oracle.memory.pga_aggregate_target_bytes` - PGA aggregate target
- `newrelic.oracle.memory.pga_in_use_bytes` - PGA memory in use
- `newrelic.oracle.memory.sorts_ratio` - Memory sorts ratio
- And many more...

### Database Performance
- `newrelic.oracle.db.cpu_time_ratio` - CPU time ratio
- `newrelic.oracle.db.sql_service_response_time` - SQL service response time
- `newrelic.oracle.db.session_count` - Active session count
- And many more...

### Disk I/O
- `newrelic.oracle.disk.physical_reads_per_second` - Physical reads per second
- `newrelic.oracle.disk.physical_writes_per_second` - Physical writes per second
- `newrelic.oracle.disk.sort_operations_per_second` - Sort operations per second
- And many more...

### Network
- `newrelic.oracle.network.io_megabytes_per_second` - Network I/O throughput
- `newrelic.oracle.network.traffic_bytes_per_second` - Network traffic
- And many more...

### Tablespace
- `newrelic.oracle.tablespace.space_used_percentage` - Tablespace usage percentage
- `newrelic.oracle.tablespace.space_consumed_bytes` - Space consumed
- `newrelic.oracle.tablespace.is_offline` - Tablespace online status
- And many more...

## Resource Attributes

Each metric includes the following resource attributes:

- `oracle.instance.name` - Oracle instance name
- `oracle.database.name` - Database name
- `oracle.global.name` - Global database name
- `oracle.database.id` - Database ID

## Prerequisites

### Oracle Client Libraries

The receiver requires Oracle Instant Client libraries to be installed:

#### Linux
```bash
# Download and install Oracle Instant Client
wget https://download.oracle.com/otn_software/linux/instantclient/2110000/instantclient-basic-linux.x64-21.10.0.0.0dbru.zip
unzip instantclient-basic-linux.x64-21.10.0.0.0dbru.zip
sudo mv instantclient_21_10 /opt/oracle/
echo "/opt/oracle/instantclient_21_10" | sudo tee /etc/ld.so.conf.d/oracle.conf
sudo ldconfig
```

#### macOS
```bash
# Download and install Oracle Instant Client
# Set environment variables
export DYLD_LIBRARY_PATH=/opt/oracle/instantclient_21_10:$DYLD_LIBRARY_PATH
```

#### Windows
Download and extract Oracle Instant Client, then add the directory to your PATH.

### Database Permissions

The monitoring user needs the following privileges:

```sql
-- Create monitoring user
CREATE USER otel_user IDENTIFIED BY otel_password;

-- Grant necessary privileges
GRANT CONNECT TO otel_user;
GRANT SELECT ON v_$session TO otel_user;
GRANT SELECT ON v_$database TO otel_user;
GRANT SELECT ON v_$instance TO otel_user;
GRANT SELECT ON v_$sysstat TO otel_user;
GRANT SELECT ON v_$system_event TO otel_user;
GRANT SELECT ON v_$sgastat TO otel_user;
GRANT SELECT ON v_$pgastat TO otel_user;
GRANT SELECT ON v_$filestat TO otel_user;
GRANT SELECT ON v_$tempstat TO otel_user;
GRANT SELECT ON v_$datafile TO otel_user;
GRANT SELECT ON v_$tempfile TO otel_user;
GRANT SELECT ON dba_tablespaces TO otel_user;
GRANT SELECT ON dba_data_files TO otel_user;
GRANT SELECT ON dba_temp_files TO otel_user;
GRANT SELECT ON dba_free_space TO otel_user;
GRANT SELECT ON v_$log TO otel_user;
GRANT SELECT ON v_$logfile TO otel_user;
GRANT SELECT ON v_$archived_log TO otel_user;
GRANT SELECT ON v_$asm_diskgroup TO otel_user;
GRANT SELECT ON v_$rollstat TO otel_user;
GRANT SELECT ON v_$undostat TO otel_user;
GRANT SELECT ON v_$sort_segment TO otel_user;
GRANT SELECT ON global_name TO otel_user;

-- For Oracle 12c+ with pluggable databases
GRANT SELECT ON gv_$instance TO otel_user;
GRANT SELECT ON gv_$database TO otel_user;
```

## Example Configuration

```yaml
receivers:
  newrelicoraclereceiver:
    endpoint: "oracle-db.example.com:1521"
    username: "otel_user"
    password: "secure_password"
    service: "PROD"
    collection_interval: 30s
    extended_metrics: true
    
processors:
  batch:
    
exporters:
  logging:
    loglevel: debug
  otlp:
    endpoint: "http://localhost:4317"
    
service:
  pipelines:
    metrics:
      receivers: [newrelicoraclereceiver]
      processors: [batch]
      exporters: [logging, otlp]
```

## Performance Considerations

- **Collection Interval**: Default is 60 seconds. Lower intervals increase database load.
- **Extended Metrics**: When enabled, collects additional detailed metrics but increases overhead.
- **Connection Pooling**: The receiver uses connection pooling to minimize database connections.
- **Query Optimization**: All queries are optimized for minimal performance impact.

## Troubleshooting

### Common Issues

1. **Oracle Client Library Not Found**
   ```
   Error: Cannot locate a 64-bit Oracle Client library
   ```
   Solution: Install Oracle Instant Client and set proper environment variables.

2. **Insufficient Privileges**
   ```
   Error: ORA-00942: table or view does not exist
   ```
   Solution: Grant the necessary SELECT privileges to the monitoring user.

3. **Connection Timeout**
   ```
   Error: ORA-01017: invalid username/password
   ```
   Solution: Verify connection parameters and network connectivity.

4. **TNS Resolution**
   ```
   Error: ORA-12154: TNS:could not resolve the connect identifier
   ```
   Solution: Check TNS configuration or use direct connection string.

### Debug Logging

Enable debug logging to troubleshoot issues:

```yaml
service:
  telemetry:
    logs:
      level: debug
```

## Development and Testing

### Building from Source

```bash
cd receiver/newrelicoraclereceiver
go build .
```

### Running Tests

```bash
go test -v
```

Note: Integration tests require a running Oracle database instance.

### Adding Custom Metrics

To add custom metrics, modify the `metadata.yaml` file and regenerate the metadata:

```bash
go generate ./...
```

## Compatibility

- **OpenTelemetry Collector**: v0.135.0+
- **Go**: 1.21+
- **Oracle Database**: 11g, 12c, 18c, 19c, 21c
- **Operating Systems**: Linux, macOS, Windows

## License

This receiver is licensed under the Apache License 2.0, the same as the OpenTelemetry Collector Contrib project.
