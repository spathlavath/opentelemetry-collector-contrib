# New Relic MySQL Receiver

The New Relic MySQL receiver is an advanced OpenTelemetry Collector receiver that provides comprehensive monitoring capabilities for MySQL databases, extending beyond standard MySQL monitoring with New Relic-specific enhancements.

## Overview

This receiver integrates advanced monitoring capabilities from the New Relic MySQL integration (`nri-mysql`), including:

- **Query Performance Monitoring**: Detailed analysis of query execution patterns, timing, and resource usage
- **Wait Events Tracking**: Monitor database wait events to identify performance bottlenecks
- **Blocking Sessions Detection**: Real-time detection and analysis of blocking and blocked sessions
- **Enhanced InnoDB Metrics**: Advanced InnoDB buffer pool and data file metrics
- **Replication Monitoring**: Master/slave replication status and lag monitoring
- **Performance Schema Integration**: Deep integration with MySQL's Performance Schema
- **Slow Query Log Analysis**: Analysis of slow query patterns and trends

## Configuration

### Basic Configuration

```yaml
receivers:
  newrelicmysql:
    endpoint: "root:password@tcp(localhost:3306)/mysql"
    username: "monitoring_user"
    password: "secure_password"
    database: "mysql"
```

### Complete Configuration

```yaml
receivers:
  newrelicmysql:
    # Connection configuration
    endpoint: "root:password@tcp(localhost:3306)/mysql"
    username: "monitoring_user"
    password: "secure_password"
    database: "mysql"
    
    # Collection settings
    collection_interval: 60s
    initial_delay: 1s
    timeout: 30s
    
    # Transport configuration
    transport: "tcp"
    connect_timeout: 10s
    read_timeout: 30s
    write_timeout: 30s
    
    # TLS configuration
    tls:
      insecure: false
      insecure_skip_verify: false
      cert_file: "/path/to/client-cert.pem"
      key_file: "/path/to/client-key.pem"
      ca_file: "/path/to/ca.pem"
      server_name: "mysql.example.com"
    
    # Query Performance Monitoring
    query_performance:
      enabled: true
      max_digests: 1000
      digest_text_max_length: 1024
      collection_interval: 60s
      include_query_examples: false
      exclude_system_queries: true
      min_query_time: 1ms
    
    # Wait Events Monitoring
    wait_events:
      enabled: true
      collection_interval: 60s
      max_events: 1000
      event_types: ["io", "lock", "sync"]
    
    # Blocking Sessions Monitoring
    blocking_sessions:
      enabled: true
      collection_interval: 30s
      max_sessions: 100
      min_wait_time: 1s
    
    # InnoDB Enhanced Metrics
    innodb:
      enabled: true
      collection_interval: 60s
      detailed_metrics: true
    
    # Replication Monitoring
    replication:
      enabled: true
      collection_interval: 60s
      monitor_slave_status: true
    
    # Performance Schema Configuration
    performance_schema:
      enabled: true
      collection_interval: 60s
      table_io_stats: true
      index_io_stats: true
      file_io_stats: false
    
    # Slow Query Log Monitoring
    slow_query:
      enabled: true
      collection_interval: 300s
      max_queries: 100
      min_query_time: 1s
    
    # Feature flags
    enabled_features:
      - "query_performance"
      - "wait_events"
      - "blocking_sessions"
      - "innodb"
      - "replication"
      - "performance_schema"
      - "slow_query"
```

## Metrics

The receiver collects the following metrics:

### Core MySQL Metrics

- `mysql.buffer_pool.efficiency`: Buffer pool read efficiency
- `mysql.buffer_pool.pages`: Buffer pool page statistics (total, free, dirty)
- `mysql.buffer_pool.reads`: Buffer pool read operations
- `mysql.commands`: MySQL command execution counts
- `mysql.connection.count`: Connection statistics
- `mysql.handler.reads`: Handler read operation counts
- `mysql.innodb.data_size`: InnoDB data size metrics
- `mysql.innodb.operations`: InnoDB operation counts

### New Relic Enhanced Metrics

- `mysql.newrelic.query.execution_count`: Query execution frequency by digest
- `mysql.newrelic.query.execution_time`: Query execution time by digest
- `mysql.newrelic.query.lock_time`: Query lock time by digest
- `mysql.newrelic.query.rows_sent`: Rows sent by query digest
- `mysql.newrelic.query.rows_examined`: Rows examined by query digest
- `mysql.newrelic.wait_events.time`: Wait event total time by type
- `mysql.newrelic.wait_events.count`: Wait event occurrence count by type
- `mysql.newrelic.blocking_sessions.wait_time`: Blocking session wait time
- `mysql.newrelic.blocking_sessions.count`: Number of blocking sessions
- `mysql.newrelic.replication.slave_lag`: Replication slave lag
- `mysql.newrelic.replication.slave_io_running`: Slave IO thread status
- `mysql.newrelic.replication.slave_sql_running`: Slave SQL thread status
- `mysql.newrelic.slow_queries.count`: Slow query count
- `mysql.newrelic.slow_queries.time_threshold`: Slow query time threshold

## Resource Attributes

The receiver sets the following resource attributes:

- `mysql.instance.endpoint`: MySQL instance endpoint
- `mysql.instance.database`: Database name

## Metric Attributes

Metrics include relevant attributes such as:

- `schema`: Database schema name
- `query_digest`: Query digest/fingerprint
- `event_type`: Wait event type
- `blocking_pid`: Process ID of blocking session
- `blocked_pid`: Process ID of blocked session
- `lock_type`: Type of database lock

## Prerequisites

### Database Permissions

The monitoring user requires the following MySQL permissions:

```sql
-- Core monitoring permissions
GRANT SELECT ON performance_schema.* TO 'monitoring_user'@'%';
GRANT SELECT ON information_schema.* TO 'monitoring_user'@'%';
GRANT PROCESS ON *.* TO 'monitoring_user'@'%';
GRANT REPLICATION CLIENT ON *.* TO 'monitoring_user'@'%';

-- For sys schema (blocking sessions)
GRANT SELECT ON sys.* TO 'monitoring_user'@'%';

-- For global status and variables
GRANT SELECT ON mysql.* TO 'monitoring_user'@'%';
```

### MySQL Configuration

Ensure the following MySQL configuration for optimal monitoring:

```ini
# Enable Performance Schema
performance_schema = ON

# Enable slow query log
slow_query_log = ON
slow_query_log_file = /var/log/mysql/slow.log
long_query_time = 1

# Enable binary logging for replication monitoring
log_bin = mysql-bin
server_id = 1
```

## Performance Considerations

- **Collection Intervals**: Adjust collection intervals based on your monitoring needs and database load
- **Query Digest Limits**: Configure `max_digests` appropriately to balance detail with performance
- **Wait Events**: Monitor only essential event types to reduce overhead
- **Resource Usage**: The receiver uses MySQL's Performance Schema, which has minimal overhead but should be monitored

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify database connectivity and credentials
   - Check firewall rules and network access
   - Ensure MySQL is accepting connections

2. **Permission Errors**
   - Verify the monitoring user has required permissions
   - Check that Performance Schema is enabled
   - Ensure sys schema is available

3. **TLS Certificate Issues**
   - Configure TLS settings appropriately
   - Use `insecure_skip_verify: true` for testing (not recommended for production)

4. **High Resource Usage**
   - Reduce collection frequency
   - Limit the number of query digests
   - Disable unnecessary features

### Debugging

Enable debug logging in the OpenTelemetry Collector configuration:

```yaml
service:
  telemetry:
    logs:
      level: debug
      development: true
```

## Integration Examples

### Complete Pipeline Configuration

```yaml
receivers:
  newrelicmysql:
    endpoint: "monitoring_user:password@tcp(mysql.example.com:3306)/mysql"
    username: "monitoring_user"
    password: "secure_password"
    collection_interval: 30s
    query_performance:
      enabled: true
      max_digests: 500
    wait_events:
      enabled: true
    blocking_sessions:
      enabled: true

processors:
  batch:
    timeout: 1s
    send_batch_size: 1024

exporters:
  newrelic:
    apikey: "your-new-relic-license-key"
    endpoint: "https://metric-api.newrelic.com/metric/v1"

service:
  pipelines:
    metrics:
      receivers: [newrelicmysql]
      processors: [batch]
      exporters: [newrelic]
```

### Multi-Instance Monitoring

```yaml
receivers:
  newrelicmysql/primary:
    endpoint: "user:pass@tcp(mysql-primary:3306)/mysql"
    username: "monitoring_user"
    password: "password"
  
  newrelicmysql/replica:
    endpoint: "user:pass@tcp(mysql-replica:3306)/mysql"
    username: "monitoring_user"
    password: "password"
    replication:
      enabled: true
      monitor_slave_status: true

service:
  pipelines:
    metrics:
      receivers: [newrelicmysql/primary, newrelicmysql/replica]
      processors: [batch]
      exporters: [newrelic]
```

## License

This receiver is part of the OpenTelemetry Collector Contrib project and is licensed under the Apache License 2.0.
