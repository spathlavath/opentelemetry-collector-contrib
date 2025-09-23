# New Relic Oracle Receiver

The New Relic Oracle Receiver is an OpenTelemetry receiver that collects Oracle database metrics and formats them in a way that's compatible with New Relic's monitoring infrastructure.

## Features

This receiver currently collects the following Oracle metrics:

- **Session Count**: Total number of active Oracle database sessions (`newrelicoracledb.sessions.count`)
- **Tablespace Metrics**: Various tablespace-related metrics including space usage and status
- **Locked Accounts**: Count of locked user accounts in the database (`newrelicoracledb.locked_accounts`)
- **Redo Log Wait Metrics**: 
  - Log file parallel write waits (`newrelicoracledb.redo_log.waits`)
  - Log file switch completion waits (`newrelicoracledb.redo_log.log_file_switch`)
  - Log file switch checkpoint incomplete waits (`newrelicoracledb.redo_log.log_file_switch_checkpoint_incomplete`)
  - Log file switch archiving needed waits (`newrelicoracledb.redo_log.log_file_switch_archiving_needed`)
- **SGA Buffer Metrics**:
  - Buffer busy waits (`newrelicoracledb.sga.buffer_busy_waits`)
  - Free buffer waits (`newrelicoracledb.sga.free_buffer_waits`)  
  - Free buffer inspected events (`newrelicoracledb.sga.free_buffer_inspected`)

## Configuration

The receiver supports the following configuration options:

### Basic Configuration

```yaml
receivers:
  newrelicoracledb:
    datasource: "oracle://username:password@hostname:port/service"
    collection_interval: 10s
```

### Alternative Configuration (Individual Parameters)

```yaml
receivers:
  newrelicoracledb:
    endpoint: "hostname:1521"
    username: "oracle_user"
    password: "oracle_password"
    service: "XE"
    collection_interval: 10s
```

### Configuration Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| `datasource` | Complete Oracle connection string | No* | |
| `endpoint` | Oracle database host and port (host:port) | No* | |
| `username` | Oracle database username | No* | |
| `password` | Oracle database password | No* | |
| `service` | Oracle service name | No* | |
| `collection_interval` | How often to collect metrics | No | 10s |

*Either `datasource` OR all of `endpoint`, `username`, `password`, and `service` must be provided.

## Prerequisites

- Oracle database (tested with Oracle 11g, 12c, 19c, and later)
- Oracle user with appropriate permissions to query system views:
  - `SELECT` permission on `v$session`
  - `SELECT` permission on `GV$SYSTEM_EVENT` and `GV$INSTANCE` (for redo log wait metrics)
  - `SELECT` permission on various DBA and system views for tablespace and account metrics

## Metrics

For a complete list of all available metrics, see the [documentation.md](documentation.md) file.

### Key Metrics Overview

#### Core Database Metrics
- `newrelicoracledb.sessions.count`: Active database sessions
- `newrelicoracledb.locked_accounts`: Count of locked user accounts

#### Redo Log Wait Metrics  
- `newrelicoracledb.redo_log.waits`: Log file parallel write waits
- `newrelicoracledb.redo_log.log_file_switch`: Log file switch completion waits
- `newrelicoracledb.redo_log.log_file_switch_checkpoint_incomplete`: Checkpoint incomplete waits
- `newrelicoracledb.redo_log.log_file_switch_archiving_needed`: Archiving needed waits

#### SGA Buffer Metrics
- `newrelicoracledb.sga.buffer_busy_waits`: Buffer busy waits  
- `newrelicoracledb.sga.free_buffer_waits`: Free buffer waits
- `newrelicoracledb.sga.free_buffer_inspected`: Free buffer inspected events

#### Tablespace Metrics
- Various tablespace space usage, status, and health metrics

## Resource Attributes

- `newrelicoracledb.instance.name`: The name of the Oracle instance
- `host.name`: The host name of the Oracle server

## Example Configuration

```yaml
receivers:
  newrelicoracledb:
    datasource: "oracle://myuser:mypassword@localhost:1521/XE"
    collection_interval: 30s
    metrics:
      newrelicoracledb.sessions.count:
        enabled: true

processors:
  batch:

exporters:
  logging:
    loglevel: debug

service:
  pipelines:
    metrics:
      receivers: [newrelicoracledb]
      processors: [batch]
      exporters: [logging]
```

## Troubleshooting

### Common Issues

1. **Connection Refused**: Verify Oracle database is running and accessible from the collector host
2. **Authentication Failed**: Check username and password are correct
3. **Permission Denied**: Ensure the Oracle user has SELECT permissions on system views

### Logs

Enable debug logging to see detailed information about the receiver's operation:

```yaml
service:
  telemetry:
    logs:
      level: debug
```
