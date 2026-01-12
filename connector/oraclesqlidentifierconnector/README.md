# Oracle SQL Identifier Connector

The Oracle SQL Identifier Connector extracts SQL identifiers from Oracle metrics and forwards them as logs. This eliminates duplicate expensive database queries between metrics and logs pipelines.

## How It Works

1. **Metrics Pipeline**: Oracle receiver collects metrics including SQL identifiers from wait events and slow queries
2. **Connector**: Extracts SQL identifiers from metrics and forwards them as log events  
3. **Logs Pipeline**: Receives SQL identifiers from connector instead of querying the database again

## Configuration

```yaml
connectors:
  oraclesqlidentifierconnector:
    # Optional: customize attribute names (defaults shown)
    sql_id_attribute: "oracle.sql_id"
    child_address_attribute: "oracle.child_address" 
    child_number_attribute: "oracle.child_number"
```

## Pipeline Configuration

```yaml
service:
  pipelines:
    metrics:
      receivers: [newrelicoraclereceiver]
      processors: [batch]
      exporters: [prometheusexporter, oraclesqlidentifierconnector]
    
    logs:
      receivers: [oraclesqlidentifierconnector]
      processors: [batch]
      exporters: [elasticsearchexporter]
```

## Benefits

- **Performance**: Eliminates duplicate expensive Oracle queries
- **Architecture**: Follows OpenTelemetry Collector connector patterns
- **Scalability**: Proper data flow coordination between pipelines
- **Reliability**: Built-in TTL and deduplication

## Metrics Consumed

The connector looks for these Oracle metric names:
- `newrelicoracledb.wait_event.count`
- `newrelicoracledb.wait_event.time`
- `newrelicoracledb.slow_query.count`
- `newrelicoracledb.child_cursor.count`

## Logs Produced

Creates log events with:
- Event Name: `oracle.sql_identifier_extracted`
- Attributes: `sql_id`, `child_address`, `child_number`
- Body: JSON representation of SQL identifier

## Testing

Use the provided test configuration in `testdata/config.yaml` to test the connector with your Oracle receiver setup.
