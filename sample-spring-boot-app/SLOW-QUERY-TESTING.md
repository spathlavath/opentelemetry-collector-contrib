# Slow Query Testing Guide

## Overview

This application includes a special endpoint designed to simulate long-running, stuck SQL queries for testing query performance monitoring capabilities of the OTEL collector.

## Endpoint Details

### `/api/users/slow-query`

**Method:** GET

**Description:** Executes a SQL query that uses `WAITFOR DELAY '00:05:00'` to pause execution for 5 minutes before returning results. This simulates a stuck or extremely slow query.

**SQL Query:**
```sql
WAITFOR DELAY '00:05:00'; SELECT * FROM users
```

**Duration:** Approximately 5 minutes (300 seconds)

**Purpose:**
- Test active running query detection
- Verify query performance monitoring metrics collection
- Simulate slow query scenarios for APM monitoring

## Testing Methods

### Method 1: Using the Dedicated Test Scripts

#### Linux/Mac:
```bash
./test-slow-query.sh
```

#### Windows:
```batch
test-slow-query.bat
```

These scripts will:
1. Display information about the test
2. Execute the slow query endpoint
3. Provide guidance on what to monitor
4. Wait for the query to complete

### Method 2: Using curl Directly

```bash
# Simple request (will block for 5 minutes)
curl http://localhost:8080/api/users/slow-query

# Run in background
curl http://localhost:8080/api/users/slow-query &

# With verbose output
curl -v http://localhost:8080/api/users/slow-query
```

### Method 3: Using the Main Test Script

The main test script (`test-api.sh` or `test-api.bat`) includes the slow query test as step 11. It will automatically start the query in the background.

```bash
./test-api.sh
```

## What to Monitor

While the slow query is running, you should be able to observe:

### 1. OTEL Collector
- Check collector logs for active query detection
- Verify query performance monitoring metrics are being collected
- Look for the query in active running queries metrics

### 2. SQL Server
Query `sys.dm_exec_requests` to see the active query:
```sql
SELECT
    session_id,
    start_time,
    status,
    command,
    wait_type,
    wait_time,
    SUBSTRING(
        qt.text,
        (er.statement_start_offset/2) + 1,
        ((CASE er.statement_end_offset
            WHEN -1 THEN DATALENGTH(qt.text)
            ELSE er.statement_end_offset
        END - er.statement_start_offset)/2) + 1
    ) AS query_text
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
WHERE session_id > 50
AND text LIKE '%WAITFOR DELAY%';
```

### 3. New Relic APM Dashboard
- Navigate to the APM dashboard
- Look for transactions with high duration (~300 seconds)
- Check transaction traces for the slow query
- Verify database query metrics

### 4. Application Logs
Check the Spring Boot application logs for:
- Incoming request to `/api/users/slow-query`
- SQL execution start
- Query completion after 5 minutes

## Expected Collector Metrics

The OTEL collector should capture this query and report metrics including:

- `sqlserver.query.active.count` - Should show 1 active query
- `sqlserver.query.active.duration` - Should show increasing duration up to ~300 seconds
- `sqlserver.query.active.wait_time` - Wait time associated with the query
- `sqlserver.query.active.cpu_time` - CPU time (should be minimal for WAITFOR DELAY)
- Query text containing "WAITFOR DELAY"
- Session information
- Blocking information (if applicable)

## Cancelling a Running Query

### From the Application:
- Press Ctrl+C in the terminal where curl is running
- Note: This may not immediately cancel the SQL query on the server

### From SQL Server:
```sql
-- Find the session_id
SELECT session_id, start_time, status
FROM sys.dm_exec_requests
WHERE text LIKE '%WAITFOR DELAY%';

-- Kill the session (replace <session_id> with actual ID)
KILL <session_id>;
```

### From the Test Script:
```bash
# Linux/Mac - Find and kill the curl process
ps aux | grep curl
kill <PID>

# Or kill by process group
killall curl
```

## Troubleshooting

### Query Doesn't Appear in Collector
- Verify the collector is running and configured correctly
- Check collector scrape interval (query might complete before next scrape)
- Ensure query performance monitoring is enabled in collector config
- Verify SQL Server permissions for the collector's database user

### Query Times Out
- Default timeout is 5 minutes; ensure client timeout is set higher
- Check SQL Server query timeout settings
- Verify network connectivity between application and SQL Server

### Query Completes Immediately
- Check if `WAITFOR DELAY` is supported in your SQL Server version
- Verify the query is actually executing on SQL Server
- Check application logs for errors

## Integration with CI/CD

For automated testing, you can use a shorter delay:

Modify the repository query annotation:
```java
@Query(value = "WAITFOR DELAY '00:00:30'; SELECT * FROM users", nativeQuery = true)
```

This will reduce the delay to 30 seconds, making it more suitable for automated testing scenarios.

## Notes

- The `WAITFOR DELAY` statement is a T-SQL feature specific to SQL Server
- This endpoint is for testing purposes only and should not be exposed in production
- The query consumes a database connection for its entire duration
- Multiple concurrent calls to this endpoint will consume multiple connections
- Consider your database connection pool size when testing
