# Active Running Queries Test Guide

## Overview

This test simulates a realistic scenario where you have:
1. **1 slow query** (1-minute duration) - Captured in slow query stats
2. **50 active queries** (5-second duration each) - Visible as active running queries

This allows you to test both slow query detection and active query monitoring in the OTEL collector.

---

## Changes Summary

### Query Durations Updated:

| Endpoint | Old Duration | New Duration | Purpose |
|----------|-------------|--------------|---------|
| `/api/users/slow-query` | 5 minutes | 5 minutes | Long-running stuck query |
| `/api/users/slow-sales-query` | 5 minutes | **1 minute** | Slow query for stats |
| `/api/users/active-sales-query` | N/A | **5 seconds** | Active running queries |
| `/api/users/slow-product-query` | 5 minutes | 5 minutes | Product analysis query |

---

## Test Scenarios

### Scenario 1: Single 1-Minute Slow Query

**Purpose:** Test slow query capture in query stats

```bash
# Linux/Mac
curl http://localhost:8080/api/users/slow-sales-query

# Windows
curl http://localhost:8080/api/users/slow-sales-query
```

**Expected Results:**
- Duration: 60 seconds
- Appears in `sys.dm_exec_query_stats` after completion
- Captured by OTEL collector as slow query

---

### Scenario 2: Single 5-Second Active Query

**Purpose:** Test active query monitoring

```bash
# Linux/Mac
curl http://localhost:8080/api/users/active-sales-query

# Windows
curl http://localhost:8080/api/users/active-sales-query
```

**Expected Results:**
- Duration: 5 seconds
- Visible in `sys.dm_exec_requests` while running
- Captured by OTEL collector as active query

---

### Scenario 3: 50 Concurrent Active Queries

**Purpose:** Test concurrent active query monitoring at scale

**Linux/Mac:**
```bash
./test-50-active-queries.sh
```

**Windows:**
```batch
test-50-active-queries.bat
```

**What Happens:**
1. Starts 1 slow query (1 minute)
2. Starts 50 active queries (5 seconds each)
3. All 51 queries run concurrently
4. 50 active queries complete after ~5 seconds
5. 1 slow query completes after ~60 seconds

**Expected Results:**
- Peak concurrent queries: 51
- After 5 seconds: 1 query remaining
- After 60 seconds: All complete
- OTEL collector captures all active queries

---

## Monitoring Active Queries

### Count Active Queries

```sql
SELECT COUNT(*) AS active_count
FROM sys.dm_exec_requests
WHERE text LIKE '%WAITFOR DELAY%';
```

**Expected:** Should show 51 queries at peak, then drop to 1 after 5 seconds

---

### View All Active Queries

```sql
SELECT
    er.session_id,
    er.start_time,
    er.status,
    er.wait_type,
    er.wait_time,
    er.total_elapsed_time / 1000 AS elapsed_seconds,
    SUBSTRING(qt.text, 1, 200) AS query_text
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
WHERE qt.text LIKE '%WAITFOR DELAY%'
ORDER BY er.start_time;
```

**Expected:**
- 1 query with `WAITFOR DELAY '00:01:00'` (slow query)
- 50 queries with `WAITFOR DELAY '00:00:05'` (active queries)

---

### Check Query by Delay Duration

```sql
-- Find 1-minute slow query
SELECT session_id, start_time, total_elapsed_time / 1000 AS elapsed_sec
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
WHERE qt.text LIKE '%00:01:00%';

-- Find 5-second active queries
SELECT session_id, start_time, total_elapsed_time / 1000 AS elapsed_sec
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
WHERE qt.text LIKE '%00:00:05%';
```

---

## OTEL Collector Configuration

Ensure your collector scrape interval is appropriate:

```yaml
receivers:
  newrelicsqlserver:
    collection_interval: 10s  # Scrape every 10 seconds
```

**Recommendations:**
- **For 5-second queries:** Use `collection_interval: 2s` or `5s`
- **For 1-minute queries:** Any interval ≤ 60s works
- **For 5-minute queries:** Any reasonable interval works

---

## What the OTEL Collector Should Capture

### Active Running Queries (sys.dm_exec_requests)

**During test execution:**
- 51 active queries initially
- Each with query text, session info, wait type, duration
- Real-time snapshot of what's executing

### Slow Query Stats (sys.dm_exec_query_stats)

**After queries complete:**
- Query text with `WAITFOR DELAY '00:01:00'`
- Execution count
- Total elapsed time (~60 seconds)
- Execution statistics

---

## Manual Testing Steps

### Step 1: Start the Application

```bash
# Linux/Mac
./start-with-apm.sh

# Windows
start-with-apm.bat
```

### Step 2: Run the 50 Active Queries Test

```bash
# Linux/Mac
./test-50-active-queries.sh

# Windows
test-50-active-queries.bat
```

### Step 3: Monitor in Real-Time

**Open SQL Server Management Studio or use sqlcmd:**

```sql
-- Run this query repeatedly to see active queries
SELECT
    COUNT(*) AS total_active,
    SUM(CASE WHEN qt.text LIKE '%00:01:00%' THEN 1 ELSE 0 END) AS slow_queries_1min,
    SUM(CASE WHEN qt.text LIKE '%00:00:05%' THEN 1 ELSE 0 END) AS active_queries_5sec
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
WHERE qt.text LIKE '%WAITFOR DELAY%';
```

**Expected Output Timeline:**

| Time | Total Active | 1-min Slow | 5-sec Active |
|------|-------------|------------|--------------|
| 0s   | 51          | 1          | 50           |
| 5s   | 1           | 1          | 0            |
| 60s  | 0           | 0          | 0            |

### Step 4: Check OTEL Collector Logs

Look for log entries indicating active queries were captured:
```
Collected X active running queries
Query text: WAITFOR DELAY '00:00:05'...
```

### Step 5: Verify in New Relic

1. Go to New Relic APM Dashboard
2. Check transaction traces
3. Look for transactions with:
   - 60-second duration (slow query)
   - 5-second duration (active queries)

---

## Troubleshooting

### Not All Queries Appear in sys.dm_exec_requests

**Issue:** Only seeing a few queries instead of 51

**Solutions:**
- Check database connection pool size in `application.properties`:
  ```properties
  spring.datasource.hikari.maximum-pool-size=60
  ```
- Increase connection pool to support 51+ concurrent connections

### OTEL Collector Missing Active Queries

**Issue:** Collector doesn't capture the 5-second queries

**Solutions:**
- Reduce `collection_interval` to 2-5 seconds
- Check collector has permissions to query `sys.dm_exec_requests`
- Verify collector is running during the test

### Queries Complete Too Fast

**Issue:** Hard to observe active queries

**Solutions:**
- Increase the 5-second delay to 10-15 seconds:
  ```sql
  WAITFOR DELAY '00:00:15';  -- Change in repository
  ```
- Run fewer concurrent queries (e.g., 10 instead of 50)

---

## Advanced Testing

### Test Different Concurrency Levels

Modify the scripts to test different numbers of concurrent queries:

**Linux/Mac (`test-50-active-queries.sh`):**
```bash
# Change this line:
for i in {1..50}; do
# To:
for i in {1..10}; do  # Test with 10 queries
```

**Windows (`test-50-active-queries.bat`):**
```batch
REM Change this line:
FOR /L %%i IN (1,1,50) DO (
REM To:
FOR /L %%i IN (1,1,10) DO (
```

---

## Summary

| Test Type | Duration | Endpoint | Purpose |
|-----------|----------|----------|---------|
| Stuck Query | 5 min | `/slow-query` | Long-running query monitoring |
| Slow Query | 1 min | `/slow-sales-query` | Slow query stats collection |
| Active Query | 5 sec | `/active-sales-query` | Active running query monitoring |
| Product Query | 5 min | `/slow-product-query` | Complex aggregation testing |

**Key Testing Script:**
- `test-50-active-queries.sh` / `test-50-active-queries.bat` - Tests 1 slow + 50 active queries

This setup provides comprehensive testing for both slow query detection and active query monitoring in your OTEL collector!
