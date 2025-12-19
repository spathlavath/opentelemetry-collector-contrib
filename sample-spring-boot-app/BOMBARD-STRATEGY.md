# Bombard Strategy - Guaranteed Active Query Capture

## Overview

The **bombard strategy** continuously fires the same query every 10 seconds for 5 minutes, creating overlapping active queries that guarantee capture by the OTEL collector.

---

## How It Works

### Timeline Visualization

```
Time:     0s    10s   20s   30s   40s   50s   60s   70s   80s   90s   100s
          │     │     │     │     │     │     │     │     │     │     │
Query 1:  ╠═════════════════════════════════════════════════════════════════╣ (90s)
Query 2:        ╠═════════════════════════════════════════════════════════════════╣
Query 3:              ╠═════════════════════════════════════════════════════════════════╣
Query 4:                    ╠═════════════════════════════════════════════════════════════════╣
Query 5:                          ╠═════════════════════════════════════════════════════════════════╣
Query 6:                                ╠═════════════════════════════════════════════════════════════════╣
Query 7:                                      ╠═════════════════════════════════════════════════════════════════╣
Query 8:                                            ╠═════════════════════════════════════════════════════════════════╣
Query 9:                                                  ╠═════════════════════════════════════════════════════════════════╣
                                                          ▲
                                                    Collector Scrape
                                                    (Sees 6 queries)
```

### Key Parameters

| Parameter | Value | Reason |
|-----------|-------|--------|
| **Test Duration** | 5 minutes (300s) | Ensures 5 collector scrapes (60s interval) |
| **Query Interval** | 10 seconds | Creates overlapping queries |
| **Query Duration** | 90 seconds | Guarantees capture (1.5x scrape interval) |
| **Concurrent Queries** | 8-9 | At any given time |
| **Total Queries Fired** | ~30 | 300s ÷ 10s = 30 queries |
| **Total Captures** | 40-45 | 5 scrapes × 8-9 queries |

---

## Math Behind The Strategy

### Concurrent Query Calculation

```
Query Duration: 90 seconds
Fire Interval:  10 seconds

Max Concurrent = Query Duration ÷ Fire Interval
               = 90s ÷ 10s
               = 9 queries

Actual Concurrent ≈ 8-9 queries (accounting for completion timing)
```

### Capture Guarantee

```
Collector Interval: 60 seconds
Query Duration:     90 seconds

Scrapes per Query = Query Duration ÷ Collector Interval
                  = 90s ÷ 60s
                  = 1.5 scrapes (minimum 1, likely 2)

Capture Probability = 100% ✅
```

### Total Capture Count

```
Test Duration:      300 seconds
Collector Interval: 60 seconds
Scrapes:           5 scrapes

Queries per Scrape: 8-9
Total Captures:     5 × 8.5 = 42.5 ≈ 40-45 captures
```

---

## Usage

### Run the Bombard Script

```bash
# Linux/Mac
./test-bombard-active-queries.sh

# Windows
test-bombard-active-queries.bat
```

### Choose Query Pattern

```
1. Active Sales Query (90s)        - Complex JOIN with WAITFOR
2. Active Aggregation (120s)       - Time-series aggregation
3. Active Blocking (90s)           - WITH (UPDLOCK, HOLDLOCK)
4. Active CPU (variable)           - CPU-intensive computation
5. Active I/O (variable)           - I/O-intensive cross join
6. ALL PATTERNS (mixed)            - Rotate through all patterns
```

**Recommendation**: Start with **Option 1** (Active Sales Query) for simplest testing.

---

## Expected Results

### SQL Server DMV Counts

| Time | Queries Running | Status |
|------|----------------|--------|
| 30s | 3-4 | Starting to build up |
| 60s | 6 | First collector scrape 🎯 |
| 90s | 8-9 | Peak concurrency reached |
| 120s | 8-9 | Second scrape 🎯 |
| 180s | 8-9 | Third scrape 🎯 |
| 240s | 8-9 | Fourth scrape 🎯 |
| 300s | 8-9 | Fifth scrape 🎯 (last) |
| 330s | 6-7 | Starting to wind down |
| 390s | 0 | All queries complete |

### New Relic Metrics

#### Expected Active Query Metrics

```nrql
SELECT
    count(*) AS total_captures,
    uniqueCount(session_id) AS unique_sessions,
    latest(query_text),
    average(sqlserver.activequery.elapsed_time_ms) / 1000 AS avg_elapsed_sec,
    max(sqlserver.activequery.elapsed_time_ms) / 1000 AS max_elapsed_sec
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND elapsed_time_ms > 5000
FACET query_id
SINCE 10 minutes ago
```

**Expected Output**:
```
query_id              | total_captures | unique_sessions | avg_elapsed_sec | max_elapsed_sec
---------------------|----------------|-----------------|-----------------|----------------
0xABCD1234EFGH5678   | 42             | 30              | 45              | 89
```

#### Query Capture Over Time

```nrql
SELECT
    count(*) AS captures_per_minute
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND elapsed_time_ms > 5000
TIMESERIES 1 minute
SINCE 10 minutes ago
```

**Expected Chart**:
```
Minute 1: 6 captures  (1st scrape)
Minute 2: 9 captures  (2nd scrape)
Minute 3: 9 captures  (3rd scrape)
Minute 4: 9 captures  (4th scrape)
Minute 5: 9 captures  (5th scrape)
Minute 6+: 0 captures (test ended)
```

---

## Verification SQL Queries

### 1. Count Active Queries (Run During Test)

```sql
SELECT COUNT(*) AS active_count
FROM sys.dm_exec_requests
WHERE session_id > 50;
```

**Expected**: 6-9 active queries

### 2. View Query Details

```sql
SELECT
    session_id,
    start_time,
    DATEDIFF(SECOND, start_time, GETDATE()) AS elapsed_sec,
    status,
    wait_type,
    wait_time,
    blocking_session_id,
    SUBSTRING(qt.text, 1, 100) AS query_preview
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
WHERE session_id > 50
ORDER BY start_time;
```

**Expected**: Multiple rows with `elapsed_sec` ranging from 0 to 89 seconds

### 3. Group by Query Hash

```sql
SELECT
    qs.query_hash,
    COUNT(DISTINCT er.session_id) AS active_sessions,
    MIN(DATEDIFF(SECOND, er.start_time, GETDATE())) AS min_elapsed_sec,
    MAX(DATEDIFF(SECOND, er.start_time, GETDATE())) AS max_elapsed_sec,
    SUBSTRING(qt.text, 1, 100) AS query_preview
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
LEFT JOIN sys.dm_exec_query_stats qs ON qs.sql_handle = er.sql_handle
WHERE er.session_id > 50
GROUP BY qs.query_hash, qt.text;
```

**Expected**: 1 row (same query hash) with `active_sessions` = 6-9

---

## Comparison: Old vs New Approach

### ❌ Old Approach (Failed)

```
Query Duration:  5 seconds
Scrape Interval: 60 seconds
Strategy:        Fire 50 queries once

Problem:
- All 50 queries start at once
- All complete in ~5 seconds
- If scrape happens at wrong time, catches ZERO queries
- Capture rate: ~8% (5s window in 60s cycle)
```

### ✅ New Approach (Guaranteed)

```
Query Duration:  90 seconds
Scrape Interval: 60 seconds
Strategy:        Fire 1 query every 10 seconds for 5 minutes

Benefits:
- Continuous overlapping queries
- 8-9 queries always active
- Every scrape catches active queries
- Capture rate: 100% (queries always running)
```

---

## Advanced Patterns

### Pattern 1: Single Query Type (Simplest)

**Use Case**: Test specific query pattern capture

```bash
./test-bombard-active-queries.sh
# Choose option 1, 2, 3, 4, or 5
```

**Result**: All captures will be same query_id

### Pattern 2: Mixed Patterns (Comprehensive)

**Use Case**: Test multiple query types simultaneously

```bash
./test-bombard-active-queries.sh
# Choose option 6 (ALL PATTERNS)
```

**Result**: 5 different query_id values, rotated evenly

### Pattern 3: Extended Duration

**Modification**: Edit script to run for 10 minutes

```bash
# In script, change:
DURATION=300  # to
DURATION=600
```

**Result**: 10 scrapes × 9 queries = 90 captures

---

## Troubleshooting

### Issue: Not seeing 8-9 concurrent queries

**Check**:
```sql
SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id > 50;
```

**Possible causes**:
1. Connection pool limit reached
2. Queries completing too fast
3. Application not starting queries properly

**Solution**:
```properties
# In application.properties, increase connection pool:
spring.datasource.hikari.maximum-pool-size=50
```

### Issue: Queries not appearing in New Relic

**Check collector logs**:
```bash
grep -i "active" otel-collector.log
```

**Verify config**:
```yaml
enable_active_running_queries: true
active_running_queries_elapsed_time_threshold: 5000
collection_interval: 60s
```

### Issue: Scrapes capturing 0 queries

**Timing check**:
- Start bombardment
- Wait exactly 60 seconds
- Run SQL query to count active queries
- Should see 6+ queries

**If zero**: Queries completing too fast, increase duration or decrease interval

---

## Performance Impact

### Database Load

```
Concurrent Queries:   8-9
Query Duration:       90 seconds
Resource Usage:       Medium
Connection Pool:      Requires 10+ connections
CPU Impact:          Low-Medium (depending on pattern)
I/O Impact:          Low-Medium (depending on pattern)
```

### Network Load

```
Requests per Minute:  6 (1 every 10 seconds)
Total Requests:       30 (over 5 minutes)
Bandwidth:           Minimal (<1 MB)
```

### Collector Load

```
Scrapes per Test:     5
Queries per Scrape:   8-9
Metrics per Scrape:   ~50-70 (8-9 queries × 6-8 metrics each)
Total Metrics:        250-350 (over 5 minutes)
```

---

## Summary

✅ **Fires 1 query every 10 seconds**
✅ **Each query runs 90 seconds**
✅ **8-9 queries always active**
✅ **5 collector scrapes guaranteed**
✅ **40-45 total captures expected**
✅ **100% capture success rate**

The bombard strategy ensures that active queries are **always visible** when the collector scrapes, eliminating the timing issues that caused zero captures in the previous approach.
