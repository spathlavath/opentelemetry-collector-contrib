# Quick Start - Bombard Active Queries

## TL;DR - Just Get It Running

### 1. Rebuild Application
```bash
cd sample-spring-boot-app
mvn clean package
```

### 2. Start Application
```bash
./start-with-apm.sh
```

### 3. Run Bombard Script
```bash
./test-bombard-active-queries.sh
```

### 4. Choose Pattern
```
Enter choice (1-6): 1
```
*Recommendation: Start with option 1*

### 5. Wait & Monitor
- **After 60 seconds**: First scrape, should see 6 queries
- **After 5 minutes**: Test complete, 40-45 captures expected

### 6. Check New Relic
```nrql
SELECT count(*), latest(query_text)
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
FACET query_id
SINCE 10 minutes ago
```

**Expected**: 40-45 metric captures with `APM_ACTIVE_` query text

---

## What Each Script Does

| Script | Purpose | Duration | Queries |
|--------|---------|----------|---------|
| `test-active-queries-guaranteed.sh` | One-time burst | Immediate | 25 at once |
| **`test-bombard-active-queries.sh`** ⭐ | Continuous bombardment | 5 minutes | ~30 over time |

---

## Quick Verification Commands

### Check Active Queries in SQL Server
```sql
SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id > 50;
```
**Expected during test**: 6-9 queries

### Check New Relic Capture Count
```nrql
SELECT count(*) as total_captures
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND elapsed_time_ms > 5000
SINCE 10 minutes ago
```
**Expected after test**: 40-45 captures

---

## Troubleshooting One-Liners

### Problem: Zero active queries
```bash
# Check if app is running
curl http://localhost:8080/api/health

# Manual test one query
curl http://localhost:8080/api/users/active-sales-query &
```

### Problem: Queries not in New Relic
```bash
# Check collector logs
tail -f otel-collector.log | grep -i active
```

### Problem: Connection pool exhausted
```properties
# Edit application.properties
spring.datasource.hikari.maximum-pool-size=50
```

---

## Expected Timeline

```
0:00  - Start bombard script
0:10  - Query 1 starts
0:20  - Query 2 starts
0:30  - Query 3 starts (3 running)
0:40  - Query 4 starts
0:50  - Query 5 starts
1:00  - Query 6 starts (6 running) ← FIRST SCRAPE 🎯
1:10  - Query 7 starts
1:20  - Query 8 starts
1:30  - Query 9 starts (9 running, Query 1 completes)
2:00  - SECOND SCRAPE 🎯 (9 queries)
3:00  - THIRD SCRAPE 🎯 (9 queries)
4:00  - FOURTH SCRAPE 🎯 (9 queries)
5:00  - FIFTH SCRAPE 🎯 (9 queries)
6:30  - All queries complete
```

---

## Success Criteria

✅ SQL Server shows 6-9 active queries during test
✅ Collector logs show "Collected X active queries" 5 times
✅ New Relic shows 40-45 metric captures
✅ Query text contains "APM_ACTIVE_" or "WAITFOR DELAY"
✅ Same query_id appears in both slow and active metrics

---

## Full Documentation

- **ACTIVE-QUERIES-FIXED.md** - Problem explanation & solution
- **BOMBARD-STRATEGY.md** - Detailed math & strategy
- **This file** - Quick reference

---

## One-Line Success Check

Run this after 5 minutes:

```nrql
SELECT count(*) FROM Metric WHERE metricName = 'sqlserver.activequery.elapsed_time_ms' SINCE 10 minutes ago
```

If result is **40+** → SUCCESS! ✅

If result is **0** → Check troubleshooting section above
