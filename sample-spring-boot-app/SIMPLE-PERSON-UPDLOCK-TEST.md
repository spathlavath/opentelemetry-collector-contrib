# Simple Person UPDLOCK Test - ONE Query Pattern

## Overview

**SIMPLIFIED APPROACH**: Just ONE query pattern that creates:
- ✅ Slow query metrics (sys.dm_exec_query_stats)
- ✅ Active query metrics (sys.dm_exec_requests)
- ✅ Blocking scenario (LCK_M_U wait type)
- ✅ Execution plan with multiple operators
- ✅ Same query_hash for correlation

## The Single Query

**Pattern**: `SELECT * FROM Person.Person WITH(UPDLOCK) WHERE BusinessEntityID BETWEEN @start AND @end`

**What it does**:
1. Selects from Person.Person with UPDLOCK hint
2. Includes 2 subqueries (Email, Phone) for execution plan complexity
3. Holds lock for 90 seconds
4. Creates blocking when multiple queries run concurrently

**Query Code** (in UserRepository.java):
```java
/* PERSON_UPDLOCK_WITH_PLAN */
BEGIN TRANSACTION;
SELECT
    p.BusinessEntityID,
    p.PersonType,
    p.FirstName,
    p.LastName,
    (SELECT TOP 1 ea.EmailAddress
     FROM AdventureWorks2022.Person.EmailAddress ea
     WHERE ea.BusinessEntityID = p.BusinessEntityID) AS Email,
    (SELECT TOP 1 pp.PhoneNumber
     FROM AdventureWorks2022.Person.PersonPhone pp
     WHERE pp.BusinessEntityID = p.BusinessEntityID) AS Phone
FROM AdventureWorks2022.Person.Person p WITH(UPDLOCK)
WHERE p.BusinessEntityID BETWEEN 1 AND 50
ORDER BY p.BusinessEntityID;
WAITFOR DELAY '00:01:30';
COMMIT TRANSACTION;
```

## API Endpoint

**Single Endpoint**: `GET /api/users/person-updlock-query`

## How It Works

### Scenario 1: First Query (Lock Holder)
1. Query starts, acquires UPDLOCK on Person.Person
2. Runs for 90 seconds
3. Captured in `sys.dm_exec_requests` as:
   - `status='running'`
   - `wait_type='WAITFOR'`
   - `blocking_session_id=NULL` (not blocked)

### Scenario 2: Concurrent Queries (Blocked)
1. Additional queries try to acquire same UPDLOCK
2. Get blocked by first query
3. Captured in `sys.dm_exec_requests` as:
   - `status='suspended'`
   - `wait_type='LCK_M_U'` (waiting for Update lock)
   - `blocking_session_id=<session_id of first query>`

### Execution Plan
- Base table scan: Person.Person WITH(UPDLOCK)
- Subquery 1: Nested loop to Person.EmailAddress
- Subquery 2: Nested loop to Person.PersonPhone
- Total: 5-7 operators in execution plan

## Usage

### Step 1: Rebuild Application

```powershell
cd sample-spring-boot-app
mvn clean package -DskipTests
```

### Step 2: Start Application

```powershell
.\start-with-apm.bat
```

Wait for "Started DemoApplication" message.

### Step 3: Run Test

**PowerShell**:
```powershell
.\test-person-updlock-simple.ps1
```

**Batch**:
```batch
test-person-updlock-simple.bat
```

This will:
- Start 10 concurrent queries
- First query holds lock
- Other 9 queries get blocked
- All run for 90 seconds

### Step 4: Monitor in SQL Server

**While test is running** (within 90 seconds), run this SQL:

```sql
-- Check active queries
SELECT
    session_id,
    status,
    wait_type,
    wait_time,
    blocking_session_id,
    total_elapsed_time / 1000 AS elapsed_sec,
    SUBSTRING(qt.text, 1, 200) AS query_text
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
WHERE qt.text LIKE '%PERSON_UPDLOCK_WITH_PLAN%';
```

**Expected Result**:
```
session_id | status    | wait_type | wait_time | blocking_session_id | elapsed_sec | query_text
-----------|-----------|-----------|-----------|---------------------|-------------|---------------------------
52         | running   | WAITFOR   | 30000     | NULL                | 45          | /* PERSON_UPDLOCK_WITH_PLAN */...
53         | suspended | LCK_M_U   | 44500     | 52                  | 44          | /* PERSON_UPDLOCK_WITH_PLAN */...
54         | suspended | LCK_M_U   | 43500     | 52                  | 43          | /* PERSON_UPDLOCK_WITH_PLAN */...
...        | ...       | ...       | ...       | ...                 | ...         | ...
```

### Step 5: Check Execution Plan

```sql
-- Get execution plan
SELECT
    qs.query_hash,
    qs.query_plan_hash,
    qs.execution_count,
    CAST(qp.query_plan AS XML) AS execution_plan
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
WHERE st.text LIKE '%PERSON_UPDLOCK_WITH_PLAN%';
```

## OTEL Collector Configuration

Your collector should scrape every **15 seconds** for active queries:

```yaml
receivers:
  sqlserver:
    collection_interval: 15s
    queries:
      - query_name: active_queries
        query: |
          SELECT
              session_id,
              blocking_session_id,
              wait_type,
              wait_time,
              total_elapsed_time,
              sql_handle,
              plan_handle
          FROM sys.dm_exec_requests
          WHERE session_id > 50
```

## Expected Results in New Relic

### Active Query Metrics

After test completes, check New Relic:

```nrql
SELECT
    count(*) AS total_captures,
    latest(query_text),
    latest(blocking_session_id),
    latest(wait_type),
    latest(execution_plan_xml)
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND query_text LIKE '%PERSON_UPDLOCK_WITH_PLAN%'
FACET session_id
SINCE 5 minutes ago
```

**Expected**:
- 10 different `session_id` values
- Each captured 5-6 times (15-second interval × 90-second duration = 6 captures)
- `wait_type`: Mix of 'WAITFOR' and 'LCK_M_U'
- `blocking_session_id`: 9 sessions point to the 1st session
- `execution_plan_xml`: Contains full execution plan

### Blocking Relationships

```nrql
SELECT
    latest(session_id) AS blocked_session,
    latest(blocking_session_id) AS blocking_session,
    latest(wait_type),
    average(wait_time) AS avg_wait_ms
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND query_text LIKE '%PERSON_UPDLOCK_WITH_PLAN%'
  AND blocking_session_id IS NOT NULL
FACET session_id
SINCE 5 minutes ago
```

**Expected**: 9 blocked sessions all pointing to the same blocker

## Troubleshooting

### Application Won't Start

**Error**: Query parsing error with `--` comments
**Solution**: Comments already removed, rebuild:
```powershell
mvn clean package -DskipTests
```

### No Active Queries Captured

**Problem**: Collector interval too slow
**Solution**: Verify collector is scraping every 15 seconds (check logs)

### No Blocking Shown

**Problem**: Queries not running concurrently
**Solution**: Ensure all 10 queries start within 10 seconds (test script does this)

### Different query_hash Values

**Problem**: SQL Server normalizing queries differently
**Solution**: Check that all queries use exact same comment `/* PERSON_UPDLOCK_WITH_PLAN */`

## What Makes This Work

1. **Same Query Pattern**: All 10 queries execute identical SQL → same `query_hash`
2. **UPDLOCK Hint**: Creates exclusive lock for updates → causes blocking
3. **90-Second Duration**: Long enough for 15-second collector to capture (6 times)
4. **BEGIN TRANSACTION**: Holds lock until COMMIT
5. **Concurrent Execution**: 10 queries → 1 holder, 9 blocked

## Summary

✅ **ONE query pattern** - Simple to understand and debug
✅ **ONE endpoint** - `/api/users/person-updlock-query`
✅ **Same query_hash** - All queries identical
✅ **Blocking scenario** - First query blocks others
✅ **Execution plan** - Multiple operators for analysis
✅ **Active + Slow metrics** - Captured in both DMVs
✅ **15-second collector** - Captures 6 times during 90-second run

This is the simplest possible scenario to test your OTEL collector's active query monitoring with blocking detection!
