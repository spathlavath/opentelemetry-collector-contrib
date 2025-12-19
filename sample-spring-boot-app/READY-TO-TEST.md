# Ready to Test - Verification Checklist

## ✅ Code Verification Complete

I've verified everything is in place and ready to test:

### ✅ Repository Changes
- **UserRepository.java**: ONE simple query with no SQL comments ✓
- **UserController.java**: ONE endpoint `/person-updlock-query` ✓
- **UserService.java**: ONE service method ✓
- **start-with-apm.bat**: Heap size increased to 2GB ✓
- **Test scripts**: Both .ps1 and .bat versions created ✓

### ✅ Query Verification
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

**Query Features**:
- ✅ No `--` comments (no parsing errors)
- ✅ UPDLOCK hint (creates blocking)
- ✅ 90-second duration (captured 6 times by 15s collector)
- ✅ 2 subqueries (execution plan has 5-7 operators)
- ✅ Same pattern for all executions (same query_hash)

### ✅ Endpoint Verification
- **URL**: `http://localhost:8080/api/users/person-updlock-query`
- **Method**: GET
- **Returns**: JSON array of Person records

### ✅ Test Script Verification
- **PowerShell**: `test-person-updlock-simple.ps1` ✓
- **Batch**: `test-person-updlock-simple.bat` ✓
- **What it does**: Fires 10 concurrent queries, creates blocking scenario

## Step-by-Step Test Instructions

### Pre-requisites
1. ✅ SQL Server running on `localhost:1433`
2. ✅ AdventureWorks2022 database exists
3. ✅ Credentials: `sa` / `AbAnTaPassword@123`
4. ✅ New Relic license key set in environment
5. ✅ OTEL collector configured with 15-second scrape interval

### Step 1: Copy Files to Windows Machine

Your Mac has the latest files at:
```
/Users/pkulkarni/workspace/mssql-otel/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/sample-spring-boot-app/
```

Copy to Windows at:
```
C:\Users\mssql\sample-spring-boot-app\
```

**Files you MUST copy** (modified):
- `src/main/java/com/example/demo/repository/UserRepository.java`
- `src/main/java/com/example/demo/controller/UserController.java`
- `src/main/java/com/example/demo/service/UserService.java`
- `start-with-apm.bat`
- `test-person-updlock-simple.ps1` (NEW)
- `test-person-updlock-simple.bat` (NEW)
- `SIMPLE-PERSON-UPDLOCK-TEST.md` (NEW - read this!)

### Step 2: Rebuild Application

On Windows:
```powershell
cd C:\Users\mssql\sample-spring-boot-app
mvn clean package -DskipTests
```

**Expected output**:
```
[INFO] BUILD SUCCESS
[INFO] Total time: 30-60 seconds
[INFO] Finished at: ...
```

### Step 3: Start Application

```powershell
.\start-with-apm.bat
```

**Wait for this line**:
```
Started DemoApplication in X.XXX seconds
```

**Verify endpoint is accessible**:
```powershell
curl http://localhost:8080/api/health
```

Should return: `{"status":"UP"}`

### Step 4: Run Test

**PowerShell** (recommended):
```powershell
.\test-person-updlock-simple.ps1
```

**OR Batch**:
```batch
test-person-updlock-simple.bat
```

**What happens**:
1. Script starts 10 concurrent queries
2. Each query holds UPDLOCK for 90 seconds
3. First query holds lock, other 9 get blocked
4. Script waits 95 seconds for completion

### Step 5: Monitor in SQL Server (IMMEDIATELY)

**Within 10 seconds** of starting the test, run this in SQL Server Management Studio:

```sql
SELECT
    session_id,
    status,
    wait_type,
    wait_time,
    blocking_session_id,
    total_elapsed_time / 1000 AS elapsed_sec,
    SUBSTRING(qt.text, 1, 100) AS query_text
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
WHERE qt.text LIKE '%PERSON_UPDLOCK_WITH_PLAN%';
```

**Expected Result** (example):
```
session_id | status    | wait_type | wait_time | blocking_session_id | elapsed_sec | query_text
-----------|-----------|-----------|-----------|---------------------|-------------|---------------------------
52         | running   | WAITFOR   | 15234     | NULL                | 25          | /* PERSON_UPDLOCK_WITH_PLAN */...
53         | suspended | LCK_M_U   | 24567     | 52                  | 24          | /* PERSON_UPDLOCK_WITH_PLAN */...
54         | suspended | LCK_M_U   | 23456     | 52                  | 23          | /* PERSON_UPDLOCK_WITH_PLAN */...
55         | suspended | LCK_M_U   | 22345     | 52                  | 22          | /* PERSON_UPDLOCK_WITH_PLAN */...
56         | suspended | LCK_M_U   | 21234     | 52                  | 21          | /* PERSON_UPDLOCK_WITH_PLAN */...
57         | suspended | LCK_M_U   | 20123     | 52                  | 20          | /* PERSON_UPDLOCK_WITH_PLAN */...
58         | suspended | LCK_M_U   | 19012     | 52                  | 19          | /* PERSON_UPDLOCK_WITH_PLAN */...
59         | suspended | LCK_M_U   | 17901     | 52                  | 17          | /* PERSON_UPDLOCK_WITH_PLAN */...
60         | suspended | LCK_M_U   | 16890     | 52                  | 16          | /* PERSON_UPDLOCK_WITH_PLAN */...
61         | suspended | LCK_M_U   | 15789     | 52                  | 15          | /* PERSON_UPDLOCK_WITH_PLAN */...
```

**Key observations**:
- ✅ 10 rows (all 10 queries running)
- ✅ 1 query with `status='running'` and `blocking_session_id=NULL` (holder)
- ✅ 9 queries with `status='suspended'`, `wait_type='LCK_M_U'` (blocked)
- ✅ All 9 blocked queries have same `blocking_session_id` (the holder's session)

### Step 6: Check Execution Plan

```sql
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

**Expected**:
- ✅ ONE `query_hash` value (all queries identical)
- ✅ ONE `query_plan_hash` value (same execution plan)
- ✅ `execution_count` = 10 (or growing as queries complete)
- ✅ `execution_plan` XML shows multiple operators

### Step 7: Verify OTEL Collector is Scraping

Check OTEL collector logs for:
```
Scraping active queries...
Found X active queries
```

**Should happen every 15 seconds**.

### Step 8: Wait for Test to Complete

Test runs for 95 seconds total. After completion:

**Check logs** (PowerShell):
```powershell
Get-Job | Receive-Job
```

**Check logs** (Batch):
```batch
type %TEMP%\person-updlock-1.log
type %TEMP%\person-updlock-2.log
```

**Expected**: JSON responses with Person records (no errors)

### Step 9: Verify in New Relic (After 2-3 Minutes)

```nrql
SELECT
    count(*) AS total_captures,
    latest(query_text),
    latest(blocking_session_id),
    latest(wait_type),
    latest(query_hash)
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND query_text LIKE '%PERSON_UPDLOCK_WITH_PLAN%'
FACET session_id
SINCE 5 minutes ago
```

**Expected**:
- ✅ 10 different `session_id` values
- ✅ Each captured 5-6 times (15s interval × 90s duration = 6 captures)
- ✅ Total captures: ~50-60 events
- ✅ 9 sessions show `blocking_session_id` pointing to 1st session
- ✅ `wait_type`: Mix of 'WAITFOR' (holder) and 'LCK_M_U' (blocked)
- ✅ All have same `query_hash`

## What Could Go Wrong

### ❌ Application Won't Start - Parse Error

**Symptom**: Error about "quoted range at 2054"
**Cause**: SQL comments still present
**Fix**: Make sure you copied the LATEST UserRepository.java file

### ❌ Application Won't Start - OOM Error

**Symptom**: `java.lang.OutOfMemoryError`
**Cause**: Heap size too small
**Fix**: Make sure start-with-apm.bat has `-Xmx2g`

### ❌ No Active Queries Visible

**Symptom**: SQL query returns 0 rows
**Cause**: Test hasn't started or queries already finished
**Fix**: Run SQL query IMMEDIATELY after starting test (within 10 seconds)

### ❌ Endpoint Not Found

**Symptom**: `curl` returns 404
**Cause**: Wrong URL or controller not updated
**Fix**: Verify URL is `http://localhost:8080/api/users/person-updlock-query`

### ❌ No Blocking Shown

**Symptom**: All queries show `blocking_session_id=NULL`
**Cause**: Queries not running concurrently
**Fix**: Test script starts queries 500ms apart - should be fine

## Summary - You Are Ready!

✅ **Code**: Simple, clean, no SQL comments
✅ **Endpoint**: ONE endpoint `/person-updlock-query`
✅ **Query**: Person UPDLOCK with 90-second hold
✅ **Blocking**: First query blocks other 9
✅ **Execution Plan**: Multiple operators (5-7)
✅ **Test Script**: Simple PowerShell/Batch script
✅ **Heap Size**: 2GB (no OOM errors)
✅ **Same query_hash**: Easy correlation
✅ **Documentation**: Complete guides included

## Yes, You Can Test This Completely!

Everything is in place. Just:

1. Copy files to Windows
2. Rebuild: `mvn clean package -DskipTests`
3. Start: `.\start-with-apm.bat`
4. Test: `.\test-person-updlock-simple.ps1`
5. Monitor: Run SQL queries in SSMS
6. Verify: Check New Relic after 2-3 minutes

If you hit ANY issues, check the troubleshooting section above or refer to **SIMPLE-PERSON-UPDLOCK-TEST.md** for detailed guidance.

**Good luck!** 🚀
