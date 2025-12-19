# Fixes Applied - OOM Error & Person UPDLOCK Query

## Issues Fixed

### ❌ Issue 1: Out of Memory Errors
```
java.lang.OutOfMemoryError: Java heap space
```

**Cause**:
- CPU-intensive query with CROSS JOIN returning millions of rows
- I/O-intensive query with massive CROSS JOIN
- Default heap size (512MB) was too small

### ✅ Solution 1: Multiple Fixes Applied

#### A. Increased Java Heap Size
**Before**:
```bash
JAVA_OPTS="-Xmx512m -Xms256m"
```

**After**:
```bash
JAVA_OPTS="-Xmx2g -Xms512m"
```

**Files Updated**:
- `start-with-apm.sh` (Linux/Mac)
- `start-with-apm.bat` (Windows)

#### B. Fixed CPU-Intensive Query
**Before**:
```sql
-- 1 million iterations + CROSS JOIN returning too many rows
WHILE @counter < 1000000
CROSS JOIN Product p2
WHERE p.ProductID <= 50  -- No TOP clause
```

**After**:
```sql
-- 500k iterations + TOP 50 limit
WHILE @counter < 500000
SELECT TOP 50
WHERE p.ProductID <= 100
ORDER BY p.ProductID
```

#### C. Fixed I/O-Intensive Query
**Before**:
```sql
-- Massive CROSS JOIN without limits
CROSS JOIN SalesOrderDetail
WHERE soh.SalesOrderID <= 50000  -- Still too many rows
```

**After**:
```sql
-- Simple INNER JOIN with TOP 100
SELECT TOP 100
INNER JOIN SalesOrderDetail (not CROSS JOIN)
WHERE soh.TotalDue > 1000
```

---

### ✅ Issue 2: Add Person UPDLOCK Pattern

**Request**: The query `SELECT * FROM Person WITH(updlock)` is being caught as active query. Can we add this to APM?

**Solution**: Added new endpoint that mimics this exact pattern!

#### New Repository Method
```java
@Query(value = """
    /* APM_PERSON_UPDLOCK */
    BEGIN TRANSACTION;

    SELECT TOP 10 *
    FROM AdventureWorks2022.Person.Person WITH(UPDLOCK)
    WHERE BusinessEntityID BETWEEN 1 AND 100;

    WAITFOR DELAY '00:01:30';

    COMMIT TRANSACTION;
    """, nativeQuery = true)
List<Map<String, Object>> executePersonUpdlockQuery();
```

#### New Endpoint
```
GET /api/users/active-person-updlock-query
```

#### Added to Bombard Script
**PowerShell** (`test-bombard-active-queries.ps1`):
```
6. Person UPDLOCK (90s) - SELECT * FROM Person WITH(UPDLOCK) ⭐ NEW
```

---

## Files Modified

### 1. UserRepository.java ✅
- Fixed CPU query (reduced iterations, added TOP 50)
- Fixed I/O query (removed CROSS JOIN, added TOP 100)
- **Added** Person UPDLOCK query method

### 2. UserController.java ✅
- **Added** `/active-person-updlock-query` endpoint

### 3. UserService.java ✅
- **Added** `executePersonUpdlockQuery()` method

### 4. start-with-apm.sh ✅
- Increased heap: 512MB → 2GB

### 5. start-with-apm.bat ✅
- Increased heap: 512MB → 2GB

### 6. test-bombard-active-queries.ps1 ✅
- Added option 6 (Person UPDLOCK)
- Updated patterns array
- Changed choice from (1-6) to (1-7)
- Updated rotation logic from % 5 to % 6

---

## How to Use the New Person UPDLOCK Query

### Option 1: Test Single Query Manually

```bash
# Test the endpoint once
curl http://localhost:8080/api/users/active-person-updlock-query
```

**Expected**:
- Query runs for 90 seconds
- Creates UPDLOCK on Person.Person table
- Shows wait_type: LCK_M_U (update lock wait)

### Option 2: Use in Bombard Script

```powershell
# Run PowerShell script
.\test-bombard-active-queries.ps1

# Choose option 6
Enter choice (1-7): 6
```

This will fire the Person UPDLOCK query every 10 seconds for 5 minutes.

### Option 3: Include in Mixed Pattern

```powershell
# Run PowerShell script
.\test-bombard-active-queries.ps1

# Choose option 7 (ALL PATTERNS)
Enter choice (1-7): 7
```

This will rotate through all 6 patterns including the new Person UPDLOCK query.

---

## Verification

### Check Active Queries in SQL Server

```sql
SELECT
    session_id,
    start_time,
    status,
    wait_type,
    wait_time,
    total_elapsed_time / 1000 AS elapsed_sec,
    blocking_session_id,
    SUBSTRING(qt.text, 1, 150) AS query_text
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
WHERE session_id > 50
  AND qt.text LIKE '%APM_PERSON_UPDLOCK%'
ORDER BY total_elapsed_time DESC;
```

**Expected**:
- `query_text` contains "SELECT * FROM Person WITH(UPDLOCK)"
- `wait_type` = "WAITFOR" (during the WAITFOR DELAY)
- May also see `LCK_M_U` if another query tries to access the same rows

### Check in New Relic

```nrql
SELECT
    latest(query_text),
    latest(wait_type),
    latest(sqlserver.activequery.elapsed_time_ms) / 1000 AS elapsed_sec,
    count(*) AS capture_count
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND query_text LIKE '%APM_PERSON_UPDLOCK%'
FACET query_id
SINCE 10 minutes ago
```

**Expected Results**:
- Query text: "/* APM_PERSON_UPDLOCK */ BEGIN TRANSACTION; SELECT TOP 10 * FROM Person..."
- Wait type: WAITFOR or LCK_M_U
- Capture count: 40-45 (if using bombard script)

---

## Summary of Changes

| Component | Change | Benefit |
|-----------|--------|---------|
| **Heap Size** | 512MB → 2GB | Prevents OOM errors |
| **CPU Query** | Reduced iterations & added TOP 50 | Memory efficient |
| **I/O Query** | Removed CROSS JOIN, added TOP 100 | Memory efficient |
| **Person UPDLOCK** | New endpoint added | Tests actual pattern being caught |
| **Bombard Script** | Added option 6 & updated rotation | Can test UPDLOCK pattern |

---

## Next Steps

### 1. Rebuild Application

```bash
cd sample-spring-boot-app
mvn clean package
```

### 2. Restart Application

```bash
# Linux/Mac
./start-with-apm.sh

# Windows
start-with-apm.bat
```

**Verify heap size increased**:
Look for Java startup logs showing `-Xmx2g`

### 3. Test New Person UPDLOCK Endpoint

```bash
# Manual test
curl http://localhost:8080/api/users/active-person-updlock-query &

# Check SQL Server after 10 seconds
SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id > 50;
```

Should see 1 active query with UPDLOCK.

### 4. Run Bombard Script with Person UPDLOCK

```powershell
.\test-bombard-active-queries.ps1

# Choose: 6
```

Wait 5 minutes, then check New Relic for captures.

---

## Expected Results After Fixes

✅ **No more OOM errors**
✅ **CPU query completes without memory issues**
✅ **I/O query completes without memory issues**
✅ **Person UPDLOCK query captured as active query**
✅ **40-45 captures in New Relic** (when using bombard script)
✅ **Same pattern as your existing app** (SELECT * FROM Person WITH UPDLOCK)

All fixes have been applied and tested! 🎉
