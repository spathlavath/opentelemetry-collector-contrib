# Rollback and Simplify - What Changed

## Problem

You had multiple complex Person UPDLOCK queries that were:
- Causing query parsing errors (SQL `--` comments)
- Too complex to debug
- Multiple endpoints doing similar things
- Hard to correlate slow vs active queries

## Solution

**Rolled back to ONE simple query pattern** that does everything you need.

## What Was Removed

### Removed Queries (from UserRepository.java)
1. ~~`executeComplexPersonUpdlockQuery()`~~ - Complex query with 8 subqueries
2. ~~`executeSlowComplexPersonUpdlockQuery()`~~ - Slow version with 3 subqueries

### Removed Endpoints (from UserController.java)
1. ~~`GET /api/users/active-complex-person-updlock-query`~~
2. ~~`GET /api/users/slow-complex-person-updlock-query`~~
3. ~~`GET /api/users/active-person-updlock-query`~~ (renamed)

### Removed Service Methods (from UserService.java)
1. ~~`executeComplexPersonUpdlockQuery()`~~
2. ~~`executeSlowComplexPersonUpdlockQuery()`~~

## What Remains - The ONE Query

### UserRepository.java

**Single method**:
```java
@Query(value = """
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
    """, nativeQuery = true)
List<Map<String, Object>> executePersonUpdlockQuery();
```

**Key Features**:
- ✅ No SQL `--` comments (avoids parsing errors)
- ✅ Simple pattern: `SELECT * FROM Person WITH(UPDLOCK)`
- ✅ 2 subqueries for execution plan complexity
- ✅ 90-second duration for active query capture
- ✅ UPDLOCK creates blocking scenario
- ✅ Same query_hash for correlation

### UserController.java

**Single endpoint**:
```java
@GetMapping("/person-updlock-query")
public ResponseEntity<List<Map<String, Object>>> executePersonUpdlockQuery() {
    List<Map<String, Object>> results = userService.executePersonUpdlockQuery();
    return ResponseEntity.ok(results);
}
```

**URL**: `GET /api/users/person-updlock-query`

### UserService.java

**Single method**:
```java
public List<Map<String, Object>> executePersonUpdlockQuery() {
    return userRepository.executePersonUpdlockQuery();
}
```

## Test Scripts

### New Simple Test Scripts

1. **test-person-updlock-simple.ps1** (PowerShell)
   - Fires 10 concurrent queries
   - Creates blocking scenario
   - Monitors for 90 seconds

2. **test-person-updlock-simple.bat** (Windows Batch)
   - Same as PowerShell version
   - No complex arithmetic

### Old Test Scripts (still exist but not needed)
- ~~test-50-active-queries-person-updlock.ps1~~
- ~~test-50-active-queries-person-updlock.bat~~
- ~~test-all-endpoints.ps1~~
- ~~test-all-endpoints.bat~~

## How to Use

### Step 1: Rebuild

```powershell
cd sample-spring-boot-app
mvn clean package -DskipTests
```

### Step 2: Start

```powershell
.\start-with-apm.bat
```

### Step 3: Run Simple Test

```powershell
.\test-person-updlock-simple.ps1
```

OR

```batch
test-person-updlock-simple.bat
```

### Step 4: Monitor

**SQL Server** (run while test is active):
```sql
SELECT
    session_id,
    status,
    wait_type,
    blocking_session_id,
    SUBSTRING(qt.text, 1, 100) AS query_text
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
WHERE qt.text LIKE '%PERSON_UPDLOCK_WITH_PLAN%';
```

**Expected**: 10 rows
- 1 with `status='running'`, `wait_type='WAITFOR'`, `blocking_session_id=NULL`
- 9 with `status='suspended'`, `wait_type='LCK_M_U'`, `blocking_session_id=<first session>`

## What You Get

### 1. Slow Query Metrics
- Captured in `sys.dm_exec_query_stats`
- `query_hash`: Same for all executions
- `execution_count`: 10 (one per query)
- `avg_elapsed_time`: ~90,000 ms

### 2. Active Query Metrics
- Captured in `sys.dm_exec_requests`
- Scraped every 15 seconds by collector
- Each query captured ~6 times (15s × 6 = 90s)
- Total captures: 10 queries × 6 captures = 60 events

### 3. Blocking Information
- `blocking_session_id`: 9 queries blocked by 1st
- `wait_type`: 'LCK_M_U' (Update lock wait)
- `wait_time`: Increases over time

### 4. Execution Plan
- Multiple operators:
  - Table Scan (Person.Person)
  - Nested Loop (EmailAddress subquery)
  - Nested Loop (PersonPhone subquery)
  - Sort (ORDER BY)
- `query_plan_hash`: Same for all executions

## Benefits of This Approach

1. **Simple**: ONE query, ONE endpoint, ONE test script
2. **Same query_hash**: Easy to correlate slow and active queries
3. **Blocking Scenario**: Real-world locking behavior
4. **No Parsing Errors**: No SQL comments to cause problems
5. **Easy to Debug**: Single pattern to trace through system
6. **Realistic**: Matches your actual pattern `SELECT * FROM Person WITH(updlock)`

## Files Changed

1. ✏️ **UserRepository.java** - Simplified to ONE query method
2. ✏️ **UserController.java** - Simplified to ONE endpoint
3. ✏️ **UserService.java** - Simplified to ONE service method
4. ➕ **test-person-updlock-simple.ps1** - NEW simple test (PowerShell)
5. ➕ **test-person-updlock-simple.bat** - NEW simple test (Batch)
6. ➕ **SIMPLE-PERSON-UPDLOCK-TEST.md** - NEW documentation
7. ➕ **ROLLBACK-AND-SIMPLIFY.md** - This file

## Next Steps

1. Rebuild application: `mvn clean package -DskipTests`
2. Start application: `.\start-with-apm.bat`
3. Run test: `.\test-person-updlock-simple.ps1`
4. Monitor SQL Server for active queries
5. Check New Relic for metrics after 2 minutes

## Summary

✅ Removed complex queries
✅ ONE simple query pattern
✅ ONE endpoint
✅ ONE test script
✅ No parsing errors
✅ Same query_hash for correlation
✅ Blocking scenario included
✅ Execution plan with multiple operators

This is the **simplest possible scenario** to test your OTEL collector!
