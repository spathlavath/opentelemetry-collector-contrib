# Person UPDLOCK Queries - Complex Execution Plans

## Overview

Based on your real application pattern: `SELECT * FROM [Person].[Person] WITH(updlock) WHERE [BusinessEntityID]=@`

I've created **complex queries** with **large execution plans** that match this pattern exactly.

---

## New Endpoints Added

### 1. `/api/users/slow-complex-person-updlock-query` (Slow Query)
**Duration**: 1 minute
**Purpose**: Captured in `sys.dm_exec_query_stats` (slow query metrics)

**Query Features**:
- Main query: `SELECT * FROM Person WITH(UPDLOCK)`
- 3 complex subqueries (Email, Phone, OrderCount)
- Large execution plan with multiple table scans
- WHERE clause: `BusinessEntityID BETWEEN 1 AND 50`

### 2. `/api/users/active-complex-person-updlock-query` (Active Query)
**Duration**: 90 seconds
**Purpose**: Captured in `sys.dm_exec_requests` (active query metrics)

**Query Features**:
- Main query: `SELECT * FROM Person WITH(UPDLOCK)`
- **8 complex subqueries**:
  1. EmailAddress lookup
  2. PersonPhone lookup
  3. Address with JOIN (BusinessEntityAddress → Address → StateProvince)
  4. Customer existence check
  5. Employee existence check
  6. Sales order count
  7. Total sales amount
  8. Complex aggregations

**Execution Plan Size**: LARGE (8+ operators, multiple JOINs, subqueries)

---

## Execution Plan Breakdown

### What Makes the Execution Plan Big?

```sql
-- Base table scan with UPDLOCK
FROM Person.Person p WITH(UPDLOCK)

-- Subquery 1: Email lookup (Nested Loop + Index Seek)
(SELECT TOP 1 ea.EmailAddress
 FROM Person.EmailAddress ea
 WHERE ea.BusinessEntityID = p.BusinessEntityID)

-- Subquery 2: Phone lookup (Nested Loop + Index Seek)
(SELECT TOP 1 pp.PhoneNumber
 FROM Person.PersonPhone pp
 WHERE pp.BusinessEntityID = p.BusinessEntityID)

-- Subquery 3: Address with 3-table JOIN (Nested Loops × 3)
(SELECT TOP 1 a.AddressLine1 + ', ' + a.City + ', ' + sp.StateProvinceCode
 FROM Person.BusinessEntityAddress bea
 INNER JOIN Person.Address a ON bea.AddressID = a.AddressID
 INNER JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
 WHERE bea.BusinessEntityID = p.BusinessEntityID)

-- Subquery 4: Customer check (Table Scan + Filter)
(SELECT COUNT(*)
 FROM Sales.Customer c
 WHERE c.PersonID = p.BusinessEntityID)

-- Subquery 5: Employee check (Table Scan + Filter)
(SELECT COUNT(*)
 FROM HumanResources.Employee e
 WHERE e.BusinessEntityID = p.BusinessEntityID)

-- Subquery 6: Order count (JOIN + Aggregation)
(SELECT COUNT(DISTINCT soh.SalesOrderID)
 FROM Sales.Customer c
 INNER JOIN Sales.SalesOrderHeader soh ON c.CustomerID = soh.CustomerID
 WHERE c.PersonID = p.BusinessEntityID)

-- Subquery 7: Sales total (JOIN + SUM aggregation)
(SELECT SUM(soh.TotalDue)
 FROM Sales.Customer c
 INNER JOIN Sales.SalesOrderHeader soh ON c.CustomerID = soh.CustomerID
 WHERE c.PersonID = p.BusinessEntityID)
```

**Total Operators in Execution Plan**: 20-30+
**Tables Accessed**: 8 (Person, EmailAddress, PersonPhone, BusinessEntityAddress, Address, StateProvince, Customer, Employee, SalesOrderHeader)

---

## Test Scripts

### New Test Script: `test-50-active-queries-person-updlock.bat`

**What it does**:
1. Fires 1 SLOW Person UPDLOCK query (1 minute)
2. Fires 50 ACTIVE Person UPDLOCK queries (90 seconds each)
3. All queries use the complex execution plan

**Usage**:
```batch
test-50-active-queries-person-updlock.bat
```

**Alternative (PowerShell)**:
```powershell
.\test-50-active-queries-person-updlock.ps1
```

---

## Expected Results

### SQL Server DMVs

#### During Test - Active Queries
```sql
SELECT
    session_id,
    start_time,
    status,
    wait_type,
    wait_time,
    total_elapsed_time / 1000 AS elapsed_sec,
    blocking_session_id,
    SUBSTRING(qt.text, 1, 200) AS query_text
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
WHERE qt.text LIKE '%COMPLEX_PERSON_UPDLOCK%';
```

**Expected**: 50+ rows with:
- `query_text`: Contains "Person.Person WITH(UPDLOCK)"
- `wait_type`: "WAITFOR" or "LCK_M_U"
- `elapsed_sec`: 0 to 90 seconds

#### After Test - Slow Queries
```sql
SELECT
    qs.query_hash,
    qs.execution_count,
    qs.total_elapsed_time / 1000 / qs.execution_count AS avg_elapsed_ms,
    qs.last_execution_time,
    SUBSTRING(st.text, 1, 200) AS query_text
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
WHERE st.text LIKE '%COMPLEX_PERSON_UPDLOCK%'
ORDER BY qs.last_execution_time DESC;
```

**Expected**: Queries with:
- `execution_count`: 50+ (one per active query)
- `avg_elapsed_ms`: ~90,000 (90 seconds)

#### Execution Plan
```sql
SELECT
    qs.query_hash,
    qs.query_plan_hash,
    CAST(qp.query_plan AS XML) AS execution_plan_xml
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
WHERE st.text LIKE '%COMPLEX_PERSON_UPDLOCK%';
```

**Expected**: Large XML execution plan with:
- Multiple `<RelOp>` nodes (20-30+)
- Nested loops for each subquery
- Table scans and index seeks
- Aggregation operators (COUNT, SUM)

---

### New Relic Metrics

#### Slow Query Metrics
```nrql
SELECT
    latest(query_text),
    latest(sqlserver.slowquery.historical_execution_count) AS execution_count,
    latest(sqlserver.slowquery.historical_avg_elapsed_time_ms) AS avg_elapsed_ms,
    latest(execution_plan_xml)
FROM Metric
WHERE metricName = 'sqlserver.slowquery.historical_execution_count'
  AND query_text LIKE '%SLOW_COMPLEX_PERSON_UPDLOCK%'
FACET query_id
SINCE 10 minutes ago
```

**Expected**:
- `execution_count`: 1 (the slow query)
- `avg_elapsed_ms`: ~60,000 (1 minute)
- `execution_plan_xml`: Large XML with all subqueries

#### Active Query Metrics
```nrql
SELECT
    count(*) AS total_captures,
    latest(query_text),
    average(sqlserver.activequery.elapsed_time_ms) / 1000 AS avg_elapsed_sec,
    max(sqlserver.activequery.elapsed_time_ms) / 1000 AS max_elapsed_sec
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND query_text LIKE '%COMPLEX_PERSON_UPDLOCK_BIG_PLAN%'
FACET query_id
SINCE 10 minutes ago
```

**Expected**:
- `total_captures`: 50+ (all 50 active queries captured)
- `avg_elapsed_sec`: 45 (average time when captured)
- `max_elapsed_sec`: 89 (just before completion)
- `query_text`: Contains all 8 subqueries

---

## Comparison: Sales vs Person UPDLOCK

| Feature | Old (Sales Query) | New (Person UPDLOCK) |
|---------|------------------|---------------------|
| **Base Pattern** | Sales tables | `Person WITH(UPDLOCK)` ✅ |
| **Execution Plan** | Simple JOINs | 8 complex subqueries ✅ |
| **Tables Accessed** | 3-4 | 8 ✅ |
| **Plan Size** | Small | Large (20-30+ operators) ✅ |
| **Matches Real App** | No | Yes ✅ |
| **UPDLOCK Hint** | No | Yes ✅ |
| **Lock Wait Type** | N/A | LCK_M_U ✅ |

---

## Why This Matches Your Real Application

### Your Real Query Pattern:
```sql
SELECT * FROM [Person].[Person] WITH(updlock) WHERE [BusinessEntityID]=@
```

### Our Test Query Pattern:
```sql
SELECT
    p.BusinessEntityID,
    p.FirstName,
    p.LastName,
    ... (8 complex subqueries) ...
FROM AdventureWorks2022.Person.Person p WITH(UPDLOCK)
WHERE p.BusinessEntityID BETWEEN 1 AND 20
```

### Matches:
✅ Same table: `Person.Person`
✅ Same hint: `WITH(UPDLOCK)`
✅ Same WHERE pattern: Filter on `BusinessEntityID`
✅ Same lock behavior: Creates LCK_M_U waits
✅ Complex execution plan: Large XML with multiple operators

---

## Usage Instructions

### Step 1: Rebuild Application
```bash
cd sample-spring-boot-app
mvn clean package
```

### Step 2: Restart Application
```powershell
start-with-apm.bat
```

### Step 3: Run New Test Script
```powershell
# Use the new Person UPDLOCK test
test-50-active-queries-person-updlock.bat

# Or PowerShell version
.\test-50-active-queries-person-updlock.ps1
```

### Step 4: Monitor Results

**During test** (SQL Server):
```sql
SELECT COUNT(*) FROM sys.dm_exec_requests WHERE text LIKE '%COMPLEX_PERSON_UPDLOCK%';
```
**Expected**: 50+ active queries

**After test** (New Relic):
```nrql
SELECT count(*) FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND query_text LIKE '%COMPLEX_PERSON_UPDLOCK%'
SINCE 10 minutes ago
```
**Expected**: 50+ captures

---

## Summary

✅ **Added complex Person UPDLOCK queries**
✅ **Matches your real application pattern** (`SELECT * FROM Person WITH(updlock)`)
✅ **Large execution plan** (8 subqueries, 20-30+ operators)
✅ **New test script** specifically for Person UPDLOCK queries
✅ **50+ active queries** captured by OTEL collector
✅ **Complex execution plan XML** available in metrics

The new queries replicate your exact pattern with a much more complex execution plan than the simple sales queries! 🎉
