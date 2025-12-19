# Slow Queries Reference

This document describes all the slow query endpoints available in the application for testing query performance monitoring.

## Overview

All queries use `WAITFOR DELAY '00:05:00'` to pause execution for 5 minutes, simulating stuck or long-running queries that should be captured by the OTEL collector.

---

## Query 1: Simple Users Table Query

**Endpoint:** `GET /api/users/slow-query`

**Purpose:** Basic slow query for testing

**SQL:**
```sql
WAITFOR DELAY '00:05:00';
SELECT * FROM users
```

**Test Command:**
```bash
curl http://localhost:8080/api/users/slow-query
```

---

## Query 2: Complex Sales Query (AdventureWorks2022)

**Endpoint:** `GET /api/users/slow-sales-query`

**Purpose:** Simulate a complex sales reporting query with multiple JOINs

**SQL:**
```sql
WAITFOR DELAY '00:05:00';

SELECT
    soh.SalesOrderID,
    soh.OrderDate,
    soh.TotalDue,
    p.Name AS ProductName,
    sod.OrderQty,
    sod.UnitPrice,
    c.FirstName + ' ' + c.LastName AS CustomerName
FROM AdventureWorks2022.Sales.SalesOrderHeader soh
INNER JOIN AdventureWorks2022.Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
INNER JOIN AdventureWorks2022.Production.Product p ON sod.ProductID = p.ProductID
INNER JOIN AdventureWorks2022.Sales.Customer cust ON soh.CustomerID = cust.CustomerID
INNER JOIN AdventureWorks2022.Person.Person c ON cust.PersonID = c.BusinessEntityID
WHERE soh.OrderDate >= '2013-01-01'
AND soh.TotalDue > 5000.00
AND p.ListPrice > 100.00
ORDER BY soh.TotalDue DESC
```

**Tables Used:**
- `Sales.SalesOrderHeader`
- `Sales.SalesOrderDetail`
- `Production.Product`
- `Sales.Customer`
- `Person.Person`

**Literal Values:**
- Date: `'2013-01-01'`
- Amount: `5000.00`
- Price: `100.00`

**Test Command:**
```bash
curl http://localhost:8080/api/users/slow-sales-query
```

**Expected Results:**
High-value sales orders with customer names and product details from 2013 onwards.

---

## Query 3: Complex Product Query (AdventureWorks2022)

**Endpoint:** `GET /api/users/slow-product-query`

**Purpose:** Simulate a complex product profitability analysis with aggregations

**SQL:**
```sql
WAITFOR DELAY '00:05:00';

SELECT
    pc.Name AS CategoryName,
    p.ProductNumber,
    p.Name AS ProductName,
    p.StandardCost,
    p.ListPrice,
    (p.ListPrice - p.StandardCost) AS ProfitMargin,
    SUM(pi.Quantity) AS TotalInventory,
    AVG(pod.OrderQty) AS AvgOrderQuantity
FROM AdventureWorks2022.Production.Product p
INNER JOIN AdventureWorks2022.Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
INNER JOIN AdventureWorks2022.Production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
LEFT JOIN AdventureWorks2022.Production.ProductInventory pi ON p.ProductID = pi.ProductID
LEFT JOIN AdventureWorks2022.Purchasing.PurchaseOrderDetail pod ON p.ProductID = pod.ProductID
WHERE p.FinishedGoodsFlag = 1
AND p.ListPrice > 50.00
AND pc.Name IN ('Bikes', 'Components', 'Clothing')
GROUP BY pc.Name, p.ProductNumber, p.Name, p.StandardCost, p.ListPrice
HAVING SUM(pi.Quantity) > 100
ORDER BY ProfitMargin DESC
```

**Tables Used:**
- `Production.Product`
- `Production.ProductSubcategory`
- `Production.ProductCategory`
- `Production.ProductInventory`
- `Purchasing.PurchaseOrderDetail`

**Literal Values:**
- Flag: `1` (FinishedGoodsFlag)
- Price: `50.00`
- Categories: `'Bikes'`, `'Components'`, `'Clothing'`
- Inventory: `100` (minimum quantity)

**Test Command:**
```bash
curl http://localhost:8080/api/users/slow-product-query
```

**Expected Results:**
Product profitability analysis with inventory levels for finished goods in Bikes, Components, and Clothing categories.

---

## Testing All Queries Simultaneously

To test multiple concurrent slow queries:

```bash
# Start all queries in parallel
curl http://localhost:8080/api/users/slow-query > /tmp/query1.log 2>&1 &
curl http://localhost:8080/api/users/slow-sales-query > /tmp/query2.log 2>&1 &
curl http://localhost:8080/api/users/slow-product-query > /tmp/query3.log 2>&1 &

# Check their PIDs
jobs -l
```

---

## Monitoring Active Queries

While queries are running, check SQL Server:

```sql
-- View all active queries with WAITFOR DELAY
SELECT
    er.session_id,
    er.start_time,
    er.status,
    er.command,
    er.wait_type,
    er.wait_time,
    er.total_elapsed_time / 1000 AS elapsed_seconds,
    SUBSTRING(qt.text, 1, 1000) AS query_text,
    c.client_net_address,
    s.program_name
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
LEFT JOIN sys.dm_exec_connections c ON er.session_id = c.session_id
LEFT JOIN sys.dm_exec_sessions s ON er.session_id = s.session_id
WHERE qt.text LIKE '%WAITFOR DELAY%'
AND er.session_id > 50
ORDER BY er.start_time;
```

---

## What the OTEL Collector Should Capture

For each slow query, the collector should report:

1. **Query Text:** Full SQL including `WAITFOR DELAY` statement
2. **Duration:** Increasing from 0 to ~300 seconds
3. **Session Info:** session_id, program_name, client_net_address
4. **Wait Info:** wait_type (likely `WAITFOR`), wait_time
5. **Database:** AdventureWorks2022
6. **Status:** Running/suspended

---

## Expected New Relic APM Behavior

1. **Transaction Traces:** Should show transactions lasting ~300+ seconds
2. **Database Queries:** Should list the slow queries with high duration
3. **Transaction Name:** Based on endpoint (e.g., `/api/users/slow-sales-query`)
4. **SQL Statement:** Should capture the full query text (depending on APM settings)

---

## Cleanup

To stop running queries:

**From Application:**
```bash
# Find and kill curl processes
pkill -f "curl.*slow-query"
```

**From SQL Server:**
```sql
-- Find session IDs
SELECT session_id FROM sys.dm_exec_requests
WHERE text LIKE '%WAITFOR DELAY%';

-- Kill the session (replace <session_id>)
KILL <session_id>;
```

---

## Notes

- All queries are designed to use real AdventureWorks2022 data
- Literal values are chosen to return meaningful results
- Queries cover different complexity levels: simple SELECT, complex JOIN, aggregation
- Each query includes common SQL Server patterns found in production systems
