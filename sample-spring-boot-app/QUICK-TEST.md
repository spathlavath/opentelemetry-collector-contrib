# Quick Test Guide for Slow Query

## Quick Start

```bash
# Start the application with APM
./start-with-apm.sh

# In another terminal, trigger the slow queries
curl http://localhost:8080/api/users/slow-query &
curl http://localhost:8080/api/users/slow-sales-query &
curl http://localhost:8080/api/users/slow-product-query &

# Or use the dedicated test script
./test-slow-query.sh
```

## Available Slow Query Endpoints

### 1. Simple Slow Query (Users Table)
**Endpoint:** `/api/users/slow-query`
**Duration:** ~5 minutes (300 seconds)
**SQL Query:**
```sql
WAITFOR DELAY '00:05:00'; SELECT * FROM users
```

### 2. Complex Sales Query (AdventureWorks2022)
**Endpoint:** `/api/users/slow-sales-query`
**Duration:** ~5 minutes (300 seconds)
**SQL Query:** Complex JOIN across Sales, Product, Customer, and Person tables
- Filters: OrderDate >= '2013-01-01', TotalDue > $5000, ListPrice > $100
- Returns: Order details with customer names and products

### 3. Complex Product Query (AdventureWorks2022)
**Endpoint:** `/api/users/slow-product-query`
**Duration:** ~5 minutes (300 seconds)
**SQL Query:** Aggregation with multiple JOINs across Product, Inventory, and Purchasing tables
- Filters: FinishedGoodsFlag = 1, ListPrice > $50, Categories: Bikes/Components/Clothing
- Returns: Product profitability analysis with inventory data

## Quick Verification

### 1. Check if Query is Running (SQL Server)
```sql
SELECT session_id, status, wait_type, wait_time,
       CAST(qt.text AS VARCHAR(MAX)) AS query_text
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
WHERE qt.text LIKE '%WAITFOR DELAY%';
```

### 2. Check OTEL Collector Output
```bash
# If running collector in terminal, watch for:
# - Active query metrics
# - Query text containing "WAITFOR DELAY"
# - Duration metrics increasing
```

### 3. Check New Relic
- APM > Transactions > Sort by duration
- Should see transaction with ~300s duration
- Click to view transaction trace

## Tips

- **Test in background:** Add `&` at the end of curl command
- **Multiple queries:** Run curl multiple times to test concurrent slow queries
- **Quick test:** Modify delay to `00:00:30` for 30-second test
- **Cancel query:** Use `KILL <session_id>` in SQL Server

## Endpoint Details

| Property | Value |
|----------|-------|
| URL | `http://localhost:8080/api/users/slow-query` |
| Method | GET |
| Duration | 5 minutes |
| Response | Array of users (after delay) |

## Common Issues

**Query not captured:**
- Collector scrape interval might be too long
- Check collector is running and connected to SQL Server

**Query completes too fast:**
- Check WAITFOR DELAY syntax in UserRepository.java
- Verify query is executing on SQL Server

**Timeout errors:**
- Increase client timeout settings
- Check application.properties for query timeout settings
