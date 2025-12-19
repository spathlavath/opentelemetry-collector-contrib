@echo off
SET BASE_URL=http://localhost:8080

echo ==========================================
echo 50 Active Person UPDLOCK Queries Test
echo ==========================================
echo.
echo This script will:
echo   1. Run 1 SLOW complex Person UPDLOCK query (1-minute delay)
echo   2. Run 50 ACTIVE complex Person UPDLOCK queries (90-second delay each)
echo.
echo Query Pattern:
echo   SELECT * FROM Person WITH(UPDLOCK) WHERE BusinessEntityID=@
echo.
echo Features:
echo   - Complex subqueries (Email, Phone, Address, Sales)
echo   - Large execution plan (8+ subqueries)
echo   - UPDLOCK hint (creates LCK_M_U wait type)
echo   - Matches your real application pattern
echo.
echo This simulates:
echo   - 1 slow query captured in sys.dm_exec_query_stats
echo   - 50 active running queries in sys.dm_exec_requests
echo   - Complex execution plans for analysis
echo.
echo ==========================================
echo.

pause

echo.
echo [1/51] Starting 1-minute SLOW complex Person UPDLOCK query at %date% %time%...
start /B curl -s "%BASE_URL%/api/users/slow-complex-person-updlock-query" > %TEMP%\slow-person-updlock-1min.log 2>&1
echo Slow Person UPDLOCK query (1 min) started
echo.

REM Wait 2 seconds for slow query to start
timeout /t 2 /nobreak > nul

echo Starting 50 ACTIVE complex Person UPDLOCK queries (90 seconds each)...
echo.

REM Start 50 active Person UPDLOCK queries
FOR /L %%i IN (1,1,50) DO (
    start /B curl -s "%BASE_URL%/api/users/active-complex-person-updlock-query" > %TEMP%\active-person-updlock-%%i.log 2>&1
    echo [%%i/50] Active Person UPDLOCK query %%i started
    timeout /t 0 /nobreak > nul
)

echo.
echo ==========================================
echo All queries started!
echo ==========================================
echo.
echo Status:
echo   - 1 slow Person UPDLOCK query (1 minute) running
echo   - 50 active Person UPDLOCK queries (90 seconds each) running
echo   - Total: 51 concurrent queries
echo.
echo Query Pattern: SELECT * FROM Person WITH(UPDLOCK) WHERE BusinessEntityID=@
echo.
echo Complex Execution Plan includes:
echo   1. Person table scan with UPDLOCK
echo   2. EmailAddress subquery
echo   3. PersonPhone subquery
echo   4. BusinessEntityAddress + Address + StateProvince JOIN
echo   5. Customer existence check
echo   6. Employee existence check
echo   7. Sales order count aggregation
echo   8. Total sales amount aggregation
echo.
echo ==========================================
echo While queries are running, check:
echo ==========================================
echo.
echo 1. SQL Server - Count active Person UPDLOCK queries:
echo    SELECT COUNT(*) AS active_count
echo    FROM sys.dm_exec_requests
echo    WHERE text LIKE '%%COMPLEX_PERSON_UPDLOCK%%';
echo.
echo 2. SQL Server - View active Person queries with details:
echo    SELECT
echo        session_id,
echo        start_time,
echo        status,
echo        wait_type,
echo        wait_time,
echo        blocking_session_id,
echo        total_elapsed_time / 1000 AS elapsed_seconds,
echo        SUBSTRING(qt.text, 1, 200) AS query_text
echo    FROM sys.dm_exec_requests er
echo    CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
echo    WHERE qt.text LIKE '%%Person.Person%%UPDLOCK%%';
echo.
echo 3. SQL Server - Check execution plan:
echo    SELECT
echo        qs.query_hash,
echo        qs.query_plan_hash,
echo        qs.execution_count,
echo        qs.total_elapsed_time / 1000 AS total_elapsed_ms,
echo        CAST(qp.query_plan AS XML) AS execution_plan
echo    FROM sys.dm_exec_query_stats qs
echo    CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
echo    WHERE qs.sql_handle IN (
echo        SELECT sql_handle FROM sys.dm_exec_requests
echo        WHERE text LIKE '%%COMPLEX_PERSON_UPDLOCK%%'
echo    );
echo.
echo 4. OTEL Collector - Should capture all active Person UPDLOCK queries
echo.
echo 5. New Relic - After 2 minutes:
echo    SELECT count(*), latest(query_text)
echo    FROM Metric
echo    WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
echo      AND query_text LIKE '%%COMPLEX_PERSON_UPDLOCK%%'
echo    FACET query_id
echo    SINCE 5 minutes ago
echo.
echo Logs saved to:
echo   %TEMP%\slow-person-updlock-1min.log
echo   %TEMP%\active-person-updlock-*.log
echo.
echo ==========================================
echo Waiting for queries to complete...
echo ==========================================
echo.
echo Timeline:
echo   - 60 seconds:  Slow query (1 min) completes
echo   - 90 seconds:  Active queries (90 sec) complete
echo.
echo Press Ctrl+C to exit early (queries will continue in background)
echo.

REM Wait for all queries to complete (90 seconds + buffer)
timeout /t 95 /nobreak

echo.
echo ==========================================
echo Test Complete!
echo ==========================================
echo All queries should have finished.
echo.
echo Verification Steps:
echo.
echo 1. Check logs for errors:
echo    type %TEMP%\slow-person-updlock-1min.log
echo    type %TEMP%\active-person-updlock-1.log
echo.
echo 2. Check SQL Server for execution plans:
echo    SELECT TOP 10
echo        qs.query_hash,
echo        qs.execution_count,
echo        qs.total_elapsed_time / 1000 AS avg_elapsed_ms,
echo        SUBSTRING(st.text, 1, 100) AS query_preview
echo    FROM sys.dm_exec_query_stats qs
echo    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
echo    WHERE st.text LIKE '%%COMPLEX_PERSON_UPDLOCK%%'
echo    ORDER BY qs.last_execution_time DESC;
echo.
echo 3. Check New Relic for metrics:
echo    - Slow query metrics: Look for SLOW_COMPLEX_PERSON_UPDLOCK
echo    - Active query metrics: Look for COMPLEX_PERSON_UPDLOCK_BIG_PLAN
echo    - Should see 50+ captures in active queries
echo.
echo ==========================================
echo.
pause
