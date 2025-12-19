@echo off
SET BASE_URL=http://localhost:8080

echo ==========================================
echo Long-Running Query Test
echo ==========================================
echo.
echo This script will execute long-running SQL queries that:
echo   - Use WAITFOR DELAY to pause for 5 minutes
echo   - Simulate stuck/slow query scenarios
echo   - Should be captured by the OTEL collector as active running queries
echo.
echo Available queries:
echo   1. Simple users table query
echo   2. Complex AdventureWorks2022 Sales query (with JOINs)
echo   3. Complex AdventureWorks2022 Product query (with aggregations)
echo.
echo ==========================================
echo.

echo Select which queries to run:
echo 1) All queries (parallel)
echo 2) Simple users query only
echo 3) Sales query only
echo 4) Product query only
echo.
set /p choice="Enter choice (1-4): "
echo.

if "%choice%"=="1" goto run_all
if "%choice%"=="2" goto run_users
if "%choice%"=="3" goto run_sales
if "%choice%"=="4" goto run_product

echo Invalid choice
pause
exit /b 1

:run_all
echo Starting ALL slow queries at %date% %time%...
echo Each will take approximately 5 minutes to complete.
echo.

start /B curl -v "%BASE_URL%/api/users/slow-query" > %TEMP%\slow-query-users.log 2>&1
echo 1. Users query started

start /B curl -v "%BASE_URL%/api/users/slow-sales-query" > %TEMP%\slow-query-sales.log 2>&1
echo 2. Sales query started

start /B curl -v "%BASE_URL%/api/users/slow-product-query" > %TEMP%\slow-query-product.log 2>&1
echo 3. Product query started

echo.
echo All 3 queries are now running in parallel!
echo Logs saved to %TEMP%\slow-query-*.log
goto monitor

:run_users
echo Starting simple users query at %date% %time%...
curl -v "%BASE_URL%/api/users/slow-query"
goto done

:run_sales
echo Starting AdventureWorks2022 Sales query at %date% %time%...
curl -v "%BASE_URL%/api/users/slow-sales-query"
goto done

:run_product
echo Starting AdventureWorks2022 Product query at %date% %time%...
curl -v "%BASE_URL%/api/users/slow-product-query"
goto done

:monitor
echo.
echo While the queries are running:
echo   1. Check your OTEL collector logs for active query detection
echo   2. Query sys.dm_exec_requests in SQL Server to see the active queries
echo   3. The queries should appear in New Relic with high duration (~300s)
echo.
echo To check running queries in SQL Server:
echo   SELECT session_id, start_time, status, command, wait_type,
echo          SUBSTRING(qt.text, 1, 500) AS query_text
echo   FROM sys.dm_exec_requests er
echo   CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
echo   WHERE qt.text LIKE '%%WAITFOR DELAY%%'
echo.
goto done

:done
echo.
echo ==========================================
echo Test Complete!
echo ==========================================
echo.
pause
