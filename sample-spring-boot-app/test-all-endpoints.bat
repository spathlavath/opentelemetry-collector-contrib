@echo off
SET BASE_URL=http://localhost:8080

echo ==========================================
echo Testing All Person UPDLOCK Endpoints
echo ==========================================
echo.

echo 1. Testing Health Endpoint...
curl -s %BASE_URL%/api/health
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ERROR: Health endpoint failed!
    echo Please start the application first:
    echo   start-with-apm.bat
    pause
    exit /b 1
)
echo.
echo Health check passed!
echo.

echo 2. Testing Person UPDLOCK Endpoints...
echo.
echo Note: These queries take 60-90 seconds to complete.
echo We'll just verify they START correctly (not wait for completion).
echo.

echo Testing: Simple Person UPDLOCK
echo URL: %BASE_URL%/api/users/active-person-updlock-query
start /B curl -s %BASE_URL%/api/users/active-person-updlock-query > %TEMP%\test-person-updlock-1.log 2>&1
timeout /t 2 /nobreak > nul
echo Status: Started (check log: %TEMP%\test-person-updlock-1.log)
echo.

echo Testing: Complex Person UPDLOCK (Active - 90 sec)
echo URL: %BASE_URL%/api/users/active-complex-person-updlock-query
start /B curl -s %BASE_URL%/api/users/active-complex-person-updlock-query > %TEMP%\test-complex-active.log 2>&1
timeout /t 2 /nobreak > nul
echo Status: Started (check log: %TEMP%\test-complex-active.log)
echo.

echo Testing: Complex Person UPDLOCK (Slow - 60 sec)
echo URL: %BASE_URL%/api/users/slow-complex-person-updlock-query
start /B curl -s %BASE_URL%/api/users/slow-complex-person-updlock-query > %TEMP%\test-complex-slow.log 2>&1
timeout /t 2 /nobreak > nul
echo Status: Started (check log: %TEMP%\test-complex-slow.log)
echo.

echo ==========================================
echo SQL Query Patterns Used
echo ==========================================
echo.
echo These endpoints execute queries on these tables:
echo.
echo [YES] 1. Person.Person (with UPDLOCK)
echo [YES] 2. Person.EmailAddress (subquery)
echo [YES] 3. Person.PersonPhone (subquery)
echo [YES] 4. Person.BusinessEntityAddress (JOIN)
echo [YES] 5. Person.Address (JOIN)
echo [YES] 6. Person.StateProvince (JOIN)
echo [YES] 7. Sales.Customer (subquery)
echo [YES] 8. HumanResources.Employee (subquery)
echo [YES] 9. Sales.SalesOrderHeader (subquery aggregation)
echo.
echo All queries include WITH(UPDLOCK) on Person.Person table
echo.

echo ==========================================
echo Verify in SQL Server (RUN NOW)
echo ==========================================
echo.
echo The queries above are now running (3 active queries).
echo.
echo Run this in SQL Server to see them:
echo.
echo SELECT
echo     session_id,
echo     start_time,
echo     total_elapsed_time / 1000 AS elapsed_sec,
echo     wait_type,
echo     SUBSTRING(qt.text, 1, 150) AS query_text
echo FROM sys.dm_exec_requests er
echo CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
echo WHERE qt.text LIKE '%%Person.Person%%UPDLOCK%%';
echo.
echo Expected: Should see 3 active queries
echo.

echo ==========================================
echo Check Logs for Errors
echo ==========================================
echo.
echo After queries complete (wait 90 seconds), check logs:
echo.
echo   type %TEMP%\test-person-updlock-1.log
echo   type %TEMP%\test-complex-active.log
echo   type %TEMP%\test-complex-slow.log
echo.
echo If logs show JSON responses, endpoints are working correctly!
echo If logs show errors, there may be a problem with the queries.
echo.

echo ==========================================
echo Next Steps
echo ==========================================
echo.
echo If all endpoints are working, run the full test:
echo.
echo   test-50-active-queries-person-updlock.bat
echo.
echo This will fire 1 SLOW + 50 ACTIVE Person UPDLOCK queries
echo with large execution plans that hit all 9 tables above.
echo.
echo ==========================================
pause
