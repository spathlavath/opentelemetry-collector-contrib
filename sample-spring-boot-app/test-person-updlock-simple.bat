@echo off
SET BASE_URL=http://localhost:8080

echo ==========================================
echo Person UPDLOCK Test - Simple Scenario
echo ==========================================
echo.
echo This test will:
echo   1. Fire 10 concurrent Person UPDLOCK queries
echo   2. Each query holds UPDLOCK for 90 seconds
echo   3. Creates blocking (first query blocks others)
echo   4. Collector (15s interval) will capture:
echo      - Active queries in sys.dm_exec_requests
echo      - Blocking session IDs
echo      - Wait events (LCK_M_U)
echo      - Execution plan
echo.
echo Query Pattern:
echo   SELECT * FROM Person.Person WITH(UPDLOCK)
echo   WHERE BusinessEntityID BETWEEN 1 AND 50
echo.
echo ==========================================
echo.

pause

echo.
echo Starting 10 concurrent Person UPDLOCK queries...
echo.

REM Start 10 queries in background
FOR /L %%i IN (1,1,10) DO (
    start /B curl -s "%BASE_URL%/api/users/person-updlock-query" > %TEMP%\person-updlock-%%i.log 2>&1
    echo [%%i/10] Query %%i started
    timeout /t 1 /nobreak > nul
)

echo.
echo ==========================================
echo All 10 queries started!
echo ==========================================
echo.
echo Status:
echo   - 10 concurrent queries running
echo   - First query holds UPDLOCK
echo   - Other 9 queries blocked (waiting for lock)
echo   - Each query runs for 90 seconds
echo.
echo ==========================================
echo SQL Server Monitoring
echo ==========================================
echo.
echo 1. Check active queries (run NOW in SQL Server):
echo.
echo SELECT
echo     session_id,
echo     status,
echo     wait_type,
echo     wait_time,
echo     blocking_session_id,
echo     total_elapsed_time / 1000 AS elapsed_sec,
echo     SUBSTRING(qt.text, 1, 200) AS query_text
echo FROM sys.dm_exec_requests er
echo CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
echo WHERE qt.text LIKE '%%PERSON_UPDLOCK_WITH_PLAN%%';
echo.
echo Expected: 10 rows
echo   - 1st query: status='running', wait_type='WAITFOR', blocking_session_id=NULL
echo   - Other 9: status='suspended', wait_type='LCK_M_U', blocking_session_id=^<first session^>
echo.
echo 2. Check execution plan:
echo.
echo SELECT
echo     qs.query_hash,
echo     qs.query_plan_hash,
echo     CAST(qp.query_plan AS XML) AS execution_plan
echo FROM sys.dm_exec_query_stats qs
echo CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
echo CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
echo WHERE st.text LIKE '%%PERSON_UPDLOCK_WITH_PLAN%%';
echo.
echo ==========================================
echo Waiting for queries to complete (95 seconds)...
echo ==========================================
echo.

REM Wait for queries to complete
timeout /t 95 /nobreak

echo.
echo ==========================================
echo Test Complete!
echo ==========================================
echo.
echo Check logs:
FOR /L %%i IN (1,1,10) DO (
    echo   %TEMP%\person-updlock-%%i.log
)
echo.
echo Verification:
echo.
echo 1. Check New Relic for active query metrics:
echo.
echo SELECT
echo     count(*) AS total_captures,
echo     latest(query_text),
echo     latest(blocking_session_id),
echo     latest(wait_type)
echo FROM Metric
echo WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
echo   AND query_text LIKE '%%PERSON_UPDLOCK_WITH_PLAN%%'
echo SINCE 5 minutes ago
echo.
echo Expected: Multiple captures showing blocking relationships
echo.
echo ==========================================
echo.
pause
