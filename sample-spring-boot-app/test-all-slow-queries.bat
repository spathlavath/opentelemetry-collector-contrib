@echo off
REM Quick script to run all 3 slow queries in parallel

SET BASE_URL=http://localhost:8080

echo Starting all 3 slow queries in parallel...
echo.

start /B curl "%BASE_URL%/api/users/slow-query" > %TEMP%\slow-query-users.log 2>&1
echo [1/3] Users query started

start /B curl "%BASE_URL%/api/users/slow-sales-query" > %TEMP%\slow-query-sales.log 2>&1
echo [2/3] Sales query started

start /B curl "%BASE_URL%/api/users/slow-product-query" > %TEMP%\slow-query-product.log 2>&1
echo [3/3] Product query started

echo.
echo All queries running! Check OTEL collector for active queries.
echo Logs: %TEMP%\slow-query-*.log
echo.
pause
