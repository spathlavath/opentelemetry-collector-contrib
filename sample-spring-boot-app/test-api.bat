@echo off
SET BASE_URL=http://localhost:8080

echo ==========================================
echo Testing Spring Boot API Endpoints
echo ==========================================
echo.

echo 1. Testing health endpoint...
curl -s "%BASE_URL%/api/health"
echo.
echo.

echo 2. Testing hello endpoint...
curl -s "%BASE_URL%/api/hello"
echo.
echo.

echo 3. Creating test users...
FOR /L %%i IN (1,1,5) DO (
    echo Creating User %%i...
    curl -s -X POST "%BASE_URL%/api/users" ^
      -H "Content-Type: application/json" ^
      -d "{\"name\":\"Test User %%i\",\"email\":\"user%%i@example.com\",\"role\":\"User\"}"
    echo.
)
echo.

echo 4. Getting all users...
curl -s "%BASE_URL%/api/users"
echo.
echo.

echo 5. Getting user count...
curl -s "%BASE_URL%/api/users/count"
echo.
echo.

echo 6. Getting user by ID (ID=1)...
curl -s "%BASE_URL%/api/users/1"
echo.
echo.

echo 7. Getting user by email...
curl -s "%BASE_URL%/api/users/email/user1@example.com"
echo.
echo.

echo 8. Updating user (ID=1)...
curl -s -X PUT "%BASE_URL%/api/users/1" ^
  -H "Content-Type: application/json" ^
  -d "{\"name\":\"Updated User\",\"email\":\"updated@example.com\",\"role\":\"Admin\"}"
echo.
echo.

echo 9. Verifying update...
curl -s "%BASE_URL%/api/users/1"
echo.
echo.

echo 10. Generating load for APM testing...
FOR /L %%i IN (1,1,20) DO (
    curl -s "%BASE_URL%/api/users" > nul
    echo Request %%i sent
    timeout /t 1 /nobreak > nul
)
echo.
echo.

echo 11. Testing long-running query endpoint...
echo WARNING: This endpoint will run for approximately 5 minutes to simulate a stuck query.
echo This allows the OTEL collector to capture it as an active running query.
echo You can cancel with Ctrl+C or let it complete.
echo Starting slow query test in the background...
start /B curl -s "%BASE_URL%/api/users/slow-query" > nul
echo Slow query started in background
echo This query should now be visible in the OTEL collector's query performance monitoring.
echo.
echo.

echo ==========================================
echo API Testing Complete!
echo ==========================================
echo.
echo Next steps:
echo 1. Check New Relic APM dashboard for transaction data
echo 2. View transaction traces for slow queries
echo 3. Monitor JVM metrics and error rates
echo 4. Check OTEL collector for active running queries (the slow query endpoint)
echo.
