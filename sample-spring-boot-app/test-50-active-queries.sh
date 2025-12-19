#!/bin/bash

BASE_URL="http://localhost:8080"

echo "=========================================="
echo "50 Active Running Queries Test"
echo "=========================================="
echo ""
echo "This script will:"
echo "  1. Run 1 slow sales query with 1-minute delay"
echo "  2. Run 50 active sales queries with 5-second delay (concurrently)"
echo ""
echo "This simulates:"
echo "  - 1 slow query captured in query stats"
echo "  - 50 active running queries visible in sys.dm_exec_requests"
echo ""
echo "=========================================="
echo ""

read -p "Press Enter to start the test (or Ctrl+C to cancel)..."
echo ""

# Start the 1-minute slow query
echo "[1/51] Starting 1-minute slow sales query at $(date)..."
curl -s "$BASE_URL/api/users/slow-sales-query" > /tmp/slow-sales-1min.log 2>&1 &
SLOW_PID=$!
echo "Slow query (1 min) started with PID: $SLOW_PID"
echo ""

# Wait 2 seconds to let the slow query start
sleep 2

# Start 50 active queries with 5-second delay
echo "Starting 50 active queries (5-second delay each)..."
echo ""

for i in {1..50}; do
    curl -s "$BASE_URL/api/users/active-sales-query" > /tmp/active-sales-$i.log 2>&1 &
    PIDS[$i]=$!
    echo "[$((i+1))/51] Active query $i started (PID: ${PIDS[$i]})"

    # Small delay to stagger the starts slightly
    sleep 0.1
done

echo ""
echo "=========================================="
echo "All queries started!"
echo "=========================================="
echo ""
echo "Status:"
echo "  - 1 slow query (1 minute) running"
echo "  - 50 active queries (5 seconds each) running"
echo "  - Total: 51 concurrent queries"
echo ""
echo "While queries are running, check:"
echo ""
echo "1. SQL Server - Count active queries:"
echo "   SELECT COUNT(*) AS active_count"
echo "   FROM sys.dm_exec_requests"
echo "   WHERE text LIKE '%WAITFOR DELAY%';"
echo ""
echo "2. SQL Server - View all active queries:"
echo "   SELECT session_id, start_time, wait_type, wait_time,"
echo "          total_elapsed_time / 1000 AS elapsed_seconds,"
echo "          SUBSTRING(qt.text, 1, 100) AS query_text"
echo "   FROM sys.dm_exec_requests er"
echo "   CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt"
echo "   WHERE qt.text LIKE '%WAITFOR DELAY%';"
echo ""
echo "3. OTEL Collector - Should capture all active running queries"
echo ""
echo "Logs saved to /tmp/slow-sales-1min.log and /tmp/active-sales-*.log"
echo ""
echo "=========================================="
echo "Waiting for queries to complete..."
echo "=========================================="
echo ""

# Monitor completion
COMPLETED=0
TOTAL=51

while [ $COMPLETED -lt $TOTAL ]; do
    sleep 1
    RUNNING=$(jobs -r | wc -l)
    COMPLETED=$((TOTAL - RUNNING))
    echo -ne "\rCompleted: $COMPLETED / $TOTAL queries"
done

echo ""
echo ""
echo "=========================================="
echo "Test Complete!"
echo "=========================================="
echo "All queries have finished."
echo ""
