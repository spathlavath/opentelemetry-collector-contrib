#!/bin/bash

# Test Active Queries - Guaranteed Capture Script
# This script runs queries that will DEFINITELY be captured as active queries
# by the OTEL collector (60-second scrape interval)

echo "======================================================================"
echo "🎯 Active Query Test - GUARANTEED CAPTURE"
echo "======================================================================"
echo ""
echo "This script will execute long-running queries that will be visible"
echo "in sys.dm_exec_requests when the OTEL collector scrapes (every 60s)."
echo ""
echo "Query Patterns:"
echo "  1. Active Sales Query      - 90 seconds  (1.5x scrape interval)"
echo "  2. Active Aggregation      - 120 seconds (2x scrape interval)"
echo "  3. Active Blocking Query   - 90 seconds  (with LCK_M_X waits)"
echo "  4. Active CPU Query        - Variable    (CPU-intensive)"
echo "  5. Active I/O Query        - Variable    (I/O-intensive)"
echo ""
echo "Each query runs long enough to guarantee capture!"
echo "======================================================================"
echo ""

BASE_URL="http://localhost:8080/api/users"

# Function to run query in background
run_query() {
    local endpoint=$1
    local description=$2
    local count=$3

    echo "🚀 Starting: $description"
    echo "   Endpoint: $endpoint"
    echo "   Count: $count concurrent queries"
    echo ""

    for ((i=1; i<=$count; i++)); do
        echo "   [$i/$count] Starting query..."
        curl -s "$BASE_URL/$endpoint" > /dev/null 2>&1 &
        sleep 0.5  # Small delay between starts
    done

    echo "✅ All $count queries started for: $description"
    echo ""
}

# Display instructions
echo "📋 INSTRUCTIONS:"
echo "1. Make sure the Spring Boot application is running"
echo "2. Make sure the OTEL Collector is running with 60-second scrape interval"
echo "3. This script will start all active queries in parallel"
echo "4. Queries will run for 90-120 seconds"
echo "5. Monitor SQL Server DMVs or New Relic for results"
echo ""
echo "Press ENTER to start the test..."
read

echo "======================================================================"
echo "🏁 STARTING ACTIVE QUERY TEST"
echo "======================================================================"
echo ""

# Record start time
START_TIME=$(date +%s)

# Pattern 1: Active Sales Query (90 seconds) - 10 concurrent
run_query "active-sales-query" "Active Sales Query (90s)" 10

# Small delay before next pattern
sleep 2

# Pattern 2: Active Aggregation Query (120 seconds) - 5 concurrent
run_query "active-aggregation-query" "Active Aggregation Query (120s)" 5

# Small delay before next pattern
sleep 2

# Pattern 3: Active Blocking Query (90 seconds) - 3 concurrent
# WARNING: These will create blocking scenarios
run_query "active-blocking-query" "Active Blocking Query (90s with locks)" 3

# Small delay before next pattern
sleep 2

# Pattern 4: Active CPU Query - 2 concurrent
run_query "active-cpu-query" "Active CPU-Intensive Query" 2

# Small delay before next pattern
sleep 2

# Pattern 5: Active I/O Query - 5 concurrent
run_query "active-io-query" "Active I/O-Intensive Query" 5

echo "======================================================================"
echo "✅ ALL QUERIES STARTED SUCCESSFULLY"
echo "======================================================================"
echo ""
echo "Total queries running: 25"
echo "  - Active Sales: 10"
echo "  - Active Aggregation: 5"
echo "  - Active Blocking: 3"
echo "  - Active CPU: 2"
echo "  - Active I/O: 5"
echo ""
echo "======================================================================"
echo "⏰ TIMING INFORMATION"
echo "======================================================================"
echo ""
echo "Start Time: $(date)"
echo ""
echo "Expected completion timeline:"
echo "  - 90 seconds:  Active Sales, Blocking queries complete"
echo "  - 120 seconds: Active Aggregation queries complete"
echo "  - Variable:    CPU and I/O queries (check progress)"
echo ""
echo "======================================================================"
echo "📊 MONITORING QUERIES"
echo "======================================================================"
echo ""
echo "Run these SQL queries to monitor progress:"
echo ""
echo "1. Count active queries:"
echo "   SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id > 50;"
echo ""
echo "2. View active queries with details:"
echo "   SELECT"
echo "       session_id,"
echo "       start_time,"
echo "       status,"
echo "       command,"
echo "       wait_type,"
echo "       wait_time,"
echo "       total_elapsed_time / 1000 AS elapsed_seconds,"
echo "       SUBSTRING(qt.text, 1, 100) AS query_text"
echo "   FROM sys.dm_exec_requests er"
echo "   CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt"
echo "   WHERE session_id > 50"
echo "   ORDER BY total_elapsed_time DESC;"
echo ""
echo "3. Find blocking queries:"
echo "   SELECT"
echo "       blocking_session_id,"
echo "       session_id,"
echo "       wait_type,"
echo "       wait_time,"
echo "       wait_resource"
echo "   FROM sys.dm_exec_requests"
echo "   WHERE blocking_session_id > 0;"
echo ""
echo "======================================================================"
echo "🔍 NEW RELIC VERIFICATION"
echo "======================================================================"
echo ""
echo "After queries run for 60+ seconds, check New Relic with:"
echo ""
echo "SELECT latest(sqlserver.activequery.wait_time_seconds)"
echo "FROM Metric"
echo "WHERE metricName = 'sqlserver.activequery.wait_time_seconds'"
echo "FACET query_id, session_id, wait_type"
echo "SINCE 5 minutes ago"
echo ""
echo "Look for queries with:"
echo "  - query_text containing 'APM_ACTIVE_'"
echo "  - elapsed_time > 5000ms"
echo "  - Various wait_types (WAITFOR, LCK_M_X, etc.)"
echo ""
echo "======================================================================"
echo "⏳ Waiting for queries to complete..."
echo "======================================================================"
echo ""
echo "This will take approximately 120 seconds..."
echo "You can press Ctrl+C to exit (queries will continue in background)"
echo ""

# Wait for queries to complete
wait

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "======================================================================"
echo "✅ TEST COMPLETE"
echo "======================================================================"
echo ""
echo "Total Duration: $DURATION seconds"
echo "End Time: $(date)"
echo ""
echo "Next Steps:"
echo "1. Check OTEL Collector logs for active query metrics"
echo "2. Query New Relic for sqlserver.activequery.* metrics"
echo "3. Verify query_id correlation between slow and active queries"
echo ""
echo "======================================================================"
