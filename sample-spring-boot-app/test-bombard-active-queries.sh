#!/bin/bash

# Bombard Active Queries - Continuous 5-Minute Test
# This script continuously fires the same query for 5 minutes
# Guarantees that multiple collector scrapes will capture active queries

echo "======================================================================"
echo "💣 BOMBARD TEST - Continuous Active Queries for 5 Minutes"
echo "======================================================================"
echo ""
echo "This script will continuously start new queries for 5 minutes."
echo "Each query runs for 90 seconds, creating overlapping active queries."
echo ""
echo "Strategy:"
echo "  - Fire a new query every 10 seconds"
echo "  - Each query runs for 90 seconds"
echo "  - Result: ~9 queries running concurrently at any time"
echo "  - Duration: 5 minutes (300 seconds)"
echo ""
echo "With 60-second collector scrapes, you'll get 5 scrapes during test."
echo "Each scrape will see 8-9 active queries!"
echo "======================================================================"
echo ""

BASE_URL="http://localhost:8080/api/users"
DURATION=300  # 5 minutes in seconds
INTERVAL=10   # Fire new query every 10 seconds
QUERY_COUNT=0

# Function to start a query
start_query() {
    local query_num=$1
    local endpoint=$2

    echo "[$(date +%H:%M:%S)] 🚀 Starting Query #$query_num - Endpoint: $endpoint"
    curl -s "$BASE_URL/$endpoint" > /dev/null 2>&1 &
    QUERY_COUNT=$((QUERY_COUNT + 1))
}

# Display instructions
echo "📋 SELECT QUERY PATTERN TO BOMBARD:"
echo ""
echo "1. Active Sales Query (90s)        - Complex JOIN with WAITFOR"
echo "2. Active Aggregation (120s)       - Time-series aggregation"
echo "3. Active Blocking (90s)           - WITH (UPDLOCK, HOLDLOCK)"
echo "4. Active CPU (variable)           - CPU-intensive computation"
echo "5. Active I/O (variable)           - I/O-intensive cross join"
echo "6. ALL PATTERNS (mixed)            - Rotate through all patterns"
echo ""
echo -n "Enter choice (1-6): "
read CHOICE

case $CHOICE in
    1)
        ENDPOINT="active-sales-query"
        PATTERN="Active Sales Query"
        ;;
    2)
        ENDPOINT="active-aggregation-query"
        PATTERN="Active Aggregation Query"
        ;;
    3)
        ENDPOINT="active-blocking-query"
        PATTERN="Active Blocking Query"
        ;;
    4)
        ENDPOINT="active-cpu-query"
        PATTERN="Active CPU Query"
        ;;
    5)
        ENDPOINT="active-io-query"
        PATTERN="Active I/O Query"
        ;;
    6)
        ENDPOINT="ALL"
        PATTERN="Mixed Pattern Rotation"
        ;;
    *)
        echo "Invalid choice. Defaulting to Active Sales Query."
        ENDPOINT="active-sales-query"
        PATTERN="Active Sales Query"
        ;;
esac

echo ""
echo "======================================================================"
echo "🎯 BOMBARD CONFIGURATION"
echo "======================================================================"
echo ""
echo "Pattern: $PATTERN"
echo "Endpoint: $ENDPOINT"
echo "Duration: 5 minutes (300 seconds)"
echo "Query Interval: Every $INTERVAL seconds"
echo "Expected Concurrent Queries: 8-9"
echo "Total Queries to Fire: ~30"
echo ""
echo "Press ENTER to start bombardment..."
read

echo "======================================================================"
echo "💣 STARTING BOMBARDMENT"
echo "======================================================================"
echo ""

START_TIME=$(date +%s)
CURRENT_TIME=$START_TIME

# Main bombardment loop
while [ $((CURRENT_TIME - START_TIME)) -lt $DURATION ]; do
    ELAPSED=$((CURRENT_TIME - START_TIME))
    REMAINING=$((DURATION - ELAPSED))

    # Select endpoint for mixed pattern
    if [ "$ENDPOINT" = "ALL" ]; then
        PATTERNS=(
            "active-sales-query"
            "active-aggregation-query"
            "active-blocking-query"
            "active-cpu-query"
            "active-io-query"
        )
        CURRENT_ENDPOINT="${PATTERNS[$((QUERY_COUNT % 5))]}"
    else
        CURRENT_ENDPOINT="$ENDPOINT"
    fi

    # Start new query
    start_query $QUERY_COUNT "$CURRENT_ENDPOINT"

    # Show progress
    echo "    ⏱️  Elapsed: ${ELAPSED}s / ${DURATION}s | Remaining: ${REMAINING}s | Total Fired: $QUERY_COUNT"

    # Wait before next query
    sleep $INTERVAL

    CURRENT_TIME=$(date +%s)
done

echo ""
echo "======================================================================"
echo "✅ BOMBARDMENT COMPLETE"
echo "======================================================================"
echo ""
echo "Total Queries Fired: $QUERY_COUNT"
echo "Duration: $(($(date +%s) - START_TIME)) seconds"
echo "End Time: $(date +%H:%M:%S)"
echo ""
echo "======================================================================"
echo "📊 MONITORING COMMANDS"
echo "======================================================================"
echo ""
echo "1. Check active query count in SQL Server:"
echo ""
echo "SELECT COUNT(*) AS active_count"
echo "FROM sys.dm_exec_requests"
echo "WHERE session_id > 50;"
echo ""
echo "2. View all active queries:"
echo ""
echo "SELECT"
echo "    session_id,"
echo "    start_time,"
echo "    status,"
echo "    wait_type,"
echo "    wait_time,"
echo "    total_elapsed_time / 1000 AS elapsed_sec,"
echo "    blocking_session_id,"
echo "    SUBSTRING(qt.text, 1, 150) AS query_text"
echo "FROM sys.dm_exec_requests er"
echo "CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt"
echo "WHERE session_id > 50"
echo "ORDER BY total_elapsed_time DESC;"
echo ""
echo "3. Group by query pattern:"
echo ""
echo "SELECT"
echo "    CASE"
echo "        WHEN qt.text LIKE '%APM_ACTIVE_AGGREGATION%' THEN 'Aggregation'"
echo "        WHEN qt.text LIKE '%APM_BLOCKING_PATTERN%' THEN 'Blocking'"
echo "        WHEN qt.text LIKE '%APM_CPU_INTENSIVE%' THEN 'CPU'"
echo "        WHEN qt.text LIKE '%APM_IO_INTENSIVE%' THEN 'I/O'"
echo "        ELSE 'Sales'"
echo "    END AS pattern_type,"
echo "    COUNT(*) AS query_count,"
echo "    AVG(total_elapsed_time / 1000) AS avg_elapsed_sec,"
echo "    MAX(total_elapsed_time / 1000) AS max_elapsed_sec"
echo "FROM sys.dm_exec_requests er"
echo "CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt"
echo "WHERE session_id > 50"
echo "GROUP BY CASE"
echo "    WHEN qt.text LIKE '%APM_ACTIVE_AGGREGATION%' THEN 'Aggregation'"
echo "    WHEN qt.text LIKE '%APM_BLOCKING_PATTERN%' THEN 'Blocking'"
echo "    WHEN qt.text LIKE '%APM_CPU_INTENSIVE%' THEN 'CPU'"
echo "    WHEN qt.text LIKE '%APM_IO_INTENSIVE%' THEN 'I/O'"
echo "    ELSE 'Sales'"
echo "END;"
echo ""
echo "======================================================================"
echo "🔍 NEW RELIC VERIFICATION"
echo "======================================================================"
echo ""
echo "Check active queries captured by collector:"
echo ""
echo "SELECT"
echo "    latest(sqlserver.activequery.elapsed_time_ms) / 1000 AS elapsed_sec,"
echo "    latest(query_text),"
echo "    latest(wait_type),"
echo "    latest(session_id),"
echo "    count(*) AS capture_count"
echo "FROM Metric"
echo "WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'"
echo "  AND elapsed_time_ms > 5000"
echo "FACET query_id"
echo "SINCE 10 minutes ago"
echo ""
echo "Expected Results:"
echo "  - 1-5 unique query_id values (depending on pattern choice)"
echo "  - Multiple capture_count per query_id (5+ scrapes)"
echo "  - elapsed_sec ranging from 5 to 90+ seconds"
echo "  - query_text containing 'APM_ACTIVE_' or similar"
echo ""
echo "======================================================================"
echo "⏳ WAIT TIME"
echo "======================================================================"
echo ""
echo "Queries will continue running for up to 120 more seconds."
echo "Monitor SQL Server or New Relic to see them complete."
echo ""
echo "Collector scrapes:"
echo "  - Scrape 1: ~60s  (should see ~6 queries)"
echo "  - Scrape 2: ~120s (should see ~9 queries)"
echo "  - Scrape 3: ~180s (should see ~9 queries)"
echo "  - Scrape 4: ~240s (should see ~9 queries)"
echo "  - Scrape 5: ~300s (should see ~9 queries)"
echo ""
echo "Total unique active query captures expected: 5 scrapes × 8-9 queries = 40-45 captures!"
echo ""
echo "======================================================================"

# Optional: Wait for queries to complete
echo ""
echo "Press ENTER to exit (queries will continue in background)..."
read
