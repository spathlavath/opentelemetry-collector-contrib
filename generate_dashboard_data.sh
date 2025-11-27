#!/bin/bash

# SQL Server Load Generation and Data Verification Script
# This script will help generate database activity and verify metrics are flowing to New Relic

echo "🏢 SQL Server Dashboard Data Generation Script"
echo "=============================================="
echo ""

# Configuration
CONFIG_FILE="sql-onhost-config.yaml"
COLLECTOR_BINARY="./bin/otelcontribcol_darwin_arm64"
LOG_FILE="collector_output.log"

# Function to check if collector binary exists
check_collector() {
    if [ ! -f "$COLLECTOR_BINARY" ]; then
        echo "❌ Collector binary not found: $COLLECTOR_BINARY"
        echo "Please build the collector first with: make otelcontribcol"
        exit 1
    fi
    echo "✅ Collector binary found"
}

# Function to check if config file exists
check_config() {
    if [ ! -f "$CONFIG_FILE" ]; then
        echo "❌ Configuration file not found: $CONFIG_FILE"
        echo "Please ensure the configuration file exists"
        exit 1
    fi
    echo "✅ Configuration file found"
}

# Function to run the collector in background
start_collector() {
    echo ""
    echo "🚀 Starting OpenTelemetry Collector..."
    
    # Kill any existing collector processes
    pkill -f "otelcontribcol_darwin_arm64" 2>/dev/null || true
    
    # Start collector in background
    nohup $COLLECTOR_BINARY --config=$CONFIG_FILE > $LOG_FILE 2>&1 &
    COLLECTOR_PID=$!
    
    echo "📊 Collector started with PID: $COLLECTOR_PID"
    echo "📋 Logs are being written to: $LOG_FILE"
    
    # Wait a bit for collector to start
    sleep 5
    
    # Check if collector is still running
    if kill -0 $COLLECTOR_PID 2>/dev/null; then
        echo "✅ Collector is running successfully"
    else
        echo "❌ Collector failed to start. Check logs:"
        tail -20 $LOG_FILE
        exit 1
    fi
}

# Function to check collector logs for errors
check_collector_logs() {
    echo ""
    echo "🔍 Checking collector logs for errors..."
    
    if grep -i "error\|failed\|fatal" $LOG_FILE | tail -5; then
        echo "⚠️  Found errors in collector logs. Recent errors shown above."
    else
        echo "✅ No recent errors found in collector logs"
    fi
    
    echo ""
    echo "📈 Recent collector activity:"
    tail -10 $LOG_FILE | grep -E "(metrics|exported|success)" || echo "No metric export activity found yet"
}

# Function to show SQL commands for load generation
show_sql_commands() {
    echo ""
    echo "🗃️  SQL COMMANDS TO GENERATE DATABASE LOAD"
    echo "=========================================="
    echo ""
    echo "Run these commands in SQL Server Management Studio or sqlcmd:"
    echo ""
    echo "1. 📊 Generate comprehensive load (run this first):"
    echo "   sqlcmd -S your_server -d master -i sql_load_generation_scripts.sql"
    echo ""
    echo "2. 🔄 For continuous activity, run in separate sessions:"
    echo "   EXEC TestLoadDB.dbo.GenerateTransactionLoad;"
    echo "   EXEC TestLoadDB.dbo.GenerateSlowQueries;"
    echo "   EXEC TestLoadDB.dbo.GenerateBufferPoolActivity;"
    echo ""
    echo "3. ⚡ To generate lock contention:"
    echo "   EXEC TestLoadDB.dbo.GenerateLockContention;"
    echo ""
    echo "4. 💥 To generate deadlocks (run simultaneously in 2 sessions):"
    echo "   -- Session A: EXEC TestLoadDB.dbo.GenerateDeadlock_Session1;"
    echo "   -- Session B: EXEC TestLoadDB.dbo.GenerateDeadlock_Session2;"
}

# Function to show New Relic verification steps
show_verification_steps() {
    echo ""
    echo "🔍 NEW RELIC DATA VERIFICATION STEPS"
    echo "====================================="
    echo ""
    echo "1. 🌐 Go to New Relic One: https://one.newrelic.com"
    echo ""
    echo "2. 📊 Navigate to Query Builder and run these test queries:"
    echo ""
    echo "   Basic data check:"
    echo "   SELECT * FROM Metric WHERE metricName LIKE 'sqlserver.%' SINCE 1 hour ago LIMIT 10"
    echo ""
    echo "   Available metrics:"
    echo "   SELECT uniques(metricName) FROM Metric WHERE metricName LIKE 'sqlserver.%' SINCE 1 hour ago"
    echo ""
    echo "   Key performance metrics:"
    echo "   SELECT latest(sqlserver.bufferPoolHitPercent) AS 'Buffer Hit %', latest(sqlserver.stats.connections) AS 'Connections' FROM Metric SINCE 30 minutes ago"
    echo ""
    echo "3. 📋 Import the dashboard:"
    echo "   - Go to Dashboards → Import dashboard"
    echo "   - Upload: newrelic_fixed_sqlserver_dashboard.json"
    echo ""
    echo "4. ✅ Verify widgets show data (may take 2-3 minutes after load generation)"
}

# Function to monitor metrics in real-time
monitor_metrics() {
    echo ""
    echo "📡 MONITORING METRICS EXPORT..."
    echo "==============================="
    echo "Press Ctrl+C to stop monitoring"
    echo ""
    
    while true; do
        echo "$(date): Checking for metric exports..."
        
        # Check recent log entries for metric exports
        if tail -50 $LOG_FILE | grep -i "export" | tail -3; then
            echo "✅ Metrics are being exported"
        else
            echo "⏳ Waiting for metric exports..."
        fi
        
        echo "---"
        sleep 10
    done
}

# Function to cleanup
cleanup() {
    echo ""
    echo "🧹 Cleaning up..."
    
    if [ ! -z "$COLLECTOR_PID" ]; then
        kill $COLLECTOR_PID 2>/dev/null || true
        echo "🛑 Collector stopped"
    fi
    
    echo "✅ Cleanup complete"
}

# Trap cleanup on exit
trap cleanup EXIT

# Main execution
main() {
    echo "Starting SQL Server dashboard data generation process..."
    echo ""
    
    # Pre-flight checks
    check_collector
    check_config
    
    # Start collector
    start_collector
    
    # Check initial logs
    sleep 3
    check_collector_logs
    
    # Show instructions
    show_sql_commands
    show_verification_steps
    
    echo ""
    echo "🎯 NEXT STEPS:"
    echo "1. Run the SQL load generation scripts in your SQL Server"
    echo "2. Wait 2-3 minutes for data to flow"
    echo "3. Check New Relic Query Builder for incoming metrics"
    echo "4. Import and view the dashboard"
    echo ""
    
    # Ask user what they want to do
    echo "Choose an option:"
    echo "1. Monitor metric exports in real-time"
    echo "2. Check collector logs now"  
    echo "3. Exit and run collector in background"
    echo ""
    read -p "Enter choice (1-3): " choice
    
    case $choice in
        1)
            monitor_metrics
            ;;
        2)
            check_collector_logs
            ;;
        3)
            echo "🏃 Collector will continue running in background (PID: $COLLECTOR_PID)"
            echo "📋 Monitor logs with: tail -f $LOG_FILE"
            echo "🛑 Stop collector with: kill $COLLECTOR_PID"
            ;;
        *)
            echo "Invalid choice. Exiting..."
            ;;
    esac
}

# Run main function
main