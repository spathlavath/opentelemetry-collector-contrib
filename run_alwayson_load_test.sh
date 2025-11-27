#!/bin/bash

# =====================================================
# AlwaysOn AG Load Generation Script (Bash version)
# This script runs SQL workloads to generate AlwaysOn metrics
# =====================================================

# Configuration
SERVER_INSTANCE="${1:-localhost}"
DATABASE_NAME="${2:-AlwaysOnLoadTest}"
DURATION_MINUTES="${3:-30}"
USERNAME="${4:-}"
PASSWORD="${5:-}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
GRAY='\033[0;37m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${YELLOW}========================================${NC}"
    echo -e "${YELLOW}AlwaysOn AG Load Generation Script${NC}"
    echo -e "${YELLOW}========================================${NC}"
    echo -e "${GRAY}Server: $SERVER_INSTANCE${NC}"
    echo -e "${GRAY}Database: $DATABASE_NAME${NC}" 
    echo -e "${GRAY}Duration: $DURATION_MINUTES minutes${NC}"
    echo -e "${YELLOW}========================================${NC}"
}

check_sqlcmd() {
    if ! command -v sqlcmd &> /dev/null; then
        echo -e "${RED}ERROR: sqlcmd not found. Please install SQL Server command line tools.${NC}"
        echo -e "${GRAY}For Ubuntu/Debian: apt-get install mssql-tools${NC}"
        echo -e "${GRAY}For RHEL/CentOS: yum install mssql-tools${NC}"
        echo -e "${GRAY}For macOS: brew install microsoft/mssql-release/mssql-tools${NC}"
        exit 1
    fi
}

build_connection_string() {
    if [ -n "$USERNAME" ] && [ -n "$PASSWORD" ]; then
        SQLCMD_CONN="-S $SERVER_INSTANCE -d $DATABASE_NAME -U $USERNAME -P $PASSWORD -t 0"
    else
        SQLCMD_CONN="-S $SERVER_INSTANCE -d $DATABASE_NAME -E -t 0"
    fi
}

test_connection() {
    echo -e "${CYAN}Testing connection...${NC}"
    if echo "SELECT 1 AS TestConnection" | sqlcmd $SQLCMD_CONN > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Connection successful${NC}"
    else
        echo -e "${RED}✗ Connection failed${NC}"
        exit 1
    fi
}

run_workload() {
    local workload_name="$1"
    local workload_file="$2"
    
    echo -e "${GREEN}[$workload_name] Starting workload...${NC}"
    
    sqlcmd $SQLCMD_CONN -i "$workload_file" > "/tmp/workload_${workload_name}.log" 2>&1 &
    local pid=$!
    
    echo "$pid" > "/tmp/workload_${workload_name}.pid"
    echo -e "${GREEN}[$workload_name] Started with PID $pid${NC}"
}

create_workload_files() {
    # Workload 1: High Volume Transactions
    cat > /tmp/workload1.sql << 'EOF'
-- High Volume Transaction Load
DECLARE @Counter INT = 0;
DECLARE @BatchSize INT = 100;
DECLARE @EndTime DATETIME2 = DATEADD(MINUTE, $(DURATION_MINUTES), GETDATE());

WHILE GETDATE() < @EndTime
BEGIN
    INSERT INTO LargeTransactionTable (TransactionData, TransactionValue, LargeTextField)
    SELECT 
        'Transaction batch ' + CAST(@Counter AS VARCHAR(10)) + ' - ' + 
        REPLICATE('LoadTestData', 50),
        RAND() * 10000,
        REPLICATE('Large text field data for testing log volume generation. ', 100)
    FROM sys.objects s1 CROSS JOIN sys.objects s2
    WHERE ROWCOUNT_BIG() < @BatchSize;
    
    BEGIN TRANSACTION;
        UPDATE LargeTransactionTable 
        SET ProcessingFlag = 1,
            TransactionValue = TransactionValue * 1.1
        WHERE Id % 10 = 0 AND ProcessingFlag = 0;
        
        WAITFOR DELAY '00:00:01';
    COMMIT TRANSACTION;
    
    SET @Counter = @Counter + 1;
    WAITFOR DELAY '00:00:02';
END;
EOF

    # Workload 2: Frequent Updates
    cat > /tmp/workload2.sql << 'EOF'
-- Frequent Updates for Redo Queue Pressure
DECLARE @IterationCount INT = 0;
DECLARE @EndTime DATETIME2 = DATEADD(MINUTE, $(DURATION_MINUTES), GETDATE());

WHILE GETDATE() < @EndTime
BEGIN
    UPDATE FrequentUpdateTable 
    SET Counter = Counter + 1,
        LastUpdated = GETDATE(),
        UpdateCount = UpdateCount + 1,
        RandomValue = NEWID()
    WHERE Id <= 100;
    
    BEGIN TRANSACTION;
        UPDATE FrequentUpdateTable 
        SET Counter = Counter + 10
        WHERE Id BETWEEN 101 AND 200;
        
        INSERT INTO BulkInsertTable (BulkData, SequenceNumber, ChecksumValue)
        VALUES 
        ('Bulk data iteration ' + CAST(@IterationCount AS VARCHAR(10)), @IterationCount, CHECKSUM(@IterationCount));
        
        WAITFOR DELAY '00:00:00.500';
    COMMIT TRANSACTION;
    
    SET @IterationCount = @IterationCount + 1;
    WAITFOR DELAY '00:00:00.100';
END;
EOF

    # Workload 3: Bulk Operations
    cat > /tmp/workload3.sql << 'EOF'
-- Bulk Operations for Log Send Queue Stress
DECLARE @BulkCounter INT = 0;
DECLARE @EndTime DATETIME2 = DATEADD(MINUTE, $(DURATION_MINUTES), GETDATE());

WHILE GETDATE() < @EndTime
BEGIN
    DECLARE @StartSequence BIGINT = @BulkCounter * 1000;
    
    INSERT INTO BulkInsertTable (BulkData, SequenceNumber, ChecksumValue)
    SELECT 
        'Bulk operation ' + CAST(@BulkCounter AS VARCHAR(10)) + ' - Row ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR(10)) + 
        ' - ' + REPLICATE('DataLoad', 25),
        @StartSequence + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
        CHECKSUM(@StartSequence + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))
    FROM sys.objects s1 CROSS JOIN sys.objects s2
    WHERE ROWCOUNT_BIG() < 500;
    
    BEGIN TRANSACTION;
        UPDATE BulkInsertTable 
        SET BulkData = BulkData + ' - Updated in batch ' + CAST(@BulkCounter AS VARCHAR(10))
        WHERE SequenceNumber BETWEEN @StartSequence - 500 AND @StartSequence;
        
        DELETE FROM BulkInsertTable 
        WHERE SequenceNumber < (@StartSequence - 2000) 
        AND SequenceNumber % 3 = 0;
        
        WAITFOR DELAY '00:00:01';
    COMMIT TRANSACTION;
    
    SET @BulkCounter = @BulkCounter + 1;
    WAITFOR DELAY '00:00:03';
END;
EOF

    # Workload 4: Mixed Workload
    cat > /tmp/workload4.sql << 'EOF'
-- Mixed Workload for Flow Control
DECLARE @MixedCounter INT = 0;
DECLARE @EndTime DATETIME2 = DATEADD(MINUTE, $(DURATION_MINUTES), GETDATE());

WHILE GETDATE() < @EndTime
BEGIN
    SELECT COUNT(*), AVG(TransactionValue), MAX(LEN(LargeTextField))
    FROM LargeTransactionTable 
    WHERE TransactionDate >= DATEADD(MINUTE, -10, GETDATE());
    
    BEGIN TRANSACTION;
        INSERT INTO LargeTransactionTable (TransactionData, TransactionValue, LargeTextField)
        VALUES 
        ('Mixed workload entry ' + CAST(@MixedCounter AS VARCHAR(10)), 
         RAND() * 1000,
         REPLICATE('Mixed workload data for flow control testing. ', 80));
         
        UPDATE FrequentUpdateTable 
        SET Counter = Counter + (@MixedCounter % 100),
            UpdateCount = UpdateCount + 1
        WHERE Id = ((@MixedCounter % 1000) + 1);
        
        WAITFOR DELAY '00:00:00.200';
    COMMIT TRANSACTION;
    
    SET @MixedCounter = @MixedCounter + 1;
    WAITFOR DELAY '00:00:00.500';
END;
EOF

    # Replace placeholder with actual duration
    sed -i "s/\$(DURATION_MINUTES)/$DURATION_MINUTES/g" /tmp/workload*.sql
}

create_monitoring_script() {
    cat > /tmp/monitor_metrics.sql << 'EOF'
-- Monitor AlwaysOn Metrics
SELECT
    'Performance Counters' AS MetricType,
    instance_name AS InstanceName,
    ISNULL(MAX(CASE WHEN counter_name = 'Log Bytes Received/sec' THEN cntr_value END), 0) AS [Log_Bytes_Received_Per_Sec],
    ISNULL(MAX(CASE WHEN counter_name = 'Transaction Delay' THEN cntr_value END), 0) AS [Transaction_Delay_Ms],
    ISNULL(MAX(CASE WHEN counter_name = 'Flow Control Time (ms/sec)' THEN cntr_value END), 0) AS [Flow_Control_Time_Ms_Per_Sec],
    GETDATE() AS [Sample_Time]
FROM
    sys.dm_os_performance_counters
WHERE
    object_name LIKE '%Database Replica%'
    AND counter_name IN (
        'Log Bytes Received/sec',
        'Transaction Delay',
        'Flow Control Time (ms/sec)'
    )
GROUP BY
    instance_name

UNION ALL

-- Monitor redo queue metrics
SELECT
    'Redo Queue Metrics' AS MetricType,
    ar.replica_server_name + '.' + d.name AS InstanceName,
    drs.log_send_queue_size AS [Log_Send_Queue_KB],
    drs.redo_queue_size AS [Redo_Queue_KB],
    drs.redo_rate AS [Redo_Rate_KB_Sec],
    GETDATE() AS [Sample_Time]
FROM
    sys.dm_hadr_database_replica_states AS drs
JOIN
    sys.availability_replicas AS ar ON drs.replica_id = ar.replica_id
JOIN
    sys.databases AS d ON drs.database_id = d.database_id
WHERE
    d.name = 'DATABASE_NAME_PLACEHOLDER';
EOF

    sed -i "s/DATABASE_NAME_PLACEHOLDER/$DATABASE_NAME/g" /tmp/monitor_metrics.sql
}

start_monitoring() {
    echo -e "${CYAN}[MONITOR] Starting metrics monitoring...${NC}"
    
    local monitor_conn
    if [ -n "$USERNAME" ] && [ -n "$PASSWORD" ]; then
        monitor_conn="-S $SERVER_INSTANCE -d master -U $USERNAME -P $PASSWORD"
    else
        monitor_conn="-S $SERVER_INSTANCE -d master -E"
    fi
    
    (
        local end_time=$(($(date +%s) + ($DURATION_MINUTES * 60)))
        local iteration=1
        
        while [ $(date +%s) -lt $end_time ]; do
            echo -e "${CYAN}[MONITOR] Iteration $iteration - $(date)${NC}"
            sqlcmd $monitor_conn -i /tmp/monitor_metrics.sql
            echo ""
            iteration=$((iteration + 1))
            sleep 30
        done
        
        echo -e "${CYAN}[MONITOR] Monitoring completed.${NC}"
    ) &
    
    echo "$!" > /tmp/monitor.pid
}

cleanup() {
    echo -e "\n${YELLOW}Stopping all jobs...${NC}"
    
    # Kill monitoring
    if [ -f /tmp/monitor.pid ]; then
        kill $(cat /tmp/monitor.pid) 2>/dev/null || true
        rm -f /tmp/monitor.pid
    fi
    
    # Kill workloads
    for i in {1..4}; do
        if [ -f "/tmp/workload_WORKLOAD$i.pid" ]; then
            kill $(cat "/tmp/workload_WORKLOAD$i.pid") 2>/dev/null || true
            rm -f "/tmp/workload_WORKLOAD$i.pid"
        fi
    done
    
    # Clean up temp files
    rm -f /tmp/workload*.sql /tmp/monitor_metrics.sql /tmp/workload*.log
    
    echo -e "${GREEN}Cleanup completed.${NC}"
}

show_usage() {
    echo "Usage: $0 [server_instance] [database_name] [duration_minutes] [username] [password]"
    echo ""
    echo "Parameters:"
    echo "  server_instance    SQL Server instance (default: localhost)"
    echo "  database_name      Target database name (default: AlwaysOnLoadTest)"  
    echo "  duration_minutes   Load test duration in minutes (default: 30)"
    echo "  username          SQL Server username (optional - uses Windows auth if not provided)"
    echo "  password          SQL Server password (optional - uses Windows auth if not provided)"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Use defaults with Windows auth"
    echo "  $0 myserver AlwaysOnDB 60            # Custom server, database, and duration"
    echo "  $0 myserver AlwaysOnDB 30 sa mypass  # With SQL Server authentication"
    echo ""
}

# Main execution
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    show_usage
    exit 0
fi

# Trap for cleanup
trap cleanup EXIT INT TERM

print_header
check_sqlcmd
build_connection_string
test_connection

echo -e "${YELLOW}\nPreparing workloads...${NC}"
create_workload_files
create_monitoring_script

echo -e "${YELLOW}Starting workloads...${NC}"
run_workload "WORKLOAD1" "/tmp/workload1.sql"
run_workload "WORKLOAD2" "/tmp/workload2.sql"
run_workload "WORKLOAD3" "/tmp/workload3.sql"
run_workload "WORKLOAD4" "/tmp/workload4.sql"

start_monitoring

echo -e "${GREEN}✓ Started 4 workload jobs and monitoring${NC}"
echo -e "${GREEN}✓ Load generation will run for $DURATION_MINUTES minutes${NC}"
echo -e "${CYAN}\nPress Ctrl+C to stop early...${NC}"

# Wait for completion
sleep $(($DURATION_MINUTES * 60 + 120))  # Extra 2 minutes buffer

echo -e "\n${YELLOW}========================================${NC}"
echo -e "${GREEN}Load generation completed!${NC}"
echo -e "${YELLOW}========================================${NC}"
echo -e "${GRAY}Check your OpenTelemetry collector output for metrics:${NC}"
echo -e "${GRAY}- sqlserver.failover_cluster.log_bytes_received_per_sec${NC}"
echo -e "${GRAY}- sqlserver.failover_cluster.transaction_delay_ms${NC}"
echo -e "${GRAY}- sqlserver.failover_cluster.flow_control_time_ms${NC}"
echo -e "${GRAY}- sqlserver.failover_cluster.log_send_queue_kb${NC}"
echo -e "${GRAY}- sqlserver.failover_cluster.redo_queue_kb${NC}"
echo -e "${GRAY}- sqlserver.failover_cluster.redo_rate_kb_sec${NC}"