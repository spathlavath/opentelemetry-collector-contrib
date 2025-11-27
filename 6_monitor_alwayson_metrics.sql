-- =====================================================
-- MONITORING SCRIPT: AlwaysOn Metrics Monitor
-- Run this in a separate query window to monitor metrics
-- during workload execution
-- =====================================================

-- Monitor Log Bytes Received/sec, Transaction Delay, Flow Control Time
SELECT
    'Performance Counters' AS MetricCategory,
    instance_name AS DatabaseInstance,
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
    AND instance_name = 'AlwaysOnLoadTest'  -- Filter for our test database
GROUP BY
    instance_name

UNION ALL

-- Monitor Log Send Queue, Redo Queue, and Redo Rate
SELECT
    'Redo Queue Metrics' AS MetricCategory,
    ar.replica_server_name + '.' + d.name AS DatabaseInstance,
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
    d.name = 'AlwaysOnLoadTest'  -- Monitor our test database

UNION ALL

-- Monitor Overall AG Health
SELECT
    'AG Health' AS MetricCategory,
    ar.replica_server_name AS DatabaseInstance,
    CASE ars.role_desc WHEN 'PRIMARY' THEN 1 ELSE 0 END AS [Is_Primary],
    CASE ars.synchronization_health_desc 
        WHEN 'HEALTHY' THEN 0
        WHEN 'PARTIALLY_HEALTHY' THEN 1  
        WHEN 'NOT_HEALTHY' THEN 2
        ELSE 3
    END AS [Sync_Health_Code],
    CASE ars.connected_state_desc WHEN 'CONNECTED' THEN 1 ELSE 0 END AS [Is_Connected],
    GETDATE() AS [Sample_Time]
FROM
    sys.dm_hadr_availability_replica_states AS ars
INNER JOIN
    sys.availability_replicas AS ar ON ars.replica_id = ar.replica_id
ORDER BY MetricCategory, DatabaseInstance;

PRINT 'AlwaysOn metrics snapshot completed at: ' + CONVERT(VARCHAR(23), GETDATE(), 121);
PRINT '';
PRINT 'Key Metrics to Watch:';
PRINT '- Log_Bytes_Received_Per_Sec: Rate of log data flowing to secondary replicas';
PRINT '- Transaction_Delay_Ms: Delay in transaction processing on secondary';
PRINT '- Flow_Control_Time_Ms_Per_Sec: Flow control activation time';
PRINT '- Log_Send_Queue_KB: Backlog of log data to be sent';
PRINT '- Redo_Queue_KB: Backlog of log data to be applied on secondary';
PRINT '- Redo_Rate_KB_Sec: Rate of redo processing on secondary';
GO