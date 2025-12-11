// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
package queries

const FailoverClusterReplicaQuery = `SELECT
    instance_name,
    ISNULL(MAX(CASE WHEN counter_name = 'Log Bytes Received/sec' THEN cntr_value END), 0) AS [Log Bytes Received/sec],
    ISNULL(MAX(CASE WHEN counter_name = 'Transaction Delay' THEN cntr_value END), 0) AS [Transaction Delay],
    ISNULL(MAX(CASE WHEN counter_name = 'Flow Control Time (ms/sec)' THEN cntr_value END), 0) AS [Flow Control Time (ms/sec)]
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
    instance_name;`

const FailoverClusterAvailabilityGroupHealthQuery = `SELECT
    ISNULL(ar.replica_server_name, 'UNKNOWN') AS replica_server_name,
    ISNULL(ars.role_desc, 'UNKNOWN') AS role_desc,
    ISNULL(ars.synchronization_health_desc, 'UNKNOWN') AS synchronization_health_desc
FROM
    sys.dm_hadr_availability_replica_states AS ars
INNER JOIN
    sys.availability_replicas AS ar ON ars.replica_id = ar.replica_id;`

const FailoverClusterAvailabilityGroupQuery = `SELECT
    ISNULL(ag.name, 'UNKNOWN') AS group_name,
    ISNULL(ag.failure_condition_level, 0) AS failure_condition_level,
    ISNULL(ag.cluster_type_desc, 'UNKNOWN') AS cluster_type_desc,
    ISNULL(ag.health_check_timeout, 0) AS health_check_timeout,
    ISNULL(ag.required_synchronized_secondaries_to_commit, 0) AS required_synchronized_secondaries_to_commit
FROM
    sys.availability_groups AS ag;`

const FailoverClusterRedoQueueQuery = `SELECT
    ar.replica_server_name,
    d.name AS database_name,
    ISNULL(drs.log_send_queue_size, 0) AS log_send_queue_kb,
    ISNULL(drs.redo_queue_size, 0) AS redo_queue_kb,
    ISNULL(drs.redo_rate, 0) AS redo_rate_kb_sec
FROM
    sys.dm_hadr_database_replica_states AS drs
JOIN
    sys.availability_replicas AS ar ON drs.replica_id = ar.replica_id
JOIN
    sys.databases AS d ON drs.database_id = d.database_id;`
