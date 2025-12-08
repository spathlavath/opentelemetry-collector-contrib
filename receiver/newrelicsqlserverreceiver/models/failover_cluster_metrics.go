// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// and failover cluster performance metrics for high availability environments.
//
// Failover Cluster-Level Data Structures:
//
// 1. Failover Cluster Replica Metrics:
//
//	type FailoverClusterReplicaMetrics struct {
//	    LogBytesReceivedPerSec    *int64   // Log bytes received per second from primary replica
//	    TransactionDelayMs        *int64   // Transaction delay in milliseconds
//	    FlowControlTimeMs         *int64   // Flow control time in milliseconds per second
//	}
//
// Data Source Mappings:
// - sys.dm_os_performance_counters: Database replica performance counters for Always On AG
//
// Metric Calculations:
//
// Log Bytes Received/sec:
// - Query: sys.dm_os_performance_counters WHERE object_name LIKE '%Database Replica%' AND counter_name = 'Log Bytes Received/sec'
// - Purpose: Measures the rate of log records received by the secondary replica from the primary replica
// - Unit: Bytes per second
// - Scope: Per database replica in Always On Availability Group
//
// Transaction Delay:
// - Query: sys.dm_os_performance_counters WHERE object_name LIKE '%Database Replica%' AND counter_name = 'Transaction Delay'
// - Purpose: Average delay for transactions on the secondary replica
// - Unit: Milliseconds
// - Scope: Per database replica in Always On Availability Group
//
// Flow Control Time (ms/sec):
// - Query: sys.dm_os_performance_counters WHERE object_name LIKE '%Database Replica%' AND counter_name = 'Flow Control Time (ms/sec)'
// - Purpose: Time spent in flow control by log records from the primary replica
// - Unit: Milliseconds per second
// - Scope: Per database replica in Always On Availability Group
//
// Usage in Scrapers:
// - Only available on SQL Server instances configured with Always On Availability Groups
// - Provides failover cluster performance monitoring for high availability scenarios
// - Helps monitor replication lag and flow control in Always On environments
// - Enables performance optimization for Always On Availability Groups
package models

// FailoverClusterReplicaMetrics represents Always On Availability Group replica performance metrics
// This model captures the replica-level performance data as defined for Always On failover clusters
type FailoverClusterReplicaMetrics struct {
	// InstanceName represents the database instance name from performance counters
	// Query source: sys.dm_os_performance_counters.instance_name for Database Replica counters
	InstanceName string `db:"instance_name" source_type:"attribute"`

	// LogBytesReceivedPerSec represents the rate of log records received by secondary replica from primary
	// This metric corresponds to 'Log Bytes Received/sec' performance counter
	// Query source: sys.dm_os_performance_counters for Database Replica counters
	LogBytesReceivedPerSec *int64 `db:"Log Bytes Received/sec" metric_name:"sqlserver.failover_cluster.log_bytes_received_per_sec" source_type:"gauge"`

	// TransactionDelayMs represents the average delay for transactions on the secondary replica
	// This metric corresponds to 'Transaction Delay' performance counter
	// Query source: sys.dm_os_performance_counters for Database Replica counters
	TransactionDelayMs *int64 `db:"Transaction Delay" metric_name:"sqlserver.failover_cluster.transaction_delay_ms" source_type:"gauge"`

	// FlowControlTimeMs represents the time spent in flow control by log records from primary replica
	// This metric corresponds to 'Flow Control Time (ms/sec)' performance counter
	// Query source: sys.dm_os_performance_counters for Database Replica counters
	FlowControlTimeMs *int64 `db:"Flow Control Time (ms/sec)" metric_name:"sqlserver.failover_cluster.flow_control_time_ms" source_type:"gauge"`
}

// FailoverClusterAvailabilityGroupHealthMetrics represents Always On Availability Group health status
// This model captures the essential health and role information for availability group replicas
type FailoverClusterAvailabilityGroupHealthMetrics struct {
	// ReplicaServerName represents the name of the server instance hosting the availability replica
	// Query source: sys.availability_replicas.replica_server_name
	ReplicaServerName string `db:"replica_server_name" source_type:"attribute"`

	// RoleDesc describes the current role of the replica within the Availability Group
	// Query source: sys.dm_hadr_availability_replica_states.role_desc
	// Expected values: "PRIMARY", "SECONDARY"
	RoleDesc string `db:"role_desc" metric_name:"sqlserver.failover_cluster.ag_replica_role" source_type:"info"`

	// SynchronizationHealthDesc indicates the health of data synchronization between primary and secondary
	// Query source: sys.dm_hadr_availability_replica_states.synchronization_health_desc
	// Expected values: "HEALTHY", "PARTIALLY_HEALTHY", "NOT_HEALTHY"
	SynchronizationHealthDesc string `db:"synchronization_health_desc" metric_name:"sqlserver.failover_cluster.ag_synchronization_health" source_type:"info"`

	// Note: Removed unused fields that are not processed in the scraper:
	// - AvailabilityModeDesc, FailoverModeDesc, BackupPriority, EndpointURL, ReadOnlyRoutingURL
	// - ConnectedStateDesc, OperationalStateDesc, RecoveryHealthDesc
	// These were defined in the data model but never used in metric creation
}

// FailoverClusterAvailabilityGroupMetrics represents Always On Availability Group configuration metrics
// This model captures the essential availability group level configuration and settings
type FailoverClusterAvailabilityGroupMetrics struct {
	// GroupName represents the name of the availability group
	// Query source: sys.availability_groups.name
	GroupName string `db:"group_name" source_type:"attribute"`

	// FailureConditionLevel represents the failure detection level for the AG
	// Query source: sys.availability_groups.failure_condition_level
	// Range: 1-5, higher values indicate more sensitive failure detection
	FailureConditionLevel *int64 `db:"failure_condition_level" metric_name:"sqlserver.failover_cluster.ag_failure_condition_level" source_type:"gauge"`

	// ClusterTypeDesc represents the cluster type for the availability group
	// Query source: sys.availability_groups.cluster_type_desc
	// Expected values: "WSFC", "EXTERNAL", "NONE"
	ClusterTypeDesc string `db:"cluster_type_desc" metric_name:"sqlserver.failover_cluster.ag_cluster_type" source_type:"info"`

	// HealthCheckTimeout represents the health check timeout for the availability group
	// Query source: sys.availability_groups.health_check_timeout
	// Unit: Milliseconds
	HealthCheckTimeout *int64 `db:"health_check_timeout" metric_name:"sqlserver.failover_cluster.ag_health_check_timeout" source_type:"gauge"`

	// RequiredSynchronizedSecondariesToCommit represents the number of synchronous secondaries required to commit
	// Query source: sys.availability_groups.required_synchronized_secondaries_to_commit
	// Used for controlling commit behavior in Always On Availability Groups
	RequiredSynchronizedSecondariesToCommit *int64 `db:"required_synchronized_secondaries_to_commit" metric_name:"sqlserver.failover_cluster.ag_required_sync_secondaries" source_type:"gauge"`
}

// FailoverClusterRedoQueueMetrics represents Always On redo queue metrics
// This model captures log send queue, redo queue, and redo rate metrics for monitoring replication performance
// Compatible with both Standard SQL Server and Azure SQL Managed Instance
type FailoverClusterRedoQueueMetrics struct {
	// ReplicaServerName represents the name of the server hosting the replica
	// Query source: sys.availability_replicas.replica_server_name joined with sys.dm_hadr_database_replica_states
	ReplicaServerName string `db:"replica_server_name" source_type:"attribute"`

	// DatabaseName represents the name of the database in the availability group
	// Query source: sys.databases.name joined with sys.dm_hadr_database_replica_states
	DatabaseName string `db:"database_name" source_type:"attribute"`

	// LogSendQueueKB represents the amount of log records not yet sent to secondary replica (KB)
	// Query source: sys.dm_hadr_database_replica_states.log_send_queue_size
	LogSendQueueKB *int64 `db:"log_send_queue_kb" metric_name:"sqlserver.failover_cluster.log_send_queue_kb" source_type:"gauge"`

	// RedoQueueKB represents the amount of log records waiting to be redone on secondary replica (KB)
	// Query source: sys.dm_hadr_database_replica_states.redo_queue_size
	RedoQueueKB *int64 `db:"redo_queue_kb" metric_name:"sqlserver.failover_cluster.redo_queue_kb" source_type:"gauge"`

	// RedoRateKBSec represents the rate at which log records are being redone on secondary replica (KB/sec)
	// Query source: sys.dm_hadr_database_replica_states.redo_rate
	RedoRateKBSec *int64 `db:"redo_rate_kb_sec" metric_name:"sqlserver.failover_cluster.redo_rate_kb_sec" source_type:"gauge"`
}
