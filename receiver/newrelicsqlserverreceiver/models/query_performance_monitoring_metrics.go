// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

// SlowQuery represents slow query performance data collected from SQL Server
// This struct provides compatibility with the existing data structure format
type SlowQuery struct {
	QueryID                *QueryID `db:"query_id" metric_name:"query_id" source_type:"attribute"`
	PlanHandle             *QueryID `db:"plan_handle" metric_name:"plan_handle" source_type:"attribute"`
	QueryText              *string  `db:"query_text" metric_name:"query_text" source_type:"attribute"`
	DatabaseName           *string  `db:"database_name" metric_name:"database_name" source_type:"attribute"`
	CreationTime           *string  `db:"creation_time" metric_name:"creation_time" source_type:"attribute"`
	LastExecutionTimestamp *string  `db:"last_execution_timestamp" metric_name:"last_execution_timestamp" source_type:"attribute"`
	ExecutionCount         *int64   `db:"execution_count" metric_name:"execution_count" source_type:"gauge"`
	AvgElapsedTimeMS       *float64 `db:"avg_elapsed_time_ms" metric_name:"sqlserver.slowquery.avg_elapsed_time_ms" source_type:"gauge"`
	TotalElapsedTimeMS     *float64 `db:"total_elapsed_time_ms"` // Used for precise delta calculation only
	CollectionTimestamp    *string  `db:"collection_timestamp" metric_name:"collection_timestamp" source_type:"attribute"`

	// Interval-based delta metrics (calculated in-memory, not from DB)
	// These are populated by the SimplifiedIntervalCalculator
	IntervalAvgElapsedTimeMS *float64 `db:"-" metric_name:"sqlserver.slowquery.interval_avg_elapsed_time_ms" source_type:"gauge"`
	IntervalExecutionCount   *int64   `db:"-" metric_name:"sqlserver.slowquery.interval_execution_count" source_type:"gauge"`

	// New Relic Metadata Extraction (calculated in-memory from query comments, not from DB)
	// These fields enable cross-language query correlation and APM integration
	ClientName        *string `db:"-" metric_name:"client_name" source_type:"attribute"`         // Extracted from nr_service comment
	TransactionName   *string `db:"-" metric_name:"transaction_name" source_type:"attribute"`    // Extracted from nr_txn comment
	NormalisedSqlHash *string `db:"-" metric_name:"normalised_sql_hash" source_type:"attribute"` // MD5 hash of normalized SQL for cross-language correlation
}

// WaitTimeAnalysis represents wait time analysis data for SQL Server queries
type WaitTimeAnalysis struct {
	QueryID             *QueryID `db:"query_id" metric_name:"query_id" source_type:"attribute"`
	DatabaseName        *string  `db:"database_name" metric_name:"database_name" source_type:"attribute"`
	QueryText           *string  `db:"query_text" metric_name:"query_text" source_type:"attribute"`
	WaitCategory        *string  `db:"wait_category" metric_name:"wait_category" source_type:"attribute"`
	TotalWaitTimeMs     *float64 `db:"total_wait_time_ms" metric_name:"total_wait_time_ms" source_type:"gauge"`
	AvgWaitTimeMs       *float64 `db:"avg_wait_time_ms" metric_name:"avg_wait_time_ms" source_type:"gauge"`
	WaitEventCount      *int64   `db:"wait_event_count" metric_name:"wait_event_count" source_type:"gauge"`
	LastExecutionTime   *string  `db:"last_execution_time" metric_name:"last_execution_time" source_type:"attribute"`
	CollectionTimestamp *string  `db:"collection_timestamp" metric_name:"collection_timestamp" source_type:"attribute"`
}

// QueryExecutionPlan represents detailed execution plan analysis data for SQL Server queries
// This model is used for drill-down analysis from slow query detection to specific execution plans
type QueryExecutionPlan struct {
	QueryID           *QueryID `db:"query_id" metric_name:"query_id" source_type:"attribute"`
	PlanHandle        *QueryID `db:"plan_handle" metric_name:"plan_handle" source_type:"attribute"`
	QueryPlanID       *QueryID `db:"query_plan_id" metric_name:"query_plan_id" source_type:"attribute"`
	SQLText           *string  `db:"sql_text" metric_name:"sql_text" source_type:"attribute"`
	TotalCPUMs        *float64 `db:"total_cpu_ms" metric_name:"total_cpu_ms" source_type:"gauge"`
	TotalElapsedMs    *float64 `db:"total_elapsed_ms" metric_name:"total_elapsed_ms" source_type:"gauge"`
	CreationTime      *string  `db:"creation_time" metric_name:"creation_time" source_type:"attribute"`
	LastExecutionTime *string  `db:"last_execution_time" metric_name:"last_execution_time" source_type:"attribute"`
	ExecutionPlanXML  *string  `db:"execution_plan_xml" metric_name:"execution_plan_xml" source_type:"attribute"`
}

// ExecutionPlanNode represents a parsed execution plan node with detailed operator information
// This model contains the parsed data structure from XML execution plans for New Relic logging
type ExecutionPlanNode struct {
	// Identifiers
	QueryID      string `json:"query_id"`
	PlanHandle   string `json:"plan_handle"`
	NodeID       int    `json:"node_id"`
	ParentNodeID int    `json:"parent_node_id"`

	// SQL Query Information
	SQLText string `json:"sql_text"`

	// Operator Information
	PhysicalOp string `json:"physical_op"`
	LogicalOp  string `json:"logical_op"`
	InputType  string `json:"input_type"`

	// Object Information (for Index Scan/Seek operators)
	SchemaName        string `json:"schema_name"`
	TableName         string `json:"table_name"`
	IndexName         string `json:"index_name"`
	ReferencedColumns string `json:"referenced_columns"`

	// Cost Estimates
	EstimateRows          float64 `json:"estimate_rows"`
	EstimateIO            float64 `json:"estimate_io"`
	EstimateCPU           float64 `json:"estimate_cpu"`
	AvgRowSize            float64 `json:"avg_row_size"`
	TotalSubtreeCost      float64 `json:"total_subtree_cost"`
	EstimatedOperatorCost float64 `json:"estimated_operator_cost"`

	// Execution Details
	EstimatedExecutionMode string `json:"estimated_execution_mode"`
	GrantedMemoryKb        int64  `json:"granted_memory_kb"`
	SpillOccurred          bool   `json:"spill_occurred"`
	NoJoinPredicate        bool   `json:"no_join_predicate"`

	// Performance Metrics
	TotalWorkerTime    float64 `json:"total_worker_time"`
	TotalElapsedTime   float64 `json:"total_elapsed_time"`
	TotalLogicalReads  int64   `json:"total_logical_reads"`
	TotalLogicalWrites int64   `json:"total_logical_writes"`
	ExecutionCount     int64   `json:"execution_count"`
	AvgElapsedTimeMs   float64 `json:"avg_elapsed_time_ms"`

	// Timestamps
	CollectionTimestamp string `json:"collection_timestamp"`
	LastExecutionTime   string `json:"last_execution_time"`
}

// ExecutionPlanAnalysis represents the complete parsed execution plan with metadata
type ExecutionPlanAnalysis struct {
	QueryID        string              `json:"query_id"`
	PlanHandle     string              `json:"plan_handle"`
	SQLText        string              `json:"sql_text"`
	TotalCost      float64             `json:"total_cost"`
	CompileTime    string              `json:"compile_time"`
	CompileCPU     int64               `json:"compile_cpu"`
	CompileMemory  int64               `json:"compile_memory"`
	Nodes          []ExecutionPlanNode `json:"nodes"`
	CollectionTime string              `json:"collection_time"`
}

// ActiveRunningQuery represents currently executing queries with wait and blocking details
// This model captures real-time execution state from sys.dm_exec_requests
//
// RCA Enhancement: Includes query_hash (query_id) for correlation with slow queries, plus a
// correlation_query_id fallback that uses text hash when query_hash is NULL (non-cached queries)
type ActiveRunningQuery struct {
	// A. SESSION IDENTIFICATION (Required for correlation)
	CurrentSessionID *int64 `db:"current_session_id" metric_name:"sqlserver.activequery.session_id" source_type:"gauge"`
	RequestID        *int64 `db:"request_id" metric_name:"request_id" source_type:"attribute"`

	// B. SESSION CONTEXT (Required by NRQL Query 2)
	DatabaseName *string `db:"database_name" metric_name:"database_name" source_type:"attribute"`
	LoginName    *string `db:"login_name" metric_name:"login_name" source_type:"attribute"`
	HostName     *string `db:"host_name" metric_name:"host_name" source_type:"attribute"`

	// C. QUERY CORRELATION (Required for slow query correlation)
	QueryID            *QueryID `db:"query_id" metric_name:"query_id" source_type:"attribute"`
	QueryStatementText *string  `db:"query_statement_text" metric_name:"query_statement_text" source_type:"attribute"`

	// D. WAIT DETAILS (Required by NRQL Query 1)
	WaitType               *string  `db:"wait_type" metric_name:"wait_type" source_type:"attribute"`
	WaitTimeS              *float64 `db:"wait_time_s" metric_name:"sqlserver.activequery.wait_time_seconds" source_type:"gauge"`
	WaitResource           *string  `db:"wait_resource" metric_name:"wait_resource" source_type:"attribute"`
	LastWaitType           *string  `db:"last_wait_type" metric_name:"last_wait_type" source_type:"attribute"`
	WaitResourceObjectName *string  `db:"wait_resource_object_name" metric_name:"wait_resource_object_name" source_type:"attribute"`

	// E. TIMESTAMPS (Required by NRQL queries)
	RequestStartTime    *string `db:"request_start_time" metric_name:"request_start_time" source_type:"attribute"`
	CollectionTimestamp *string `db:"collection_timestamp" metric_name:"collection_timestamp" source_type:"attribute"`

	// F. TRANSACTION CONTEXT (Required by NRQL Query 1)
	TransactionID        *int64 `db:"transaction_id" metric_name:"transaction_id" source_type:"attribute"`
	OpenTransactionCount *int64 `db:"open_transaction_count" metric_name:"open_transaction_count" source_type:"gauge"`

	// G. PLAN HANDLE (Required for execution plan retrieval)
	PlanHandle *QueryID `db:"plan_handle" metric_name:"plan_handle" source_type:"attribute"`

	// H. BLOCKING DETAILS (Required by NRQL Query 1)
	BlockingSessionID          *int64   `db:"blocking_session_id" metric_name:"blocking_session_id" source_type:"attribute"`
	BlockerLoginName           *string  `db:"blocker_login_name" metric_name:"blocker_login_name" source_type:"attribute"`
	BlockingQueryStatementText *string  `db:"blocking_query_statement_text" metric_name:"blocking_query_statement_text" source_type:"attribute"`
	BlockingQueryHash          *QueryID `db:"blocking_query_hash" metric_name:"blocking_query_hash" source_type:"attribute"`

	// K. APM Integration (calculated in-memory from query comments, not from DB)
	// These fields enable cross-language query correlation for active queries
	ClientName        *string `db:"-" metric_name:"client_name" source_type:"attribute"`         // Extracted from nr_service comment
	TransactionName   *string `db:"-" metric_name:"transaction_name" source_type:"attribute"`    // Extracted from nr_txn comment
	NormalisedSqlHash *string `db:"-" metric_name:"normalised_sql_hash" source_type:"attribute"` // MD5 hash of normalized SQL for cross-language correlation
}

// LockedObject represents detailed information about database objects locked by a session
// This model resolves lock resources to actual table/object names for troubleshooting lock contention
type LockedObject struct {
	// Session and Database Context
	SessionID    *int64  `db:"session_id" metric_name:"session_id" source_type:"attribute"`
	DatabaseName *string `db:"database_name" metric_name:"database_name" source_type:"attribute"`

	// Object Identification
	SchemaName       *string `db:"schema_name" metric_name:"schema_name" source_type:"attribute"`
	LockedObjectName *string `db:"locked_object_name" metric_name:"locked_object_name" source_type:"attribute"`

	// Lock Details
	ResourceType        *string `db:"resource_type" metric_name:"resource_type" source_type:"attribute"`
	LockGranularity     *string `db:"lock_granularity" metric_name:"lock_granularity" source_type:"attribute"`
	LockMode            *string `db:"lock_mode" metric_name:"lock_mode" source_type:"attribute"`
	LockStatus          *string `db:"lock_status" metric_name:"lock_status" source_type:"attribute"`
	LockRequestType     *string `db:"lock_request_type" metric_name:"lock_request_type" source_type:"attribute"`
	ResourceDescription *string `db:"resource_description" metric_name:"resource_description" source_type:"attribute"`

	// Metadata
	CollectionTimestamp *string `db:"collection_timestamp" metric_name:"collection_timestamp" source_type:"attribute"`
}

// SlowQueryPlanData represents lightweight plan data extracted from slow queries
// Used for in-memory correlation with active queries (NO database query needed)
// Contains ONLY the fields needed for sqlserver.plan.* metrics
type SlowQueryPlanData struct {
	QueryHash         *QueryID // query_hash - for correlation
	PlanHandle        *QueryID // plan_handle - for fetching XML
	CreationTime      *string  // When plan was created
	LastExecutionTime *string  // Last execution timestamp
	AvgElapsedTimeMs  *float64 // Average elapsed time per execution
}

// PlanHandleResult represents lightweight plan data for emitting plan metrics
// Contains only the fields needed for sqlserver.plan.* metrics
type PlanHandleResult struct {
	QueryID           *QueryID // query_hash - for correlation
	PlanHandle        *QueryID // plan_handle identifier
	LastExecutionTime *string  // Last execution timestamp
	CreationTime      *string  // When plan was created
	AvgElapsedTimeMs  *float64 // Average elapsed time per execution
}

// ExecutionPlanTopLevelDetails represents high-level execution plan details (not node-level)
// Extracted from XML execution plan for lightweight analysis
type ExecutionPlanTopLevelDetails struct {
	// Identifiers
	QueryID       string `json:"query_id"`
	PlanHandle    string `json:"plan_handle"`
	QueryPlanHash string `json:"query_plan_hash"`

	// SQL Query Information
	SQLText string `json:"sql_text"`

	// Top-Level Plan Costs (from root RelOp)
	TotalSubtreeCost     float64 `json:"total_subtree_cost"`
	StatementSubTreeCost float64 `json:"statement_subtree_cost"`
	EstimatedTotalCost   float64 `json:"estimated_total_cost"`

	// Compilation Details
	CompileTime    string `json:"compile_time"`
	CompileCPU     int64  `json:"compile_cpu"`
	CompileMemory  int64  `json:"compile_memory"`
	CachedPlanSize int64  `json:"cached_plan_size"`

	// Optimizer Details
	StatementOptmLevel            string `json:"statement_optm_level"`
	StatementOptmEarlyAbortReason string `json:"statement_optm_early_abort_reason"`

	// Execution Counts and Timing
	ExecutionCount    int64   `json:"execution_count"`
	AvgElapsedTimeMs  float64 `json:"avg_elapsed_time_ms"`
	AvgWorkerTimeMs   float64 `json:"avg_worker_time_ms"`
	LastExecutionTime string  `json:"last_execution_time"`
	CreationTime      string  `json:"creation_time"`

	// Resource Estimates (from StmtSimple)
	EstimateRows float64 `json:"estimate_rows"`
	EstimateIO   float64 `json:"estimate_io"`
	EstimateCPU  float64 `json:"estimate_cpu"`
	AvgRowSize   int64   `json:"avg_row_size"`

	// Plan Details
	DegreeOfParallelism int64 `json:"degree_of_parallelism"`
	MemoryGrant         int64 `json:"memory_grant"`
	CachedPlanSize64    int64 `json:"cached_plan_size_64"`

	// Missing Index Information (if any)
	MissingIndexCount  int     `json:"missing_index_count"`
	MissingIndexImpact float64 `json:"missing_index_impact"`

	// Warnings
	NoJoinPredicate         bool `json:"no_join_predicate"`
	ColumnsWithNoStatistics int  `json:"columns_with_no_statistics"`
	UnmatchedIndexes        int  `json:"unmatched_indexes"`

	// Operator Summary
	TotalOperators int `json:"total_operators"`
	ScansCount     int `json:"scans_count"`
	SeeksCount     int `json:"seeks_count"`

	// Timestamps
	CollectionTimestamp string `json:"collection_timestamp"`
}
