package models

import "database/sql"

// SlowQuery represents a slow query record from Oracle V$SQL view
type SlowQuery struct {
	CollectionTimestamp sql.NullString // Timestamp when query was collected from Oracle
	DatabaseName        sql.NullString
	QueryID             sql.NullString
	SchemaName          sql.NullString
	UserName            sql.NullString // NEW: The user who parsed the statement
	ExecutionCount      sql.NullInt64
	QueryText           sql.NullString  // Full query text from sql_fulltext (used for metadata extraction, normalization, and hash generation)
	LastActiveTime      sql.NullString
	TotalElapsedTimeMS  sql.NullFloat64 // Total elapsed time - used for precise delta calculation
	TotalCPUTimeMS      sql.NullFloat64 // Total CPU time - used for delta calculation
	TotalDiskReads      sql.NullInt64   // Total disk reads - used for delta calculation
	TotalBufferGets     sql.NullInt64   // Total buffer gets (rows examined) - used for delta calculation
	TotalRowsProcessed  sql.NullInt64   // Total rows processed (rows returned) - used for delta calculation
	TotalDiskWrites     sql.NullInt64   // Total disk writes - used for delta calculation
	TotalWaitTimeMS     sql.NullFloat64 // Total wait time (user I/O wait) - used for delta calculation

	// Interval-based delta metrics (calculated in-memory, not from DB)
	// These are populated by the OracleIntervalCalculator

	// Interval averages (delta divided by execution count)
	IntervalAvgElapsedTimeMS *float64 // Average elapsed time in the last interval (milliseconds)
	IntervalAvgCPUTimeMS     *float64 // Average CPU time in the last interval (milliseconds)
	IntervalAvgWaitTimeMS    *float64 // Average wait time in the last interval (milliseconds)
	IntervalAvgDiskReads     *float64 // Average disk reads in the last interval
	IntervalAvgDiskWrites    *float64 // Average direct writes in the last interval (bypass buffer cache)
	IntervalAvgBufferGets    *float64 // Average buffer gets in the last interval
	IntervalAvgRowsProcessed *float64 // Average rows processed in the last interval
	IntervalExecutionCount   *int64   // Number of executions in the last interval

	// Interval totals (delta values without averaging)
	IntervalElapsedTimeMS *float64 // Total elapsed time in the last interval (milliseconds, not averaged)
	IntervalCPUTimeMS     *float64 // Total CPU time in the last interval (milliseconds, not averaged)
	IntervalWaitTimeMS    *float64 // Total wait time in the last interval (milliseconds, not averaged)
	IntervalDiskReads     *float64 // Total disk reads in the last interval (not averaged)
	IntervalDiskWrites    *float64 // Total direct writes in the last interval (not averaged)
	IntervalBufferGets    *float64 // Total buffer gets in the last interval (not averaged)
	IntervalRowsProcessed *float64 // Total rows processed in the last interval (not averaged)
}

// GetCollectionTimestamp returns the collection timestamp as a string, empty if null
func (sq *SlowQuery) GetCollectionTimestamp() string {
	if sq.CollectionTimestamp.Valid {
		return sq.CollectionTimestamp.String
	}
	return ""
}

// GetDatabaseName returns the database name as a string, empty if null
func (sq *SlowQuery) GetDatabaseName() string {
	if sq.DatabaseName.Valid {
		return sq.DatabaseName.String
	}
	return ""
}

// GetQueryID returns the query ID as a string, empty if null
func (sq *SlowQuery) GetQueryID() string {
	if sq.QueryID.Valid {
		return sq.QueryID.String
	}
	return ""
}

// GetSchemaName returns the schema name as a string, empty if null
func (sq *SlowQuery) GetSchemaName() string {
	if sq.SchemaName.Valid {
		return sq.SchemaName.String
	}
	return ""
}

// GetQueryText returns the query text as a string, empty if null
func (sq *SlowQuery) GetQueryText() string {
	if sq.QueryText.Valid {
		return sq.QueryText.String
	}
	return ""
}

// GetUserName returns the username as a string, empty if null
func (sq *SlowQuery) GetUserName() string {
	if sq.UserName.Valid {
		return sq.UserName.String
	}
	return ""
}

// GetLastActiveTime returns the last active time as a string, empty if null
func (sq *SlowQuery) GetLastActiveTime() string {
	if sq.LastActiveTime.Valid {
		return sq.LastActiveTime.String
	}
	return ""
}


// HasValidQueryID checks if the query has a valid query ID
func (sq *SlowQuery) HasValidQueryID() bool {
	return sq.QueryID.Valid
}

// HasValidElapsedTime checks if the query has a valid elapsed time
func (sq *SlowQuery) HasValidElapsedTime() bool {
	return sq.TotalElapsedTimeMS.Valid
}

// IsValidForMetrics checks if the slow query has the minimum required fields for metrics
func (sq *SlowQuery) IsValidForMetrics() bool {
	return sq.HasValidQueryID() && sq.HasValidElapsedTime()
}
