package models

import "database/sql"

// WaitEvent represents a wait event record from Oracle V$ACTIVE_SESSION_HISTORY view
type WaitEvent struct {
    DatabaseName      sql.NullString
    QueryID           sql.NullString
    WaitCategory      sql.NullString
    WaitEventName     sql.NullString
    WaitingTasksCount sql.NullInt64
    TotalWaitTimeMs   sql.NullFloat64
    AvgWaitTimeMs     sql.NullFloat64
}

// GetDatabaseName returns the database name as a string, empty if null
func (we *WaitEvent) GetDatabaseName() string {
    if we.DatabaseName.Valid {
        return we.DatabaseName.String
    }
    return ""
}

// GetQueryID returns the query ID as a string, empty if null
func (we *WaitEvent) GetQueryID() string {
    if we.QueryID.Valid {
        return we.QueryID.String
    }
    return ""
}

// GetWaitCategory returns the wait category as a string, empty if null
func (we *WaitEvent) GetWaitCategory() string {
    if we.WaitCategory.Valid {
        return we.WaitCategory.String
    }
    return ""
}

// GetWaitEventName returns the wait event name as a string, empty if null
func (we *WaitEvent) GetWaitEventName() string {
    if we.WaitEventName.Valid {
        return we.WaitEventName.String
    }
    return ""
}

// HasValidQueryID checks if the wait event has a valid query ID
func (we *WaitEvent) HasValidQueryID() bool {
    return we.QueryID.Valid
}

// HasValidWaitTime checks if the wait event has valid wait time data
func (we *WaitEvent) HasValidWaitTime() bool {
    return we.TotalWaitTimeMs.Valid && we.TotalWaitTimeMs.Float64 >= 0
}

// HasValidEventName checks if the wait event has a valid event name
func (we *WaitEvent) HasValidEventName() bool {
    return we.WaitEventName.Valid
}

// IsValidForMetrics checks if the wait event has the minimum required fields for metrics
func (we *WaitEvent) IsValidForMetrics() bool {
    return we.HasValidQueryID() && we.HasValidWaitTime() && we.HasValidEventName()
}

// GetWaitingTasksCount returns the waiting tasks count, 0 if null
func (we *WaitEvent) GetWaitingTasksCount() int64 {
    if we.WaitingTasksCount.Valid {
        return we.WaitingTasksCount.Int64
    }
    return 0
}

// GetTotalWaitTimeMs returns the total wait time in milliseconds, 0 if null
func (we *WaitEvent) GetTotalWaitTimeMs() float64 {
    if we.TotalWaitTimeMs.Valid {
        return we.TotalWaitTimeMs.Float64
    }
    return 0
}

// GetAvgWaitTimeMs returns the average wait time in milliseconds, 0 if null
func (we *WaitEvent) GetAvgWaitTimeMs() float64 {
    if we.AvgWaitTimeMs.Valid {
        return we.AvgWaitTimeMs.Float64
    }
    return 0
}