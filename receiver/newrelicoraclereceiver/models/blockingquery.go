package models

import "database/sql"

// BlockingQuery represents a blocking query record from Oracle V$SESSION views
type BlockingQuery struct {
	BlockedSID       sql.NullInt64
	BlockedSerial    sql.NullInt64
	BlockedUser      sql.NullString
	BlockedWaitSec   sql.NullFloat64
	QueryID          sql.NullString
	BlockedQueryText sql.NullString
	BlockingSID      sql.NullInt64
	BlockingSerial   sql.NullInt64
	BlockingUser     sql.NullString
	BlockingSQLID    sql.NullString
	BlockingQueryText sql.NullString
	DatabaseName     sql.NullString
}

// GetBlockedUser returns the blocked user as a string, empty if null
func (bq *BlockingQuery) GetBlockedUser() string {
	if bq.BlockedUser.Valid {
		return bq.BlockedUser.String
	}
	return ""
}

// GetBlockedQueryText returns the blocked query text as a string, empty if null
func (bq *BlockingQuery) GetBlockedQueryText() string {
	if bq.BlockedQueryText.Valid {
		return bq.BlockedQueryText.String
	}
	return ""
}

// GetBlockingUser returns the blocking user as a string, empty if null
func (bq *BlockingQuery) GetBlockingUser() string {
	if bq.BlockingUser.Valid {
		return bq.BlockingUser.String
	}
	return ""
}

// GetQueryID returns the query ID as a string, empty if null
func (bq *BlockingQuery) GetQueryID() string {
	if bq.QueryID.Valid {
		return bq.QueryID.String
	}
	return ""
}

// GetDatabaseName returns the database name as a string, empty if null
func (bq *BlockingQuery) GetDatabaseName() string {
	if bq.DatabaseName.Valid {
		return bq.DatabaseName.String
	}
	return ""
}

// GetBlockingSQLID returns the blocking SQL ID as a string, empty if null
func (bq *BlockingQuery) GetBlockingSQLID() string {
	if bq.BlockingSQLID.Valid {
		return bq.BlockingSQLID.String
	}
	return ""
}

// GetBlockingQueryText returns the blocking query text as a string, empty if null
func (bq *BlockingQuery) GetBlockingQueryText() string {
	if bq.BlockingQueryText.Valid {
		return bq.BlockingQueryText.String
	}
	return ""
}

// HasValidWaitTime checks if the query has a valid wait time
func (bq *BlockingQuery) HasValidWaitTime() bool {
	return bq.BlockedWaitSec.Valid && bq.BlockedWaitSec.Float64 >= 0
}

// HasValidQueryID checks if the query has a valid query ID
func (bq *BlockingQuery) HasValidQueryID() bool {
	return bq.QueryID.Valid
}

// IsValidForMetrics checks if the blocking query has the minimum required fields for metrics
func (bq *BlockingQuery) IsValidForMetrics() bool {
	return bq.HasValidQueryID() && bq.HasValidWaitTime()
}
