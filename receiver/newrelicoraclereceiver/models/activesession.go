package models

import (
	"database/sql"
	"time"
)

// ActiveSession represents an active session executing a query
type ActiveSession struct {
	Username       sql.NullString
	SID            sql.NullInt64
	Serial         sql.NullInt64
	QueryID        sql.NullString
	SQLChildNumber sql.NullInt64
	SQLExecStart   sql.NullTime
	SQLExecID      sql.NullInt64
	SecondsInWait  sql.NullFloat64
}

// GetUsername returns the username as a string, empty if null
func (as *ActiveSession) GetUsername() string {
	if as.Username.Valid {
		return as.Username.String
	}
	return ""
}

// GetSID returns the session ID as int64, 0 if null
func (as *ActiveSession) GetSID() int64 {
	if as.SID.Valid {
		return as.SID.Int64
	}
	return 0
}

// GetSerial returns the serial number as int64, 0 if null
func (as *ActiveSession) GetSerial() int64 {
	if as.Serial.Valid {
		return as.Serial.Int64
	}
	return 0
}

// GetQueryID returns the query ID as a string, empty if null
func (as *ActiveSession) GetQueryID() string {
	if as.QueryID.Valid {
		return as.QueryID.String
	}
	return ""
}

// GetSQLChildNumber returns the SQL child number as int64, 0 if null
func (as *ActiveSession) GetSQLChildNumber() int64 {
	if as.SQLChildNumber.Valid {
		return as.SQLChildNumber.Int64
	}
	return 0
}

// GetSQLExecStart returns the SQL execution start time, zero time if null
func (as *ActiveSession) GetSQLExecStart() time.Time {
	if as.SQLExecStart.Valid {
		return as.SQLExecStart.Time
	}
	return time.Time{}
}

// GetSQLExecID returns the SQL execution ID as int64, 0 if null
func (as *ActiveSession) GetSQLExecID() int64 {
	if as.SQLExecID.Valid {
		return as.SQLExecID.Int64
	}
	return 0
}

// GetSecondsInWait returns the seconds in wait as int64, 0 if null
func (as *ActiveSession) GetSecondsInWait() float64 {
	if as.SecondsInWait.Valid {
		return as.SecondsInWait.Float64
	}
	return 0
}

// IsValidForMetrics checks if the active session has the minimum required fields
func (as *ActiveSession) IsValidForMetrics() bool {
	return as.QueryID.Valid && as.SID.Valid
}
