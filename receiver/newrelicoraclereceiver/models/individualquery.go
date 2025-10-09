package models

import (
	"database/sql"
	"strconv"
)

// IndividualQuery represents an individual query record from Oracle V$SESSION and V$SQL views
type IndividualQuery struct {
	SessionID     sql.NullInt64
	Serial        sql.NullInt64
	Username      sql.NullString
	Status        sql.NullString
	QueryID       sql.NullString
	PlanHashValue sql.NullInt64
	ElapsedTimeMs sql.NullFloat64
	CPUTimeMs     sql.NullFloat64
	OSUser        sql.NullString
	Hostname      sql.NullString
	QueryText     sql.NullString
}

// GetSessionID returns the session ID as a string, empty if null
func (iq *IndividualQuery) GetSessionID() string {
	if iq.SessionID.Valid {
		return strconv.FormatInt(iq.SessionID.Int64, 10)
	}
	return ""
}

// GetSerial returns the serial number as a string, empty if null
func (iq *IndividualQuery) GetSerial() string {
	if iq.Serial.Valid {
		return strconv.FormatInt(iq.Serial.Int64, 10)
	}
	return ""
}

// GetUsername returns the username as a string, empty if null
func (iq *IndividualQuery) GetUsername() string {
	if iq.Username.Valid {
		return iq.Username.String
	}
	return ""
}

// GetStatus returns the status as a string, empty if null
func (iq *IndividualQuery) GetStatus() string {
	if iq.Status.Valid {
		return iq.Status.String
	}
	return ""
}

// GetQueryID returns the query ID as a string, empty if null
func (iq *IndividualQuery) GetQueryID() string {
	if iq.QueryID.Valid {
		return iq.QueryID.String
	}
	return ""
}

// GetPlanHashValue returns the plan hash value as a string, empty if null
func (iq *IndividualQuery) GetPlanHashValue() string {
	if iq.PlanHashValue.Valid {
		return strconv.FormatInt(iq.PlanHashValue.Int64, 10)
	}
	return ""
}

// GetOSUser returns the OS user as a string, empty if null
func (iq *IndividualQuery) GetOSUser() string {
	if iq.OSUser.Valid {
		return iq.OSUser.String
	}
	return ""
}

// GetHostname returns the hostname as a string, empty if null
func (iq *IndividualQuery) GetHostname() string {
	if iq.Hostname.Valid {
		return iq.Hostname.String
	}
	return ""
}

// GetQueryText returns the query text as a string, empty if null
func (iq *IndividualQuery) GetQueryText() string {
	if iq.QueryText.Valid {
		return iq.QueryText.String
	}
	return ""
}

// HasValidQueryID checks if the query has a valid query ID
func (iq *IndividualQuery) HasValidQueryID() bool {
	return iq.QueryID.Valid
}

// HasValidElapsedTime checks if the query has a valid elapsed time
func (iq *IndividualQuery) HasValidElapsedTime() bool {
	return iq.ElapsedTimeMs.Valid
}

// IsValidForMetrics checks if the individual query has the minimum required fields for metrics
func (iq *IndividualQuery) IsValidForMetrics() bool {
	return iq.HasValidQueryID() && iq.HasValidElapsedTime()
}
