package models

import "database/sql"

// IndividualQuery represents an individual query record from Oracle V$SESSION and V$SQL views
type IndividualQuery struct {
	SID           sql.NullInt64
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

// GetSID returns the session ID as an int64, 0 if null
func (iq *IndividualQuery) GetSID() int64 {
	if iq.SID.Valid {
		return iq.SID.Int64
	}
	return 0
}

// GetSerial returns the serial number as an int64, 0 if null
func (iq *IndividualQuery) GetSerial() int64 {
	if iq.Serial.Valid {
		return iq.Serial.Int64
	}
	return 0
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

// GetPlanHashValue returns the plan hash value as an int64, 0 if null
func (iq *IndividualQuery) GetPlanHashValue() int64 {
	if iq.PlanHashValue.Valid {
		return iq.PlanHashValue.Int64
	}
	return 0
}

// GetElapsedTimeMs returns the elapsed time in milliseconds as a float64, 0 if null
func (iq *IndividualQuery) GetElapsedTimeMs() float64 {
	if iq.ElapsedTimeMs.Valid {
		return iq.ElapsedTimeMs.Float64
	}
	return 0
}

// GetCPUTimeMs returns the CPU time in milliseconds as a float64, 0 if null
func (iq *IndividualQuery) GetCPUTimeMs() float64 {
	if iq.CPUTimeMs.Valid {
		return iq.CPUTimeMs.Float64
	}
	return 0
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

// HasValidSID checks if the query has a valid session ID
func (iq *IndividualQuery) HasValidSID() bool {
	return iq.SID.Valid
}

// IsValidForMetrics checks if the individual query has the minimum required fields for metrics
func (iq *IndividualQuery) IsValidForMetrics() bool {
	return iq.HasValidQueryID() && iq.HasValidSID() && iq.HasValidElapsedTime()
}
