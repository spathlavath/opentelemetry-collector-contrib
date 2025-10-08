package models

import "database/sql"

// IndividualQuery represents an individual query record from Oracle V$SQL view
type IndividualQuery struct {
	QueryID       sql.NullString
	QueryText     sql.NullString
	CPUTimeMs     sql.NullFloat64
	ElapsedTimeMs sql.NullFloat64
}

// GetQueryID returns the query ID as a string, empty if null
func (iq *IndividualQuery) GetQueryID() string {
	if iq.QueryID.Valid {
		return iq.QueryID.String
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
