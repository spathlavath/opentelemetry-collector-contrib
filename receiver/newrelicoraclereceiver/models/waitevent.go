package models

import (
	"database/sql"
	"time"
)

// WaitEvent represents a wait event record from Oracle wait events queries
type WaitEvent struct {
	CollectionTimestamp sql.NullTime
	Username           sql.NullString
	SID                sql.NullInt64
	Status             sql.NullString
	QueryID            sql.NullString
	WaitCategory       sql.NullString
	WaitEventName      sql.NullString
	CurrentWaitSeconds sql.NullInt64
	SQLExecStart       sql.NullTime
	SQLExecID          sql.NullInt64
	Program            sql.NullString
	Machine            sql.NullString
	LockedObjectID     sql.NullInt64
	ObjectOwner        sql.NullString
	ObjectNameWaitedOn sql.NullString
	ObjectTypeWaitedOn sql.NullString
	LockedFileID       sql.NullInt64
	LockedBlockID      sql.NullInt64
	P1Text             sql.NullString
	P1                 sql.NullInt64
	P2Text             sql.NullString
	P2                 sql.NullInt64
	P3Text             sql.NullString
	P3                 sql.NullInt64
}

// GetUsername returns the username as a string, empty if null
func (we *WaitEvent) GetUsername() string {
	if we.Username.Valid {
		return we.Username.String
	}
	return ""
}

// GetStatus returns the session status as a string, empty if null
func (we *WaitEvent) GetStatus() string {
	if we.Status.Valid {
		return we.Status.String
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

// GetProgram returns the program name as a string, empty if null
func (we *WaitEvent) GetProgram() string {
	if we.Program.Valid {
		return we.Program.String
	}
	return ""
}

// GetMachine returns the machine name as a string, empty if null
func (we *WaitEvent) GetMachine() string {
	if we.Machine.Valid {
		return we.Machine.String
	}
	return ""
}

// GetObjectOwner returns the object owner as a string, empty if null
func (we *WaitEvent) GetObjectOwner() string {
	if we.ObjectOwner.Valid {
		return we.ObjectOwner.String
	}
	return ""
}

// GetObjectNameWaitedOn returns the object name being waited on as a string, empty if null
func (we *WaitEvent) GetObjectNameWaitedOn() string {
	if we.ObjectNameWaitedOn.Valid {
		return we.ObjectNameWaitedOn.String
	}
	return ""
}

// GetObjectTypeWaitedOn returns the object type being waited on as a string, empty if null
func (we *WaitEvent) GetObjectTypeWaitedOn() string {
	if we.ObjectTypeWaitedOn.Valid {
		return we.ObjectTypeWaitedOn.String
	}
	return ""
}

// GetP1Text returns the P1 parameter text as a string, empty if null
func (we *WaitEvent) GetP1Text() string {
	if we.P1Text.Valid {
		return we.P1Text.String
	}
	return ""
}

// GetP2Text returns the P2 parameter text as a string, empty if null
func (we *WaitEvent) GetP2Text() string {
	if we.P2Text.Valid {
		return we.P2Text.String
	}
	return ""
}

// GetP3Text returns the P3 parameter text as a string, empty if null
func (we *WaitEvent) GetP3Text() string {
	if we.P3Text.Valid {
		return we.P3Text.String
	}
	return ""
}

// GetSID returns the session ID as int64, 0 if null
func (we *WaitEvent) GetSID() int64 {
	if we.SID.Valid {
		return we.SID.Int64
	}
	return 0
}

// GetSQLExecID returns the SQL execution ID as int64, -1 if null
func (we *WaitEvent) GetSQLExecID() int64 {
	if we.SQLExecID.Valid {
		return we.SQLExecID.Int64
	}
	return -1
}

// GetCurrentWaitSeconds returns the current wait seconds as int64, 0 if null
func (we *WaitEvent) GetCurrentWaitSeconds() int64 {
	if we.CurrentWaitSeconds.Valid {
		return we.CurrentWaitSeconds.Int64
	}
	return 0
}

// GetLockedObjectID returns the locked object ID as int64, 0 if null
func (we *WaitEvent) GetLockedObjectID() int64 {
	if we.LockedObjectID.Valid {
		return we.LockedObjectID.Int64
	}
	return 0
}

// GetLockedFileID returns the locked file ID as int64, 0 if null
func (we *WaitEvent) GetLockedFileID() int64 {
	if we.LockedFileID.Valid {
		return we.LockedFileID.Int64
	}
	return 0
}

// GetLockedBlockID returns the locked block ID as int64, 0 if null
func (we *WaitEvent) GetLockedBlockID() int64 {
	if we.LockedBlockID.Valid {
		return we.LockedBlockID.Int64
	}
	return 0
}

// GetP1 returns the P1 parameter as int64, 0 if null
func (we *WaitEvent) GetP1() int64 {
	if we.P1.Valid {
		return we.P1.Int64
	}
	return 0
}

// GetP2 returns the P2 parameter as int64, 0 if null
func (we *WaitEvent) GetP2() int64 {
	if we.P2.Valid {
		return we.P2.Int64
	}
	return 0
}

// GetP3 returns the P3 parameter as int64, 0 if null
func (we *WaitEvent) GetP3() int64 {
	if we.P3.Valid {
		return we.P3.Int64
	}
	return 0
}

// GetSQLExecStart returns the SQL execution start time, zero time if null
func (we *WaitEvent) GetSQLExecStart() time.Time {
	if we.SQLExecStart.Valid {
		return we.SQLExecStart.Time
	}
	return time.Time{}
}

// HasValidQueryID checks if the wait event has a valid query ID
func (we *WaitEvent) HasValidQueryID() bool {
	return we.QueryID.Valid
}

// HasValidWaitEventName checks if the wait event has a valid wait event name
func (we *WaitEvent) HasValidWaitEventName() bool {
	return we.WaitEventName.Valid
}

// HasValidCurrentWaitSeconds checks if the wait event has valid current wait seconds
func (we *WaitEvent) HasValidCurrentWaitSeconds() bool {
	return we.CurrentWaitSeconds.Valid
}

// IsValidForMetrics checks if the wait event has the minimum required fields for metrics
func (we *WaitEvent) IsValidForMetrics() bool {
	return we.HasValidQueryID() && we.HasValidWaitEventName() && we.HasValidCurrentWaitSeconds()
}

// GetCollectionTimestamp returns the collection timestamp, or zero time if null
func (we *WaitEvent) GetCollectionTimestamp() time.Time {
	if we.CollectionTimestamp.Valid {
		return we.CollectionTimestamp.Time
	}
	return time.Time{}
}
