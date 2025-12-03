package models

import (
	"database/sql"
	"time"
)

// WaitEventWithBlocking represents a unified record combining wait events and blocking session information
// This model is populated by the combined GetWaitEventsAndBlockingSQL query
// Field order matches SQL column order from GetWaitEventsAndBlockingSQL
type WaitEventWithBlocking struct {
	// 1. COLLECTION_TIMESTAMP
	CollectionTimestamp sql.NullTime
	// 2. database_name
	DatabaseName sql.NullString

	// Session identification (from waiting/blocked session)
	// 3. username
	Username sql.NullString
	// 4. sid
	SID sql.NullInt64
	// 5. serial#
	Serial sql.NullInt64
	// 6. status
	Status sql.NullString

	// SQL identification
	// 7. sql_id
	SQLID sql.NullString
	// 8. SQL_CHILD_NUMBER
	SQLChildNumber sql.NullInt64

	// Wait event information
	// 9. wait_class
	WaitClass sql.NullString
	// 10. event
	Event sql.NullString
	// 11. wait_time_ms (milliseconds the session has been waiting - general wait time)
	WaitTimeMs sql.NullFloat64
	// 12. blocked_time_ms (milliseconds waiting due to blocking - ONLY populated when BLOCKING_SESSION IS NOT NULL)
	BlockedTimeMs sql.NullFloat64
	// 13. time_since_last_wait_ms (milliseconds since last wait, useful for ON CPU time)
	TimeSinceLastWaitMs sql.NullFloat64
	// 14. time_remaining_ms (milliseconds remaining, NULL for indefinite waits)
	TimeRemainingMs sql.NullFloat64

	// SQL execution context
	// 15. SQL_EXEC_START
	SQLExecStart sql.NullTime
	// 16. SQL_EXEC_ID
	SQLExecID sql.NullInt64

	// Session context
	// 17. PROGRAM
	Program sql.NullString
	// 18. MACHINE
	Machine sql.NullString

	// Object being waited on
	// 19. ROW_WAIT_OBJ#
	RowWaitObj sql.NullInt64
	// 20. OWNER
	Owner sql.NullString
	// 21. OBJECT_NAME
	ObjectName sql.NullString
	// 22. OBJECT_TYPE
	ObjectType sql.NullString
	// 23. ROW_WAIT_FILE#
	RowWaitFile sql.NullInt64
	// 24. ROW_WAIT_BLOCK#
	RowWaitBlock sql.NullInt64

	// Wait parameters
	// 25. p1text
	P1Text sql.NullString
	// 26. p1
	P1 sql.NullInt64
	// 27. p2text
	P2Text sql.NullString
	// 28. p2
	P2 sql.NullInt64
	// 29. p3text
	P3Text sql.NullString
	// 30. p3
	P3 sql.NullInt64

	// Blocking session context
	// 31. BLOCKING_SESSION_STATUS
	BlockingSessionStatus sql.NullString
	// 32. immediate_blocker_sid
	ImmediateBlockerSID sql.NullInt64
	// 33. FINAL_BLOCKING_SESSION_STATUS
	FinalBlockingSessionStatus sql.NullString
	// 34. final_blocker_sid
	FinalBlockerSID sql.NullInt64

	// Final blocker details
	// 35. final_blocker_user
	FinalBlockerUser sql.NullString
	// 36. final_blocker_serial
	FinalBlockerSerial sql.NullInt64
	// 37. final_blocker_query_id
	FinalBlockerQueryID sql.NullString
	// 38. final_blocker_query_text
	FinalBlockerQueryText sql.NullString
}

// ========================================
// Getter methods for wait event fields
// ========================================

func (w *WaitEventWithBlocking) GetUsername() string {
	if w.Username.Valid {
		return w.Username.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetStatus() string {
	if w.Status.Valid {
		return w.Status.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetQueryID() string {
	if w.SQLID.Valid {
		return w.SQLID.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetWaitCategory() string {
	if w.WaitClass.Valid {
		return w.WaitClass.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetWaitEventName() string {
	if w.Event.Valid {
		return w.Event.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetProgram() string {
	if w.Program.Valid {
		return w.Program.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetMachine() string {
	if w.Machine.Valid {
		return w.Machine.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetObjectOwner() string {
	if w.Owner.Valid {
		return w.Owner.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetObjectNameWaitedOn() string {
	if w.ObjectName.Valid {
		return w.ObjectName.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetObjectTypeWaitedOn() string {
	if w.ObjectType.Valid {
		return w.ObjectType.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetP1Text() string {
	if w.P1Text.Valid {
		return w.P1Text.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetP2Text() string {
	if w.P2Text.Valid {
		return w.P2Text.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetP3Text() string {
	if w.P3Text.Valid {
		return w.P3Text.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetSID() int64 {
	if w.SID.Valid {
		return w.SID.Int64
	}
	return 0
}

func (w *WaitEventWithBlocking) GetSerial() int64 {
	if w.Serial.Valid {
		return w.Serial.Int64
	}
	return 0
}

func (w *WaitEventWithBlocking) GetSQLChildNumber() int64 {
	if w.SQLChildNumber.Valid {
		return w.SQLChildNumber.Int64
	}
	return -1 // Return -1 for NULL to distinguish from legitimate child_number 0
}

func (w *WaitEventWithBlocking) GetSQLExecID() int64 {
	if w.SQLExecID.Valid {
		return w.SQLExecID.Int64
	}
	return -1
}

// GetCurrentWaitMs returns wait time in milliseconds (general wait time)
func (w *WaitEventWithBlocking) GetCurrentWaitMs() float64 {
	if w.WaitTimeMs.Valid {
		return w.WaitTimeMs.Float64
	}
	return 0
}

// GetBlockedTimeMs returns blocked wait time in milliseconds (ONLY populated when there's a blocker)
func (w *WaitEventWithBlocking) GetBlockedTimeMs() float64 {
	if w.BlockedTimeMs.Valid {
		return w.BlockedTimeMs.Float64
	}
	return 0
}

// GetTimeSinceLastWaitMs returns time since last wait in milliseconds (useful for ON CPU time)
func (w *WaitEventWithBlocking) GetTimeSinceLastWaitMs() float64 {
	if w.TimeSinceLastWaitMs.Valid {
		return w.TimeSinceLastWaitMs.Float64
	}
	return 0
}

// GetTimeRemainingMs returns time remaining in milliseconds (NULL/0 for indefinite waits)
func (w *WaitEventWithBlocking) GetTimeRemainingMs() float64 {
	if w.TimeRemainingMs.Valid {
		return w.TimeRemainingMs.Float64
	}
	return 0
}

func (w *WaitEventWithBlocking) GetLockedObjectID() int64 {
	if w.RowWaitObj.Valid {
		return w.RowWaitObj.Int64
	}
	return 0
}

func (w *WaitEventWithBlocking) GetLockedFileID() int64 {
	if w.RowWaitFile.Valid {
		return w.RowWaitFile.Int64
	}
	return 0
}

func (w *WaitEventWithBlocking) GetLockedBlockID() int64 {
	if w.RowWaitBlock.Valid {
		return w.RowWaitBlock.Int64
	}
	return 0
}

func (w *WaitEventWithBlocking) GetP1() int64 {
	if w.P1.Valid {
		return w.P1.Int64
	}
	return 0
}

func (w *WaitEventWithBlocking) GetP2() int64 {
	if w.P2.Valid {
		return w.P2.Int64
	}
	return 0
}

func (w *WaitEventWithBlocking) GetP3() int64 {
	if w.P3.Valid {
		return w.P3.Int64
	}
	return 0
}

func (w *WaitEventWithBlocking) GetSQLExecStart() time.Time {
	if w.SQLExecStart.Valid {
		return w.SQLExecStart.Time
	}
	return time.Time{}
}

func (w *WaitEventWithBlocking) GetCollectionTimestamp() time.Time {
	if w.CollectionTimestamp.Valid {
		return w.CollectionTimestamp.Time
	}
	return time.Time{}
}

func (w *WaitEventWithBlocking) GetDatabaseName() string {
	if w.DatabaseName.Valid {
		return w.DatabaseName.String
	}
	return ""
}

// ========================================
// Getter methods for blocking fields
// ========================================

func (w *WaitEventWithBlocking) GetBlockingSessionStatus() string {
	if w.BlockingSessionStatus.Valid {
		return w.BlockingSessionStatus.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetImmediateBlockerSID() int64 {
	if w.ImmediateBlockerSID.Valid {
		return w.ImmediateBlockerSID.Int64
	}
	return 0
}

func (w *WaitEventWithBlocking) GetFinalBlockingSessionStatus() string {
	if w.FinalBlockingSessionStatus.Valid {
		return w.FinalBlockingSessionStatus.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetFinalBlockerSID() int64 {
	if w.FinalBlockerSID.Valid {
		return w.FinalBlockerSID.Int64
	}
	return 0
}

func (w *WaitEventWithBlocking) GetFinalBlockerUser() string {
	if w.FinalBlockerUser.Valid {
		return w.FinalBlockerUser.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetFinalBlockerSerial() int64 {
	if w.FinalBlockerSerial.Valid {
		return w.FinalBlockerSerial.Int64
	}
	return 0
}

func (w *WaitEventWithBlocking) GetFinalBlockerQueryID() string {
	if w.FinalBlockerQueryID.Valid {
		return w.FinalBlockerQueryID.String
	}
	return ""
}

func (w *WaitEventWithBlocking) GetFinalBlockerQueryText() string {
	if w.FinalBlockerQueryText.Valid {
		return w.FinalBlockerQueryText.String
	}
	return ""
}

// ========================================
// Validation methods
// ========================================

func (w *WaitEventWithBlocking) HasValidQueryID() bool {
	return w.SQLID.Valid
}

func (w *WaitEventWithBlocking) HasValidWaitEventName() bool {
	return w.Event.Valid
}

func (w *WaitEventWithBlocking) HasValidCurrentWaitSeconds() bool {
	return w.WaitTimeMs.Valid && w.WaitTimeMs.Float64 > 0
}

func (w *WaitEventWithBlocking) IsValidForMetrics() bool {
	return w.HasValidQueryID() && w.HasValidWaitEventName() && w.HasValidCurrentWaitSeconds()
}

// IsBlocked checks if this session is currently blocked by another session
func (w *WaitEventWithBlocking) IsBlocked() bool {
	return w.FinalBlockerSID.Valid && w.FinalBlockerSID.Int64 > 0
}

// HasBlockingInfo checks if blocking information is available
func (w *WaitEventWithBlocking) HasBlockingInfo() bool {
	return w.IsBlocked() && w.FinalBlockerQueryID.Valid
}
