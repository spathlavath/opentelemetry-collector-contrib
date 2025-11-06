package queries

// Lock-related SQL queries for Oracle database

const (
	// LockCountSQL returns count of locks by type and mode with instance ID for RAC support
	LockCountSQL = `
		SELECT 
			INST_ID,
			TYPE AS lock_type,
			DECODE(LMODE, 
				0, 'None',
				1, 'Null',
				2, 'Row-S (SS)', 
				3, 'Row-X (SX)',
				4, 'Share (S)',
				5, 'S/Row-X (SSX)',
				6, 'Exclusive (X)',
				'Unknown') AS lock_mode,
			COUNT(*) AS lock_count
		FROM GV$LOCK
		WHERE LMODE > 0
		GROUP BY INST_ID, TYPE, LMODE
		ORDER BY INST_ID, TYPE, LMODE`

	// LockSessionCountSQL returns count of sessions holding locks by type with instance ID
	LockSessionCountSQL = `
		SELECT 
			INST_ID,
			TYPE AS lock_type,
			COUNT(DISTINCT SID) AS session_count
		FROM GV$LOCK
		WHERE LMODE > 0
		GROUP BY INST_ID, TYPE
		ORDER BY INST_ID, TYPE`

	// LockedObjectCountSQL returns count of locked objects by lock type and object type with instance ID
	LockedObjectCountSQL = `
		SELECT 
			l.INST_ID,
			l.TYPE AS lock_type,
			o.OBJECT_TYPE,
			COUNT(DISTINCT l.ID1) AS object_count
		FROM GV$LOCK l
		JOIN DBA_OBJECTS o ON l.ID1 = o.OBJECT_ID
		WHERE l.LMODE > 0 
			AND l.TYPE IN ('TM', 'TX')
		GROUP BY l.INST_ID, l.TYPE, o.OBJECT_TYPE
		ORDER BY l.INST_ID, l.TYPE, o.OBJECT_TYPE`

	// DeadlockCountSQL returns cumulative count of deadlocks detected
	DeadlockCountSQL = `
		SELECT VALUE AS deadlock_count
		FROM V$SYSSTAT
		WHERE NAME = 'enqueue deadlocks'`

	// DetailedLockInfoSQL returns detailed lock information for monitoring
	DetailedLockInfoSQL = `
		SELECT 
			l.SID,
			s.USERNAME,
			l.TYPE AS lock_type,
			DECODE(l.LMODE, 
				0, 'None',
				1, 'Null',
				2, 'Row-S (SS)', 
				3, 'Row-X (SX)',
				4, 'Share (S)',
				5, 'S/Row-X (SSX)',
				6, 'Exclusive (X)',
				'Unknown') AS lock_mode,
			DECODE(l.REQUEST,
				0, 'None',
				1, 'Null',
				2, 'Row-S (SS)', 
				3, 'Row-X (SX)',
				4, 'Share (S)',
				5, 'S/Row-X (SSX)',
				6, 'Exclusive (X)',
				'Unknown') AS lock_request,
			o.OWNER,
			o.OBJECT_NAME,
			o.OBJECT_TYPE,
			l.BLOCK
		FROM V$LOCK l
		JOIN V$SESSION s ON l.SID = s.SID
		LEFT JOIN DBA_OBJECTS o ON l.ID1 = o.OBJECT_ID
		WHERE l.LMODE > 0
			AND l.TYPE IN ('TM', 'TX', 'UL', 'DX')
		ORDER BY l.TYPE, l.SID`
)
