// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package helpers

import (
	"fmt"
	"regexp"
	"strings"
)

// DecodeWaitType returns a human-readable description for SQL Server wait types
func DecodeWaitType(waitType string) string {
	if waitType == "" || waitType == "N/A" {
		return "Not Waiting"
	}

	waitTypeMap := map[string]string{
		// Parallelism waits
		"CXPACKET":        "Parallel Query - Thread Synchronization",
		"CXSYNC_PORT":     "Parallel Query - Thread Coordination",
		"CXSYNC_CONSUMER": "Parallel Query - Consumer Thread Wait",
		"CXCONSUMER":      "Parallel Query - Consumer Wait",
		"EXECSYNC":        "Parallel Query - Execution Sync",
		"EXCHANGE":        "Parallel Query - Data Exchange",

		// Disk I/O waits
		"PAGEIOLATCH_SH":      "Disk I/O - Reading Data Page (Shared)",
		"PAGEIOLATCH_EX":      "Disk I/O - Writing Data Page (Exclusive)",
		"PAGEIOLATCH_UP":      "Disk I/O - Updating Data Page",
		"PAGEIOLATCH_KP":      "Disk I/O - Keep Page",
		"PAGEIOLATCH_DT":      "Disk I/O - Destroy Page",
		"PAGEIOLATCH_NL":      "Disk I/O - No Latch",
		"WRITELOG":            "Transaction Log - Writing to Disk",
		"LOGBUFFER":           "Transaction Log - Buffer Wait",
		"LOGMGR_FLUSH":        "Transaction Log - Flush Wait",
		"IO_COMPLETION":       "Disk I/O - Completion Wait",
		"ASYNC_IO_COMPLETION": "Disk I/O - Async Completion",

		// Lock waits
		"LCK_M_X":      "Lock - Exclusive Lock Contention",
		"LCK_M_S":      "Lock - Shared Lock Contention",
		"LCK_M_U":      "Lock - Update Lock Contention",
		"LCK_M_IX":     "Lock - Intent Exclusive",
		"LCK_M_IS":     "Lock - Intent Shared",
		"LCK_M_IU":     "Lock - Intent Update",
		"LCK_M_SIX":    "Lock - Shared Intent Exclusive",
		"LCK_M_SIU":    "Lock - Shared Intent Update",
		"LCK_M_UIX":    "Lock - Update Intent Exclusive",
		"LCK_M_BU":     "Lock - Bulk Update",
		"LCK_M_RS_S":   "Lock - Range Shared-Shared",
		"LCK_M_RS_U":   "Lock - Range Shared-Update",
		"LCK_M_RX_S":   "Lock - Range Exclusive-Shared",
		"LCK_M_RX_U":   "Lock - Range Exclusive-Update",
		"LCK_M_RX_X":   "Lock - Range Exclusive-Exclusive",
		"LCK_M_RIn_NL": "Lock - Range Insert Null",
		"LCK_M_RIn_S":  "Lock - Range Insert Shared",
		"LCK_M_RIn_U":  "Lock - Range Insert Update",
		"LCK_M_RIn_X":  "Lock - Range Insert Exclusive",

		// Memory waits
		"RESOURCE_SEMAPHORE":                              "Memory - Waiting for Memory Grant",
		"RESOURCE_SEMAPHORE_QUERY_COMPILE":                "Memory - Query Compilation Memory",
		"CMEMTHREAD":                                      "Memory - Thread Memory Wait",
		"PWAIT_RESOURCE_SEMAPHORE_FT_PARALLEL_QUERY_SYNC": "Memory - Full-Text Query Sync",

		// Network waits
		"ASYNC_NETWORK_IO":   "Network - Client Not Consuming Results",
		"NETWORKIO":          "Network - General Network I/O",
		"NET_WAITFOR_PACKET": "Network - Waiting for Packet",

		// CPU waits
		"SOS_SCHEDULER_YIELD":   "CPU - High CPU Pressure (Thread Yield)",
		"THREADPOOL":            "CPU - Thread Pool Starvation",
		"SQLTRACE_WAIT_ENTRIES": "CPU - SQL Trace Wait",

		// Latch waits (in-memory)
		"PAGELATCH_SH": "Latch - Shared Page Latch (In-Memory Buffer Contention)",
		"PAGELATCH_EX": "Latch - Exclusive Page Latch (In-Memory Buffer Contention)",
		"PAGELATCH_UP": "Latch - Update Page Latch (In-Memory Buffer Contention)",
		"LATCH_EX":     "Latch - Exclusive Latch Wait",
		"LATCH_SH":     "Latch - Shared Latch Wait",
		"LATCH_UP":     "Latch - Update Latch Wait",
		"LATCH_KP":     "Latch - Keep Latch Wait",
		"LATCH_DT":     "Latch - Destroy Latch Wait",

		// Transaction waits
		"DTC":               "Transaction - Distributed Transaction Wait",
		"XACTLOCKINFO":      "Transaction - Lock Info Wait",
		"TRANSACTION_MUTEX": "Transaction - Mutex Wait",

		// Backup/Restore waits
		"BACKUP":                            "Backup - Backup Operation Wait",
		"BACKUPBUFFER":                      "Backup - Buffer Wait",
		"BACKUPIO":                          "Backup - I/O Wait",
		"RESTORE_FILEHANDLECACHE_ENTRYLOCK": "Restore - File Handle Cache",

		// PREEMPTIVE waits (OS operations)
		"PREEMPTIVE_OS_QUERYREGISTRY":     "OS Call - Registry Query",
		"PREEMPTIVE_OS_FILEOPS":           "OS Call - File Operations",
		"PREEMPTIVE_OS_LIBRARYOPS":        "OS Call - Library Operations",
		"PREEMPTIVE_OS_CRYPTOPS":          "OS Call - Cryptographic Operations",
		"PREEMPTIVE_OS_AUTHENTICATIONOPS": "OS Call - Authentication",
		"PREEMPTIVE_OS_GENERICOPS":        "OS Call - Generic Operations",

		// Availability Group waits
		"HADR_SYNC_COMMIT":            "AlwaysOn - Sync Commit Wait",
		"HADR_SYNCHRONIZING_THROTTLE": "AlwaysOn - Synchronizing Throttle",
		"HADR_LOGCAPTURE_WAIT":        "AlwaysOn - Log Capture Wait",

		// Other common waits
		"SLEEP_TASK":             "Sleep - Intentional Wait (e.g., WAITFOR)",
		"SLEEP_BPOOL_STEAL":      "Sleep - Buffer Pool Steal",
		"DBMIRROR_EVENTS_QUEUE":  "Database Mirroring - Event Queue",
		"BROKER_RECEIVE_WAITFOR": "Service Broker - Receive Wait",
	}

	if description, exists := waitTypeMap[waitType]; exists {
		return description
	}

	// Handle prefix patterns
	if strings.HasPrefix(waitType, "PREEMPTIVE_") {
		return fmt.Sprintf("OS Call - %s", strings.TrimPrefix(waitType, "PREEMPTIVE_"))
	}
	if strings.HasPrefix(waitType, "HADR_") {
		return fmt.Sprintf("AlwaysOn - %s", strings.TrimPrefix(waitType, "HADR_"))
	}
	if strings.HasPrefix(waitType, "LCK_M_") {
		return fmt.Sprintf("Lock - %s", strings.TrimPrefix(waitType, "LCK_M_"))
	}
	if strings.HasPrefix(waitType, "PAGEIOLATCH_") {
		return fmt.Sprintf("Disk I/O - Page Latch %s", strings.TrimPrefix(waitType, "PAGEIOLATCH_"))
	}

	// Return original if unknown
	return waitType
}

// GetWaitTypeCategory returns the high-level category for a wait type
func GetWaitTypeCategory(waitType string) string {
	if waitType == "" || waitType == "N/A" {
		return "None"
	}

	switch {
	case strings.HasPrefix(waitType, "CXPACKET") || strings.HasPrefix(waitType, "CXSYNC") || strings.HasPrefix(waitType, "CXCONSUMER") || waitType == "EXECSYNC" || waitType == "EXCHANGE":
		return "Parallelism"
	case strings.HasPrefix(waitType, "PAGEIOLATCH") || strings.HasPrefix(waitType, "WRITELOG") || strings.HasPrefix(waitType, "LOGBUFFER") || strings.HasPrefix(waitType, "IO_"):
		return "Disk I/O"
	case strings.HasPrefix(waitType, "LCK_M_"):
		return "Locking"
	case strings.HasPrefix(waitType, "RESOURCE_SEMAPHORE") || strings.HasPrefix(waitType, "CMEMTHREAD"):
		return "Memory"
	case strings.HasPrefix(waitType, "ASYNC_NETWORK_IO") || strings.HasPrefix(waitType, "NETWORK"):
		return "Network"
	case waitType == "SOS_SCHEDULER_YIELD" || waitType == "THREADPOOL":
		return "CPU"
	case strings.HasPrefix(waitType, "LATCH_") || strings.HasPrefix(waitType, "PAGELATCH_"):
		return "Latch"
	case strings.HasPrefix(waitType, "PREEMPTIVE_"):
		return "OS Call"
	case strings.HasPrefix(waitType, "HADR_"):
		return "AlwaysOn"
	case strings.HasPrefix(waitType, "BACKUP"):
		return "Backup"
	case strings.HasPrefix(waitType, "DTC") || strings.HasPrefix(waitType, "XACT"):
		return "Transaction"
	case waitType == "SLEEP_TASK" || waitType == "SLEEP_BPOOL_STEAL":
		return "Sleep"
	default:
		return "Other"
	}
}

// DecodeWaitResource parses and returns human-readable wait resource information
// Based on sys.dm_tran_locks resource_description format
func DecodeWaitResource(waitResource string) (resourceType string, description string) {
	return DecodeWaitResourceWithMetadata(waitResource, nil)
}

// DecodeWaitResourceWithMetadata parses wait_resource with optional metadata enrichment
// If metadataCache is nil, returns basic ID-based descriptions
// If metadataCache is provided, enriches with database/object/file names
func DecodeWaitResourceWithMetadata(waitResource string, metadataCache *MetadataCache) (resourceType string, description string) {
	if waitResource == "" {
		return "N/A", "Not Applicable"
	}

	// KEY: <database_id>:<hobt_id> (<hash_value>)
	// Example: KEY: 5:72057594049986560 (419e4517fb6a)
	keyPattern := regexp.MustCompile(`^KEY:\s*(\d+):(\d+)\s*\(([a-f0-9]+)\)`)
	if matches := keyPattern.FindStringSubmatch(waitResource); matches != nil {
		dbID := matches[1]
		hobtID := matches[2]
		hashValue := matches[3]

		// Basic description with IDs
		desc := fmt.Sprintf("Database ID: %s | HOBT: %s | Key Hash: %s", dbID, hobtID, hashValue)

		// Enrich with metadata if available
		if metadataCache != nil {
			desc = enrichKeyLock(dbID, hobtID, hashValue, metadataCache)
		}

		return "Key Lock", desc
	}

	// PAGE: <database_id>:<file_id>:<page_id>
	// Example: PAGE: 5:1:104
	pagePattern := regexp.MustCompile(`^PAGE:\s*(\d+):(\d+):(\d+)`)
	if matches := pagePattern.FindStringSubmatch(waitResource); matches != nil {
		dbID := matches[1]
		fileID := matches[2]
		pageID := matches[3]

		desc := fmt.Sprintf("Database ID: %s | File: %s | Page: %s", dbID, fileID, pageID)
		if metadataCache != nil {
			desc = enrichPageLock(dbID, fileID, pageID, metadataCache)
		}

		return "Page Lock", desc
	}

	// RID: <database_id>:<file_id>:<page_id>:<row_on_page>
	// Example: RID: 5:1:104:0
	ridPattern := regexp.MustCompile(`^RID:\s*(\d+):(\d+):(\d+):(\d+)`)
	if matches := ridPattern.FindStringSubmatch(waitResource); matches != nil {
		dbID := matches[1]
		fileID := matches[2]
		pageID := matches[3]
		rowSlot := matches[4]

		desc := fmt.Sprintf("Database ID: %s | File: %s | Page: %s | Row: %s", dbID, fileID, pageID, rowSlot)
		if metadataCache != nil {
			desc = enrichRIDLock(dbID, fileID, pageID, rowSlot, metadataCache)
		}

		return "Row Lock", desc
	}

	// OBJECT: <database_id>:<object_id>:<lock_partition>
	// Example: OBJECT: 5:245575913:0
	objectPattern := regexp.MustCompile(`^OBJECT:\s*(\d+):(\d+):(\d+)`)
	if matches := objectPattern.FindStringSubmatch(waitResource); matches != nil {
		dbID := matches[1]
		objectID := matches[2]
		partition := matches[3]

		desc := fmt.Sprintf("Database ID: %s | Object ID: %s | Partition: %s", dbID, objectID, partition)
		if metadataCache != nil {
			desc = enrichObjectLock(dbID, objectID, partition, metadataCache)
		}

		return "Object Lock", desc
	}

	// DATABASE: <database_id>
	// Example: DATABASE: 5
	databasePattern := regexp.MustCompile(`^DATABASE:\s*(\d+)`)
	if matches := databasePattern.FindStringSubmatch(waitResource); matches != nil {
		dbID := matches[1]

		desc := fmt.Sprintf("Database ID: %s", dbID)
		if metadataCache != nil {
			desc = enrichDatabaseLock(dbID, metadataCache)
		}

		return "Database Lock", desc
	}

	// FILE: <database_id>:<file_id>
	// Example: FILE: 5:1
	filePattern := regexp.MustCompile(`^FILE:\s*(\d+):(\d+)`)
	if matches := filePattern.FindStringSubmatch(waitResource); matches != nil {
		dbID := matches[1]
		fileID := matches[2]

		desc := fmt.Sprintf("Database ID: %s | File: %s", dbID, fileID)
		if metadataCache != nil {
			desc = enrichFileLock(dbID, fileID, metadataCache)
		}

		return "File Lock", desc
	}

	// EXTENT: <database_id>:<file_id>:<page_id>
	// Example: EXTENT: 5:1:104
	extentPattern := regexp.MustCompile(`^EXTENT:\s*(\d+):(\d+):(\d+)`)
	if matches := extentPattern.FindStringSubmatch(waitResource); matches != nil {
		dbID := matches[1]
		fileID := matches[2]
		pageID := matches[3]

		desc := fmt.Sprintf("Database ID: %s | File: %s | First Page: %s", dbID, fileID, pageID)
		if metadataCache != nil {
			desc = enrichExtentLock(dbID, fileID, pageID, metadataCache)
		}

		return "Extent Lock", desc
	}

	// HOBT: Heap or B-tree
	// Example: HOBT: 72057594049986560
	hobtPattern := regexp.MustCompile(`^HOBT:\s*(\d+)`)
	if matches := hobtPattern.FindStringSubmatch(waitResource); matches != nil {
		hobtID := matches[1]

		desc := fmt.Sprintf("HOBT ID: %s", hobtID)
		if metadataCache != nil {
			desc = enrichHOBTLock(hobtID, metadataCache)
		}

		return "Heap/B-Tree Lock", desc
	}

	// APPLICATION: <DbPrincipalId>:<up to 32 characters>:(<hash_value>)
	// Example: APPLICATION: 1:MyAppLock:(abc123)
	applicationPattern := regexp.MustCompile(`^APPLICATION:\s*(\d+):([^:]+):\(([a-f0-9]+)\)`)
	if matches := applicationPattern.FindStringSubmatch(waitResource); matches != nil {
		principalID := matches[1]
		lockName := matches[2]
		hashValue := matches[3]
		return "Application Lock", fmt.Sprintf("Principal: %s | Lock Name: %s | Hash: %s", principalID, lockName, hashValue)
	}

	// Synchronization Object: 0x<hex_value>
	// Example: 0x82f2de703704dd7c (used in parallel query coordination)
	syncObjectPattern := regexp.MustCompile(`^0x([a-f0-9]+)$`)
	if matches := syncObjectPattern.FindStringSubmatch(waitResource); matches != nil {
		hexValue := matches[1]
		return "Synchronization Object", fmt.Sprintf("Parallel Query Sync Object: 0x%s", hexValue)
	}

	// METADATA: <metadata_class>:<metadata_id>
	// Example: METADATA: 21:5
	metadataPattern := regexp.MustCompile(`^METADATA:\s*(\d+):(\d+)`)
	if matches := metadataPattern.FindStringSubmatch(waitResource); matches != nil {
		metadataClass := matches[1]
		metadataID := matches[2]

		// Decode metadata class to human-readable name
		className := getMetadataClassName(metadataClass)
		desc := fmt.Sprintf("Metadata Class: %s (%s) | ID: %s", metadataClass, className, metadataID)

		return "Metadata Lock", desc
	}

	// ALLOCATION_UNIT: <allocation_unit_id>
	// Example: ALLOCATION_UNIT: 72057594044612608
	allocationUnitPattern := regexp.MustCompile(`^ALLOCATION_UNIT:\s*(\d+)`)
	if matches := allocationUnitPattern.FindStringSubmatch(waitResource); matches != nil {
		auID := matches[1]

		desc := fmt.Sprintf("Allocation Unit ID: %s", auID)
		if metadataCache != nil {
			desc = enrichAllocationUnitLock(auID, metadataCache)
		}

		return "Allocation Unit Lock", desc
	}

	// Unknown format - return as-is
	return "Unknown", waitResource
}

// getMetadataClassName returns human-readable name for metadata class ID
// Based on sys.dm_tran_locks documentation
func getMetadataClassName(classID string) string {
	metadataClasses := map[string]string{
		"0":  "DATABASE",
		"1":  "OBJECT",
		"2":  "INDEX",
		"3":  "STAT",
		"4":  "COLUMN",
		"5":  "KEY",
		"6":  "USER",
		"7":  "ROLE",
		"8":  "PARTITION_FUNCTION",
		"9":  "PARTITION_SCHEME",
		"10": "FILEGROUP",
		"11": "XML_SCHEMA_COLLECTION",
		"12": "SCHEMA",
		"13": "ASSEMBLY",
		"14": "DEFAULT_CONSTRAINT",
		"15": "CHECK_CONSTRAINT",
		"16": "FOREIGN_KEY_CONSTRAINT",
		"17": "PRIMARY_KEY_CONSTRAINT",
		"18": "UNIQUE_CONSTRAINT",
		"19": "SERVICE",
		"20": "QUEUE",
		"21": "DATABASE_DDL_TRIGGER",
		"22": "BROKER_PRIORITY",
		"23": "FULL_TEXT_CATALOG",
		"24": "FULL_TEXT_STOPLIST",
		"25": "SEARCH_PROPERTY_LIST",
		"26": "SEQUENCE",
	}

	if name, ok := metadataClasses[classID]; ok {
		return name
	}
	return "UNKNOWN"
}

// Enrichment helper functions

func enrichKeyLock(dbID, hobtID, hashValue string, mc *MetadataCache) string {
	// Parse IDs
	dbIDInt := mustAtoi(dbID)
	hobtIDInt64 := mustAtoi64(hobtID)

	// Look up database name
	dbName, ok := mc.GetDatabaseName(dbIDInt)
	if !ok {
		dbName = fmt.Sprintf("DB_%s", dbID)
	}

	// Look up HOBT metadata (table/index info)
	hobtMeta, ok := mc.GetHOBTMetadata(hobtIDInt64)
	if ok {
		return fmt.Sprintf("[%s].[%s].[%s] (%s) | Key Hash: %s",
			dbName, hobtMeta.SchemaName, hobtMeta.ObjectName, hobtMeta.IndexName, hashValue)
	}

	// Fallback to IDs if metadata not found
	return fmt.Sprintf("[%s] | HOBT: %s | Key Hash: %s", dbName, hobtID, hashValue)
}

func enrichPageLock(dbID, fileID, pageID string, mc *MetadataCache) string {
	dbIDInt := mustAtoi(dbID)
	fileIDInt := mustAtoi(fileID)

	dbName, ok := mc.GetDatabaseName(dbIDInt)
	if !ok {
		dbName = fmt.Sprintf("DB_%s", dbID)
	}

	fileName, ok := mc.GetFileName(dbIDInt, fileIDInt)
	if ok {
		return fmt.Sprintf("[%s].[%s] | Page: %s", dbName, fileName, pageID)
	}

	return fmt.Sprintf("[%s] | File ID: %s | Page: %s", dbName, fileID, pageID)
}

func enrichRIDLock(dbID, fileID, pageID, rowSlot string, mc *MetadataCache) string {
	dbIDInt := mustAtoi(dbID)
	fileIDInt := mustAtoi(fileID)

	dbName, ok := mc.GetDatabaseName(dbIDInt)
	if !ok {
		dbName = fmt.Sprintf("DB_%s", dbID)
	}

	fileName, ok := mc.GetFileName(dbIDInt, fileIDInt)
	if ok {
		return fmt.Sprintf("[%s].[%s] | Page: %s | Row: %s", dbName, fileName, pageID, rowSlot)
	}

	return fmt.Sprintf("[%s] | File ID: %s | Page: %s | Row: %s", dbName, fileID, pageID, rowSlot)
}

func enrichObjectLock(dbID, objectID, partition string, mc *MetadataCache) string {
	dbIDInt := mustAtoi(dbID)
	objectIDInt := mustAtoi(objectID)

	dbName, ok := mc.GetDatabaseName(dbIDInt)
	if !ok {
		dbName = fmt.Sprintf("DB_%s", dbID)
	}

	objectMeta, ok := mc.GetObjectMetadata(objectIDInt)
	if ok {
		if partition == "0" {
			return fmt.Sprintf("[%s].[%s].[%s]", dbName, objectMeta.SchemaName, objectMeta.ObjectName)
		}
		return fmt.Sprintf("[%s].[%s].[%s] | Partition: %s", dbName, objectMeta.SchemaName, objectMeta.ObjectName, partition)
	}

	return fmt.Sprintf("[%s] | Object ID: %s | Partition: %s", dbName, objectID, partition)
}

func enrichDatabaseLock(dbID string, mc *MetadataCache) string {
	dbIDInt := mustAtoi(dbID)

	dbName, ok := mc.GetDatabaseName(dbIDInt)
	if ok {
		return fmt.Sprintf("[%s]", dbName)
	}

	return fmt.Sprintf("Database ID: %s", dbID)
}

func enrichFileLock(dbID, fileID string, mc *MetadataCache) string {
	dbIDInt := mustAtoi(dbID)
	fileIDInt := mustAtoi(fileID)

	dbName, ok := mc.GetDatabaseName(dbIDInt)
	if !ok {
		dbName = fmt.Sprintf("DB_%s", dbID)
	}

	fileName, ok := mc.GetFileName(dbIDInt, fileIDInt)
	if ok {
		return fmt.Sprintf("[%s].[%s]", dbName, fileName)
	}

	return fmt.Sprintf("[%s] | File ID: %s", dbName, fileID)
}

func enrichExtentLock(dbID, fileID, pageID string, mc *MetadataCache) string {
	dbIDInt := mustAtoi(dbID)
	fileIDInt := mustAtoi(fileID)

	dbName, ok := mc.GetDatabaseName(dbIDInt)
	if !ok {
		dbName = fmt.Sprintf("DB_%s", dbID)
	}

	fileName, ok := mc.GetFileName(dbIDInt, fileIDInt)
	if ok {
		return fmt.Sprintf("[%s].[%s] | First Page: %s", dbName, fileName, pageID)
	}

	return fmt.Sprintf("[%s] | File ID: %s | First Page: %s", dbName, fileID, pageID)
}

func enrichHOBTLock(hobtID string, mc *MetadataCache) string {
	hobtIDInt64 := mustAtoi64(hobtID)

	hobtMeta, ok := mc.GetHOBTMetadata(hobtIDInt64)
	if ok {
		return fmt.Sprintf("[%s].[%s].[%s] (%s %s)",
			hobtMeta.DatabaseName, hobtMeta.SchemaName, hobtMeta.ObjectName,
			hobtMeta.IndexType, hobtMeta.IndexName)
	}

	return fmt.Sprintf("HOBT ID: %s", hobtID)
}

func enrichAllocationUnitLock(auID string, mc *MetadataCache) string {
	auIDInt64 := mustAtoi64(auID)

	desc, ok := mc.GetAllocationUnitDescription(auIDInt64)
	if ok {
		return fmt.Sprintf("Allocation Unit: %s", desc)
	}

	return fmt.Sprintf("Allocation Unit ID: %s", auID)
}

// Helper functions for string to int conversion (with error suppression)

func mustAtoi(s string) int {
	val := 0
	fmt.Sscanf(s, "%d", &val)
	return val
}

func mustAtoi64(s string) int64 {
	var val int64
	fmt.Sscanf(s, "%d", &val)
	return val
}
