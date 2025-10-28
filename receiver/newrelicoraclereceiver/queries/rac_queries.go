// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// Oracle Real Application Clusters (RAC) related queries

// RAC Detection and Status Queries
const (
	// RACDetectionSQL checks if Oracle is running in RAC mode
	RACDetectionSQL = `
		SELECT 
			VALUE 
		FROM V$PARAMETER 
		WHERE NAME = 'cluster_database'`

	// RACInstanceStatusSQL returns status of all RAC instances
	RACInstanceStatusSQL = `
		SELECT
			INST_ID,
			INSTANCE_NAME,
			HOST_NAME,
			STATUS,
			STARTUP_TIME,
			DATABASE_STATUS,
			ACTIVE_STATE,
			LOGINS,
			ARCHIVER,
			VERSION
		FROM GV$INSTANCE`

	// RACActiveServicesSQL returns active services for failover tracking
	RACActiveServicesSQL = `
		SELECT
			NAME AS SERVICE_NAME,
			INST_ID,
			FAILOVER_METHOD,
			FAILOVER_TYPE,
			GOAL,
			NETWORK_NAME,
			CREATION_DATE,
			FAILOVER_RETRIES,
			FAILOVER_DELAY,
			CLB_GOAL
		FROM GV$ACTIVE_SERVICES`
)

// ASM (Automatic Storage Management) Queries
const (
	// ASMDetectionSQL checks if ASM instance is available
	ASMDetectionSQL = `
		SELECT COUNT(*) AS ASM_COUNT
		FROM ALL_TABLES 
		WHERE TABLE_NAME = 'GV$ASM_DISKGROUP' 
			AND OWNER = 'SYS'`

	// ASMDiskGroupSQL returns ASM disk group information
	ASMDiskGroupSQL = `
		SELECT
			NAME,
			TOTAL_MB,
			FREE_MB,
			OFFLINE_DISKS
		FROM GV$ASM_DISKGROUP`
)

// Cluster Performance and Wait Events
const (
	// ClusterWaitEventsSQL returns cluster-specific wait events
	ClusterWaitEventsSQL = `
		SELECT
			INST_ID,
			EVENT,
			TOTAL_WAITS,
			TIME_WAITED_MICRO
		FROM GV$SYSTEM_EVENT
		WHERE WAIT_CLASS = 'Cluster'`
)
