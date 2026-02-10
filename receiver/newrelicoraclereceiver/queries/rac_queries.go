// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package queries // import "github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/queries"

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

	// RACActiveServicesSQL returns active services across all RAC instances
	RACActiveServicesSQL = `
		SELECT
			INST_ID,
			NAME AS SERVICE_NAME,
			NETWORK_NAME,
			GOAL,
			CLB_GOAL,
			BLOCKED,
			AQ_HA_NOTIFICATION,
			COMMIT_OUTCOME,
			DRAIN_TIMEOUT,
			REPLAY_INITIATION_TIMEOUT
		FROM GV$ACTIVE_SERVICES
		WHERE ROWNUM <= 500
		ORDER BY NAME, INST_ID`
)

// ASM (Automatic Storage Management) Queries
const (
	// ASMDetectionSQL checks if ASM instance is available by querying the view directly
	ASMDetectionSQL = `
		SELECT COUNT(*) AS ASM_COUNT
		FROM GV$ASM_DISKGROUP`

	// ASMDiskGroupSQL returns ASM disk group information
	ASMDiskGroupSQL = `
		SELECT
			NAME,
			TOTAL_MB,
			FREE_MB,
			OFFLINE_DISKS
		FROM GV$ASM_DISKGROUP
		WHERE ROWNUM <= 100`
)

// Cluster Performance and Wait Events
const (
	// ClusterWaitEventsSQL returns cluster-specific wait events including Global Cache waits
	ClusterWaitEventsSQL = `
		SELECT
			INST_ID,
			EVENT,
			TOTAL_WAITS,
			TIME_WAITED_MICRO
		FROM GV$SYSTEM_EVENT
		WHERE (WAIT_CLASS = 'Cluster'
		   OR EVENT LIKE 'gc%request'
		   OR EVENT LIKE 'gc%busy'
		   OR EVENT LIKE 'gc%retry'
		   OR EVENT LIKE 'gc%lost')
		   AND TOTAL_WAITS > 0
		   AND ROWNUM <= 500`
)
