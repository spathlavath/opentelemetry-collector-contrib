// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// Oracle SQL queries for metrics collection
const (
	// SessionCountSQL retrieves the count of user sessions
	SessionCountSQL = "SELECT COUNT(*) as SESSION_COUNT FROM v$session WHERE type = 'USER'"

	// PDBSysMetricsSQL retrieves all system metrics from gv$con_sysmetric
	// This matches the approach used in nri-oracledb for PDB container metrics
	PDBSysMetricsSQL = "SELECT INST_ID, METRIC_NAME, VALUE FROM gv$con_sysmetric"
)
