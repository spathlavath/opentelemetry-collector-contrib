// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// Oracle SQL query for session count metric
const (
	SessionCountSQL = "SELECT COUNT(*) as SESSION_COUNT FROM v$session WHERE type = 'USER'"
)

// Oracle SQL query for PDB system metrics
const (
	// PDBSysMetricsSQL retrieves all system metrics from gv$con_sysmetric
	// This matches the approach used in nri-oracledb for simplicity
	PDBSysMetricsSQL = "SELECT INST_ID, METRIC_NAME, VALUE FROM gv$con_sysmetric"
)
