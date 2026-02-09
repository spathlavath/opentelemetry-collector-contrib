// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// QPM (Query Performance Monitoring) configuration constants for PostgreSQL slow query monitoring
const (
	// Default values used when configuration is not specified
	DefaultQueryMonitoringResponseTimeThreshold = 1000 // milliseconds
	DefaultQueryMonitoringCountThreshold        = 100

	// Validation ranges for configuration values
	MinQueryMonitoringResponseTimeThreshold = 1    // Minimum response time threshold (1ms)
	MaxQueryMonitoringResponseTimeThreshold = 5000 // Maximum response time threshold (5000ms)
	MinQueryMonitoringCountThreshold        = 10   // Minimum count threshold
	MaxQueryMonitoringCountThreshold        = 500  // Maximum count threshold
)
