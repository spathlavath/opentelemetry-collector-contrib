// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package queries // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"

// QPM (Query Performance Monitoring) configuration constants
const (
	// Default values used when configuration is not specified
	DefaultQueryMonitoringResponseTimeThreshold = 500 // milliseconds
	DefaultQueryMonitoringCountThreshold        = 30
	// DefaultActiveQueryCountThreshold is the default row limit for the wait-events/blocking query.
	// It is intentionally set to the maximum allowed value (50) to capture as many active sessions
	// as possible, with slow-query sessions prioritized so they are never cut off.
	DefaultActiveQueryCountThreshold = MaxQueryMonitoringCountThreshold // 50

	// Validation ranges for configuration values
	MinQueryMonitoringResponseTimeThreshold = 0
	MinQueryMonitoringCountThreshold        = 20
	MaxQueryMonitoringCountThreshold        = 50
)
