// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package models // import "github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/models"

import "database/sql"

// SessionStatus represents session count by status
type SessionStatus struct {
	Status sql.NullString
	Count  sql.NullInt64
}

// SessionType represents session count by type
type SessionType struct {
	Type  sql.NullString
	Count sql.NullInt64
}

// LogonStat represents logon statistics
type LogonStat struct {
	Name  sql.NullString
	Value sql.NullFloat64
}

// ConnectionPoolMetric represents connection pool metrics
type ConnectionPoolMetric struct {
	MetricName sql.NullString
	Value      sql.NullInt64
}

// SessionLimit represents session and resource limits
type SessionLimit struct {
	ResourceName       sql.NullString
	CurrentUtilization sql.NullInt64
	MaxUtilization     sql.NullInt64
	InitialAllocation  sql.NullString
	LimitValue         sql.NullString
}

// ConnectionQualityMetric represents connection quality metrics
type ConnectionQualityMetric struct {
	Name  sql.NullString
	Value sql.NullFloat64
}
