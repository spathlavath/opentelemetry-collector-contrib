// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// DatabaseInfoMetric represents database version and platform information
type DatabaseInfoMetric struct {
	InstID       sql.NullString
	VersionFull  sql.NullString
	HostName     sql.NullString
	DatabaseName sql.NullString
	PlatformName sql.NullString
}
