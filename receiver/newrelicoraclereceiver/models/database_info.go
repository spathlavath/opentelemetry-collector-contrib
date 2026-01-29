// Copyright 2025 New Relic Corporation. All rights reserved.
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

// DatabaseRole represents the database role and configuration
type DatabaseRole struct {
	DatabaseRole    sql.NullString
	OpenMode        sql.NullString
	ProtectionMode  sql.NullString
	ProtectionLevel sql.NullString
}
