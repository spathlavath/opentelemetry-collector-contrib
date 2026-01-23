// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import (
	"database/sql"
)

// PgStatReplicationMetric represents a row from pg_stat_replication
// This view contains information about WAL sender processes serving
// streaming replication to standby servers
//
// Available in PostgreSQL 9.6+, enhanced in 10+
type PgStatReplicationMetric struct {
	// ApplicationName is the name of the application connected to this WAL sender
	// Can be set via application_name in connection string
	ApplicationName sql.NullString

	// State is the current state of the WAL sender
	// Possible values: startup, catchup, streaming, backup, stopping
	State sql.NullString

	// SyncState is the synchronous state of this standby server
	// Possible values: async, potential, sync, quorum
	SyncState sql.NullString

	// ClientAddr is the IP address of the client connected to this WAL sender
	// NULL for Unix domain socket connections
	ClientAddr sql.NullString

	// BackendXminAge is the age of the oldest transaction on the standby
	// Measured in number of transactions
	// Useful for detecting standbys holding back vacuum
	BackendXminAge sql.NullInt64

	// SentLsnDelay is the number of bytes of WAL sent but not yet written to disk on standby
	// Calculated as: pg_current_wal_lsn() - sent_lsn
	// In bytes
	SentLsnDelay sql.NullInt64

	// WriteLsnDelay is the number of bytes of WAL written but not yet flushed on standby
	// Calculated as: pg_current_wal_lsn() - write_lsn
	// In bytes
	WriteLsnDelay sql.NullInt64

	// FlushLsnDelay is the number of bytes of WAL flushed but not yet applied on standby
	// Calculated as: pg_current_wal_lsn() - flush_lsn
	// In bytes
	FlushLsnDelay sql.NullInt64

	// ReplayLsnDelay is the number of bytes of WAL not yet replayed on standby
	// Calculated as: pg_current_wal_lsn() - replay_lsn
	// This is the total replication lag in bytes
	// In bytes
	ReplayLsnDelay sql.NullInt64

	// WriteLag is the time elapsed between flushing WAL on primary and receiving notification
	// that it has been written to disk on the standby
	// In seconds (converted from interval)
	// PostgreSQL 10+
	WriteLag sql.NullFloat64

	// FlushLag is the time elapsed between flushing WAL on primary and receiving notification
	// that it has been flushed to disk on the standby
	// In seconds (converted from interval)
	// PostgreSQL 10+
	FlushLag sql.NullFloat64

	// ReplayLag is the time elapsed between flushing WAL on primary and receiving notification
	// that it has been replayed on the standby
	// This is the total replication lag in time
	// In seconds (converted from interval)
	// PostgreSQL 10+
	ReplayLag sql.NullFloat64
}
