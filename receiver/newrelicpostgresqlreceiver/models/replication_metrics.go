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

// PgReplicationSlotMetric represents a row from pg_replication_slots
// This view contains information about replication slots on the database server
// Replication slots provide an automated way to ensure that the primary does not
// delete WAL segments until they have been received by all standbys
//
// Available in PostgreSQL 9.4+
type PgReplicationSlotMetric struct {
	// SlotName is the unique identifier of the replication slot
	// Set when creating the slot
	SlotName sql.NullString

	// SlotType is the type of the replication slot
	// Possible values: physical, logical
	// Physical slots are used for streaming replication
	// Logical slots are used for logical replication/decoding
	SlotType sql.NullString

	// Plugin is the name of the output plugin used by logical replication slot
	// NULL for physical slots
	// Examples: pgoutput, test_decoding, wal2json
	Plugin sql.NullString

	// Active indicates whether this slot is currently being used
	// true if a receiver process is connected and streaming
	Active sql.NullBool

	// XminAge is the age of the oldest transaction that this slot needs to keep
	// Measured in number of transactions
	// Affects VACUUM behavior - transactions older than this cannot be removed
	XminAge sql.NullInt64

	// CatalogXminAge is the age of the oldest transaction affecting system catalogs
	// that this slot needs to keep
	// Measured in number of transactions
	// Used by logical replication to preserve catalog changes
	CatalogXminAge sql.NullInt64

	// RestartDelayBytes is the number of bytes of WAL between current position
	// and the slot's restart_lsn
	// Indicates how far behind the slot is
	// In bytes
	RestartDelayBytes sql.NullInt64

	// ConfirmedFlushDelayBytes is the number of bytes of WAL between current position
	// and the slot's confirmed_flush_lsn
	// Only applicable for logical replication slots
	// NULL or 0 for physical slots
	// In bytes
	ConfirmedFlushDelayBytes sql.NullInt64
}
