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

// PgStatReplicationSlotMetric represents a row from pg_stat_replication_slots
// This view provides statistics about logical decoding on replication slots
// Only contains data for logical replication slots (physical slots don't appear here)
//
// Available in PostgreSQL 14+
type PgStatReplicationSlotMetric struct {
	// SlotName is the unique identifier of the replication slot
	SlotName sql.NullString

	// SlotType is the type of the replication slot (physical or logical)
	// From pg_replication_slots via JOIN
	SlotType sql.NullString

	// State indicates whether the slot is currently active or inactive
	// 'active' if a receiver process is connected, 'inactive' otherwise
	State sql.NullString

	// SpillTxns is the number of transactions spilled to disk
	// Transactions are spilled when they exceed logical_decoding_work_mem
	SpillTxns sql.NullInt64

	// SpillCount is the number of times transactions were spilled to disk
	// A single transaction can be spilled multiple times
	SpillCount sql.NullInt64

	// SpillBytes is the total amount of data spilled to disk
	// In bytes
	SpillBytes sql.NullInt64

	// StreamTxns is the number of in-progress transactions streamed to the decoding output plugin
	// Streaming allows large transactions to be decoded incrementally
	StreamTxns sql.NullInt64

	// StreamCount is the number of times in-progress transactions were streamed
	// A single transaction can be streamed multiple times as it progresses
	StreamCount sql.NullInt64

	// StreamBytes is the total amount of data streamed for in-progress transactions
	// In bytes
	StreamBytes sql.NullInt64

	// TotalTxns is the total number of decoded transactions sent to the decoding output plugin
	// Includes both committed and aborted transactions
	TotalTxns sql.NullInt64

	// TotalBytes is the total amount of data decoded for transactions sent to the plugin
	// In bytes
	TotalBytes sql.NullInt64
}

// PgReplicationDelayMetric represents replication lag metrics on a standby server
// These metrics measure how far behind the standby is from the primary
// Only available on standby servers (pg_is_in_recovery() returns true)
//
// Available in PostgreSQL 9.6+
type PgReplicationDelayMetric struct {
	// ReplicationDelay is the time lag between primary and standby
	// Calculated as: now() - pg_last_xact_replay_timestamp()
	// Measures how old the last replayed transaction is
	// In seconds
	// Returns 0 on primary servers
	ReplicationDelay sql.NullFloat64

	// ReplicationDelayBytes is the byte lag between WAL received and replayed
	// Calculated as: pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
	// Measures how much WAL data is waiting to be replayed
	// In bytes
	// Returns 0 on primary servers
	ReplicationDelayBytes sql.NullInt64
}

// PgStatWalReceiverMetric represents a row from pg_stat_wal_receiver
// This view shows the state of the WAL receiver process on a standby server
// WAL receiver is responsible for receiving WAL data from the primary server
// Only contains data on standby servers (returns no rows on primary servers)
//
// Available in PostgreSQL 9.6+
type PgStatWalReceiverMetric struct {
	// Status is the activity status of the WAL receiver process
	// Possible values: starting, streaming, waiting, restarting, stopped, disconnected
	// 'streaming' indicates normal operation
	// 'disconnected' is used when WAL receiver is not running (returned as default)
	Status sql.NullString

	// ReceivedTli is the timeline number of the last WAL file received and synced to disk
	// Timeline increases when recovery from a backup or after failover
	// Useful for tracking timeline changes during recovery
	ReceivedTli sql.NullInt64

	// LastMsgSendAge is the time elapsed since last message sent from primary
	// Calculated as: clock_timestamp() - last_msg_send_time
	// In seconds
	// Indicates how recently the primary sent data
	LastMsgSendAge sql.NullFloat64

	// LastMsgReceiptAge is the time elapsed since last message received from primary
	// Calculated as: clock_timestamp() - last_msg_receipt_time
	// In seconds
	// Indicates how recently the standby received data
	// Large values may indicate network issues or primary being down
	LastMsgReceiptAge sql.NullFloat64

	// LatestEndAge is the time elapsed since last WAL location reported back to primary
	// Calculated as: clock_timestamp() - latest_end_time
	// In seconds
	// Indicates how recently the standby reported its progress to the primary
	LatestEndAge sql.NullFloat64
}
