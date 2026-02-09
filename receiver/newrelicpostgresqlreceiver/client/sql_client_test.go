// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupMockDB(t *testing.T) (*SQLClient, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)

	client := &SQLClient{
		db: db,
	}

	return client, mock
}

func TestSQLClient_Close(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectClose()

	err := client.Close()
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_Close_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectClose().WillReturnError(errors.New("close error"))

	err := client.Close()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "close error")
}

func TestSQLClient_Ping(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectPing()

	err := client.Ping(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_Ping_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectPing().WillReturnError(errors.New("connection failed"))

	err := client.Ping(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection failed")
}

func TestSQLClient_GetVersion(t *testing.T) {
	tests := []struct {
		name            string
		mockVersion     int
		expectedVersion int
		expectError     bool
	}{
		{
			name:            "PostgreSQL 16",
			mockVersion:     160001,
			expectedVersion: 160001,
			expectError:     false,
		},
		{
			name:            "PostgreSQL 14",
			mockVersion:     140005,
			expectedVersion: 140005,
			expectError:     false,
		},
		{
			name:            "PostgreSQL 9.6",
			mockVersion:     90624,
			expectedVersion: 90624,
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := setupMockDB(t)

			rows := sqlmock.NewRows([]string{"version"}).AddRow(tt.mockVersion)
			mock.ExpectQuery("SELECT current_setting\\('server_version_num'\\)::int").WillReturnRows(rows)

			version, err := client.GetVersion(context.Background())

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedVersion, version)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSQLClient_GetVersion_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT current_setting\\('server_version_num'\\)::int").
		WillReturnError(errors.New("query failed"))

	version, err := client.GetVersion(context.Background())
	assert.Error(t, err)
	assert.Equal(t, 0, version)
	assert.Contains(t, err.Error(), "failed to get PostgreSQL version")
}

func TestSQLClient_QueryDatabaseMetrics(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"datname", "numbackends", "xact_commit", "xact_rollback",
		"blks_read", "blks_hit", "tup_returned", "tup_fetched",
		"tup_inserted", "tup_updated", "tup_deleted", "conflicts",
		"temp_files", "temp_bytes", "deadlocks", "blk_read_time",
		"blk_write_time", "before_xid_wraparound", "database_size",
		"checksum_failures", "checksum_last_failure", "checksums_enabled",
	}).AddRow(
		"testdb", 5, int64(1000), int64(10),
		int64(500), int64(9500), int64(50000), int64(25000),
		int64(100), int64(50), int64(10), int64(0),
		int64(5), int64(1024000), int64(0), 123.45,
		67.89, int64(1000000), int64(1073741824),
		int64(0), nil, true,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QueryDatabaseMetrics(context.Background(), true)

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "testdb", metrics[0].DatName)
	assert.Equal(t, int64(5), metrics[0].NumBackends.Int64)
	assert.Equal(t, int64(1000), metrics[0].XactCommit.Int64)
	assert.Equal(t, int64(10), metrics[0].XactRollback.Int64)
	assert.Equal(t, int64(500), metrics[0].BlksRead.Int64)
	assert.Equal(t, int64(9500), metrics[0].BlksHit.Int64)
	assert.Equal(t, int64(0), metrics[0].ChecksumFailures.Int64)
	assert.True(t, metrics[0].ChecksumsEnabled.Bool)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryDatabaseMetrics_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query failed"))

	metrics, err := client.QueryDatabaseMetrics(context.Background(), true)

	assert.Error(t, err)
	assert.Nil(t, metrics)
	assert.Contains(t, err.Error(), "failed to query pg_stat_database")
}

func TestSQLClient_QuerySessionMetrics(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"datname", "session_time", "active_time", "idle_in_transaction_time",
		"sessions", "sessions_abandoned", "sessions_fatal", "sessions_killed",
	}).AddRow(
		"testdb", 456.78, 123.45, 78.90, int64(50), int64(1), int64(0), int64(2),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QuerySessionMetrics(context.Background())

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "testdb", metrics[0].DatName)
	assert.Equal(t, 456.78, metrics[0].SessionTime.Float64)
	assert.Equal(t, 123.45, metrics[0].ActiveTime.Float64)
	assert.Equal(t, 78.90, metrics[0].IdleInTransactionTime.Float64)
	assert.Equal(t, int64(50), metrics[0].SessionCount.Int64)
	assert.Equal(t, int64(1), metrics[0].SessionsAbandoned.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryConflictMetrics(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"datname", "confl_tablespace", "confl_lock", "confl_snapshot",
		"confl_bufferpin", "confl_deadlock",
	}).AddRow(
		"testdb", int64(1), int64(2), int64(3), int64(4), int64(5),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QueryConflictMetrics(context.Background())

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "testdb", metrics[0].DatName)
	assert.Equal(t, int64(1), metrics[0].ConflTablespace.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryActivityMetrics(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"datname", "usename", "application_name", "backend_type",
		"active_connections", "active_waiting_queries", "xact_start_age",
		"backend_xid_age", "backend_xmin_age", "max_transaction_duration",
		"sum_transaction_duration",
	}).AddRow(
		"testdb", "testuser", "psql", "client backend",
		int64(5), int64(2), 123.45,
		int64(100), int64(200), 45.67, 89.12,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QueryActivityMetrics(context.Background())

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "testdb", metrics[0].DatName.String)
	assert.Equal(t, int64(5), metrics[0].ActiveConnections.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryWaitEvents(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"datname", "usename", "application_name", "backend_type",
		"wait_event", "wait_event_count",
	}).AddRow(
		"testdb", "testuser", "psql", "client backend",
		"Lock", int64(3),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QueryWaitEvents(context.Background())

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "Lock", metrics[0].WaitEvent.String)
	assert.Equal(t, int64(3), metrics[0].WaitEventCount.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryPgStatStatementsDealloc(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{"dealloc"}).AddRow(int64(42))
	mock.ExpectQuery("SELECT dealloc FROM pg_stat_statements_info").WillReturnRows(rows)

	metric, err := client.QueryPgStatStatementsDealloc(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, int64(42), metric.Dealloc.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QuerySnapshot(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{"xmin", "xmax", "xip_count"}).
		AddRow(int64(1000), int64(1050), int64(5))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QuerySnapshot(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, int64(1000), metric.Xmin.Int64)
	assert.Equal(t, int64(1050), metric.Xmax.Int64)
	assert.Equal(t, int64(5), metric.XipCount.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryBuffercache(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"database", "schema", "table", "used_buffers", "unused_buffers",
		"usage_count", "dirty_buffers", "pinning_backends",
	}).AddRow(
		"testdb", "public", "users", int64(100), int64(50),
		int64(500), int64(10), int64(2),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QueryBuffercache(context.Background())

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "testdb", metrics[0].Database.String)
	assert.Equal(t, "public", metrics[0].Schema.String)
	assert.Equal(t, int64(100), metrics[0].UsedBuffers.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryServerUptime(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{"uptime_seconds"}).AddRow(86400.5)
	mock.ExpectQuery("SELECT EXTRACT").WillReturnRows(rows)

	metric, err := client.QueryServerUptime(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, 86400.5, metric.Uptime.Float64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryDatabaseCount(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{"database_count"}).AddRow(int64(5))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(rows)

	metric, err := client.QueryDatabaseCount(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, int64(5), metric.DatabaseCount.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryRunningStatus(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{"running"}).AddRow(int64(1))
	mock.ExpectQuery("SELECT 1").WillReturnRows(rows)

	metric, err := client.QueryRunningStatus(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, int64(1), metric.Running.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryConnectionStats(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{"max_connections", "current_connections"}).
		AddRow(int64(100), int64(25))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QueryConnectionStats(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, int64(100), metric.MaxConnections.Int64)
	assert.Equal(t, int64(25), metric.CurrentConnections.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryReplicationMetrics(t *testing.T) {
	tests := []struct {
		name    string
		version int
	}{
		{"PostgreSQL 9.6", 9},
		{"PostgreSQL 10+", 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := setupMockDB(t)

			var rows *sqlmock.Rows
			if tt.version >= 10 {
				rows = sqlmock.NewRows([]string{
					"application_name", "state", "sync_state", "client_addr",
					"backend_xmin_age", "sent_lsn_delay", "write_lsn_delay",
					"flush_lsn_delay", "replay_lsn_delay", "write_lag", "flush_lag", "replay_lag",
				}).AddRow(
					"walreceiver", "streaming", "async", "192.168.1.10",
					int64(100), int64(1024), int64(512),
					int64(256), int64(128), 1.5, 2.0, 2.5,
				)
			} else {
				rows = sqlmock.NewRows([]string{
					"application_name", "state", "sync_state", "client_addr",
					"backend_xmin_age", "sent_lsn_delay", "write_lsn_delay",
					"flush_lsn_delay", "replay_lsn_delay", "write_lag", "flush_lag", "replay_lag",
				}).AddRow(
					"walreceiver", "streaming", "async", "192.168.1.10",
					int64(100), int64(1024), int64(512),
					int64(256), int64(128), nil, nil, nil,
				)
			}

			mock.ExpectQuery("SELECT").WillReturnRows(rows)

			metrics, err := client.QueryReplicationMetrics(context.Background(), tt.version)

			assert.NoError(t, err)
			assert.Len(t, metrics, 1)
			assert.Equal(t, "walreceiver", metrics[0].ApplicationName.String)
			assert.Equal(t, "streaming", metrics[0].State.String)
			assert.Equal(t, "async", metrics[0].SyncState.String)
			assert.Equal(t, int64(100), metrics[0].BackendXminAge.Int64)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSQLClient_QueryReplicationSlots(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"slot_name", "slot_type", "plugin", "active",
		"xmin_age", "catalog_xmin_age", "restart_delay_bytes", "confirmed_flush_delay_bytes",
	}).AddRow(
		"slot1", "physical", "", true,
		int64(150), int64(200), int64(2048), int64(0),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QueryReplicationSlots(context.Background(), 10)

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "slot1", metrics[0].SlotName.String)
	assert.Equal(t, "physical", metrics[0].SlotType.String)
	assert.True(t, metrics[0].Active.Bool)
	assert.Equal(t, int64(150), metrics[0].XminAge.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryReplicationSlotStats(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"slot_name", "slot_type", "state", "spill_txns", "spill_count", "spill_bytes",
		"stream_txns", "stream_count", "stream_bytes",
		"total_txns", "total_bytes",
	}).AddRow(
		"slot1", "logical", "active", int64(10), int64(100), int64(10240),
		int64(20), int64(200), int64(20480),
		int64(30), int64(30720),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QueryReplicationSlotStats(context.Background())

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "slot1", metrics[0].SlotName.String)
	assert.Equal(t, "logical", metrics[0].SlotType.String)
	assert.Equal(t, "active", metrics[0].State.String)
	assert.Equal(t, int64(10), metrics[0].SpillTxns.Int64)
	assert.Equal(t, int64(100), metrics[0].SpillCount.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryReplicationDelay(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{"replication_delay", "replication_delay_bytes"}).
		AddRow(5.5, int64(5120))

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QueryReplicationDelay(context.Background(), 10)

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, 5.5, metric.ReplicationDelay.Float64)
	assert.Equal(t, int64(5120), metric.ReplicationDelayBytes.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryWalReceiverMetrics(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"status", "received_tli", "last_msg_send_age",
		"last_msg_receipt_age", "latest_end_age",
	}).AddRow(
		"streaming", int64(1), 10.5,
		11.0, 11.5,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QueryWalReceiverMetrics(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, "streaming", metric.Status.String)
	assert.Equal(t, int64(1), metric.ReceivedTli.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryWalStatistics(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full",
		"wal_write", "wal_sync", "wal_write_time", "wal_sync_time",
	}).AddRow(
		int64(1000), int64(50), int64(1048576), int64(5),
		int64(100), int64(80), 123.45, 67.89,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QueryWalStatistics(context.Background(), 14)

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, int64(1000), metric.WalRecords.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryWalFiles(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"wal_file_count", "wal_file_size", "wal_file_age_seconds",
	}).AddRow(int64(10), int64(167772160), 300.5)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QueryWalFiles(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, int64(10), metric.WalCount.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QuerySubscriptionStats(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"subscription_name", "last_msg_send_age", "last_msg_receipt_age",
		"latest_end_age", "apply_error_count", "sync_error_count", "state",
	}).AddRow(
		"sub1", 20.5, 21.0,
		21.5, int64(0), int64(0), "active",
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QuerySubscriptionStats(context.Background())

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "sub1", metrics[0].SubscriptionName.String)
	assert.Equal(t, "active", metrics[0].State.String)
	assert.Equal(t, int64(0), metrics[0].ApplyErrorCount.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryBgwriterMetrics(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"buffers_clean", "maxwritten_clean", "buffers_alloc",
		"checkpoints_timed", "checkpoints_requested", "buffers_checkpoint",
		"checkpoint_write_time", "checkpoint_sync_time",
		"buffers_backend", "buffers_backend_fsync",
	}).AddRow(
		int64(1000), int64(50), int64(5000),
		int64(100), int64(10), int64(2000),
		123.45, 67.89,
		int64(500), int64(5),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QueryBgwriterMetrics(context.Background(), 16)

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, int64(1000), metric.BuffersClean.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryControlCheckpoint(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"timeline_id", "checkpoint_delay", "checkpoint_delay_bytes", "redo_delay_bytes",
	}).AddRow(int64(1), 300.5, int64(16777216), int64(8388608))

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QueryControlCheckpoint(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, int64(1), metric.TimelineID.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryArchiverStats(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{"archived_count", "failed_count"}).
		AddRow(int64(1000), int64(5))

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QueryArchiverStats(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, int64(1000), metric.ArchivedCount.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QuerySLRUStats(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"slru_name", "blks_zeroed", "blks_hit", "blks_read",
		"blks_written", "blks_exists", "flushes", "truncates",
	}).AddRow(
		"CommitTs", int64(10), int64(1000), int64(50),
		int64(100), int64(5), int64(20), int64(2),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QuerySLRUStats(context.Background())

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "CommitTs", metrics[0].SLRUName)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryRecoveryPrefetch(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"prefetch", "hit", "skip_init", "skip_new", "skip_fpw",
		"skip_rep", "wal_distance", "block_distance", "io_depth",
	}).AddRow(
		int64(1000), int64(800), int64(10), int64(20), int64(30),
		int64(40), int64(1048576), int64(128), int64(16),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QueryRecoveryPrefetch(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, int64(1000), metric.Prefetch.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryUserTables(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "table_name", "seq_scan", "seq_tup_read",
		"idx_scan", "idx_tup_fetch", "n_tup_ins", "n_tup_upd", "n_tup_del",
		"n_tup_hot_upd", "n_live_tup", "n_dead_tup", "n_mod_since_analyze",
		"last_vacuum_age", "last_autovacuum_age", "last_analyze_age",
		"last_autoanalyze_age", "vacuum_count", "autovacuum_count",
		"analyze_count", "autoanalyze_count",
	}).AddRow(
		"testdb", "public", "users", int64(100), int64(10000),
		int64(500), int64(5000), int64(100), int64(50), int64(10),
		int64(30), int64(1000), int64(50), int64(20),
		3600.5, 7200.0, 1800.0, 3600.0,
		int64(10), int64(50), int64(5), int64(25),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	schemas := []string{"public"}
	tables := []string{"users"}
	metrics, err := client.QueryUserTables(context.Background(), schemas, tables)

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "users", metrics[0].TableName)
	assert.Equal(t, int64(100), metrics[0].SeqScan.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryIOUserTables(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "table_name", "heap_blks_read", "heap_blks_hit",
		"idx_blks_read", "idx_blks_hit", "toast_blks_read", "toast_blks_hit",
		"tidx_blks_read", "tidx_blks_hit",
	}).AddRow(
		"testdb", "public", "users", int64(100), int64(900),
		int64(50), int64(450), int64(10), int64(90),
		int64(5), int64(45),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	schemas := []string{"public"}
	tables := []string{"users"}
	metrics, err := client.QueryIOUserTables(context.Background(), schemas, tables)

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "users", metrics[0].TableName)
	assert.Equal(t, int64(100), metrics[0].HeapBlksRead.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryUserIndexes(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "table_name", "index_name",
		"indexrelid", "idx_scan", "idx_tup_read", "idx_tup_fetch",
	}).AddRow(
		"testdb", "public", "users", "users_pkey",
		int64(12345), int64(1000), int64(10000), int64(9500),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	schemas := []string{"public"}
	tables := []string{"users"}
	metrics, err := client.QueryUserIndexes(context.Background(), schemas, tables)

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "users_pkey", metrics[0].IndexName)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryToastTables(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "table_name", "toast_vacuumed",
		"toast_autovacuumed", "toast_last_vacuum_age", "toast_last_autovacuum_age",
	}).AddRow(
		"testdb", "public", "large_table", int64(5),
		int64(25), 7200.0, 3600.0,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	schemas := []string{"public"}
	tables := []string{"large_table"}
	metrics, err := client.QueryToastTables(context.Background(), schemas, tables)

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "large_table", metrics[0].TableName)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryTableSizes(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"database", "schema_name", "table_name", "relation_size",
		"toast_size", "total_size", "indexes_size",
	}).AddRow(
		"testdb", "public", "users", int64(1048576),
		int64(0), int64(2097152), int64(1048576),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	schemas := []string{"public"}
	tables := []string{"users"}
	metrics, err := client.QueryTableSizes(context.Background(), schemas, tables)

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "users", metrics[0].TableName)
	assert.Equal(t, int64(1048576), metrics[0].RelationSize.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryRelationStats(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"database", "schema_name", "table_name", "pages",
		"tuples", "all_visible", "xmin",
	}).AddRow(
		"testdb", "public", "users", int64(128),
		1000.5, int64(120), int64(100000),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	schemas := []string{"public"}
	tables := []string{"users"}
	metrics, err := client.QueryRelationStats(context.Background(), schemas, tables)

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "users", metrics[0].TableName)
	assert.Equal(t, int64(128), metrics[0].RelPages.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryLocks(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"database", "access_share_lock", "row_share_lock", "row_exclusive_lock", "share_update_exclusive_lock",
		"share_lock", "share_row_exclusive_lock", "exclusive_lock", "access_exclusive_lock",
	}).AddRow(
		"testdb", int64(10), int64(5), int64(20), int64(2),
		int64(1), int64(3), int64(4), int64(1),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QueryLocks(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, int64(10), metric.AccessShareLock.Int64)
	assert.Equal(t, int64(5), metric.RowShareLock.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryIndexSize(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{"pg_relation_size"}).AddRow(int64(524288))
	mock.ExpectQuery("SELECT pg_relation_size").WillReturnRows(rows)

	size, err := client.QueryIndexSize(context.Background(), 12345)

	assert.NoError(t, err)
	assert.Equal(t, int64(524288), size)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryIndexSize_NULL(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{"pg_relation_size"}).AddRow(nil)
	mock.ExpectQuery("SELECT pg_relation_size").WillReturnRows(rows)

	size, err := client.QueryIndexSize(context.Background(), 12345)

	assert.NoError(t, err)
	assert.Equal(t, int64(0), size)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryIndexSize_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT pg_relation_size").WillReturnError(errors.New("query failed"))

	size, err := client.QueryIndexSize(context.Background(), 12345)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to query index size")
	assert.Equal(t, int64(0), size)
}

func TestSQLClient_QueryPgBouncerStats(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"database", "total_server_assignment_count", "total_xact_count", "total_query_count",
		"total_received", "total_sent", "total_xact_time", "total_query_time", "total_wait_time",
		"total_client_parse_count", "total_server_parse_count", "total_bind_count",
		"avg_server_assignment_count", "avg_xact_count", "avg_query_count",
		"avg_recv", "avg_sent", "avg_xact_time", "avg_query_time", "avg_wait_time",
		"avg_client_parse_count", "avg_server_parse_count", "avg_bind_count",
	}).AddRow(
		"pgbouncer", int64(100), int64(1000), int64(5000),
		int64(1048576), int64(2097152), int64(10000), int64(5000), int64(500),
		int64(50), int64(40), int64(30),
		int64(10), int64(100), int64(500),
		int64(10485), int64(20971), int64(100), int64(50), int64(5),
		int64(5), int64(4), int64(3),
	)

	mock.ExpectQuery("SHOW STATS").WillReturnRows(rows)

	metrics, err := client.QueryPgBouncerStats(context.Background())

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "pgbouncer", metrics[0].Database)
	assert.Equal(t, int64(1000), metrics[0].TotalXactCount.Int64)
	assert.Equal(t, int64(5000), metrics[0].TotalQueryCount.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLClient_QueryPgBouncerPools(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"database", "user", "cl_active", "cl_waiting", "cl_active_cancel_req", "cl_waiting_cancel_req",
		"sv_active", "sv_active_cancel", "sv_being_cancel", "sv_idle", "sv_used", "sv_tested", "sv_login",
		"maxwait", "maxwait_us", "pool_mode", "min_pool_size",
	}).AddRow(
		"testdb", "testuser", int64(5), int64(2), int64(0), int64(0),
		int64(3), int64(0), int64(0), int64(2), int64(5), int64(0), int64(0),
		int64(0), int64(0), "session", int64(1),
	)

	mock.ExpectQuery("SHOW POOLS").WillReturnRows(rows)

	metrics, err := client.QueryPgBouncerPools(context.Background())

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "testdb", metrics[0].Database)
	assert.Equal(t, "testuser", metrics[0].User)
	assert.Equal(t, int64(5), metrics[0].ClActive.Int64)
	assert.Equal(t, int64(3), metrics[0].SvActive.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test NULL value handling
func TestSQLClient_NULLHandling(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"datname", "numbackends", "xact_commit", "xact_rollback",
		"blks_read", "blks_hit", "tup_returned", "tup_fetched",
		"tup_inserted", "tup_updated", "tup_deleted", "conflicts",
		"temp_files", "temp_bytes", "deadlocks", "blk_read_time",
		"blk_write_time", "before_xid_wraparound", "database_size",
		"checksum_failures", "checksum_last_failure", "checksums_enabled",
	}).AddRow(
		"testdb", nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil,
		nil, nil, nil,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QueryDatabaseMetrics(context.Background(), true)

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.False(t, metrics[0].NumBackends.Valid)
	assert.False(t, metrics[0].XactCommit.Valid)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test empty result set
func TestSQLClient_EmptyResultSet(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"datname", "numbackends", "xact_commit", "xact_rollback",
		"blks_read", "blks_hit", "tup_returned", "tup_fetched",
		"tup_inserted", "tup_updated", "tup_deleted", "conflicts",
		"temp_files", "temp_bytes", "deadlocks", "blk_read_time",
		"blk_write_time", "checksum_failures", "checksum_last_failure",
		"active_time", "sessions", "sessions_abandoned", "sessions_fatal", "sessions_killed",
	})

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QueryDatabaseMetrics(context.Background(), true)

	assert.NoError(t, err)
	assert.Empty(t, metrics)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test QueryDatabaseMetrics without PG12 support (< PG12)
func TestSQLClient_QueryDatabaseMetrics_PrePG12(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"datname", "numbackends", "xact_commit", "xact_rollback",
		"blks_read", "blks_hit", "tup_returned", "tup_fetched",
		"tup_inserted", "tup_updated", "tup_deleted", "conflicts",
		"temp_files", "temp_bytes", "deadlocks", "blk_read_time",
		"blk_write_time", "before_xid_wraparound", "database_size",
	}).AddRow(
		"testdb", 5, int64(1000), int64(10),
		int64(500), int64(9500), int64(50000), int64(25000),
		int64(100), int64(50), int64(10), int64(0),
		int64(5), int64(1024000), int64(0), 123.45,
		67.89, int64(1000000), int64(1073741824),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QueryDatabaseMetrics(context.Background(), false)

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "testdb", metrics[0].DatName)
	assert.Equal(t, int64(5), metrics[0].NumBackends.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test QueryBgwriterMetrics for PostgreSQL 17+
func TestSQLClient_QueryBgwriterMetrics_PG17(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"buffers_clean", "maxwritten_clean", "buffers_alloc",
		"checkpoints_timed", "checkpoints_requested", "buffers_checkpoint",
		"checkpoint_write_time", "checkpoint_sync_time",
		"buffers_backend", "buffers_backend_fsync",
	}).AddRow(
		int64(1000), int64(50), int64(5000),
		int64(100), int64(20), int64(3000),
		123.45, 67.89,
		int64(200), int64(5),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QueryBgwriterMetrics(context.Background(), 170000) // PG 17

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, int64(1000), metric.BuffersClean.Int64)
	assert.Equal(t, int64(100), metric.CheckpointsTimed.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test QueryWalStatistics for PostgreSQL 18+
func TestSQLClient_QueryWalStatistics_PG18(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full",
	}).AddRow(
		int64(1000), int64(50), int64(1048576), int64(5),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QueryWalStatistics(context.Background(), 180000) // PG 18

	assert.NoError(t, err)
	assert.NotNil(t, metric)
	assert.Equal(t, int64(1000), metric.WalRecords.Int64)
	assert.Equal(t, int64(50), metric.WalFpi.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test QueryWalReceiverMetrics returns nil when no rows (primary server)
func TestSQLClient_QueryWalReceiverMetrics_NoRows(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"status", "received_tli", "last_msg_send_age",
		"last_msg_receipt_age", "latest_end_age",
	})

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QueryWalReceiverMetrics(context.Background())

	assert.NoError(t, err)
	assert.Nil(t, metric)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test QueryRecoveryPrefetch returns nil when no rows
func TestSQLClient_QueryRecoveryPrefetch_NoRows(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"prefetch", "hit", "skip_init", "skip_new", "skip_fpw",
		"skip_rep", "wal_distance", "block_distance", "io_depth",
	})

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metric, err := client.QueryRecoveryPrefetch(context.Background())

	assert.NoError(t, err)
	assert.Nil(t, metric)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test SetPgBouncerDB
func TestSQLClient_SetPgBouncerDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	pgBouncerDB, pgBouncerMock, err := sqlmock.New()
	require.NoError(t, err)
	defer pgBouncerDB.Close()

	client := &SQLClient{
		db: db,
	}

	client.SetPgBouncerDB(pgBouncerDB)

	assert.NotNil(t, client.pgBouncerDB)
	assert.Equal(t, pgBouncerDB, client.pgBouncerDB)

	// Verify mock expectations (even if empty)
	assert.NoError(t, mock.ExpectationsWereMet())
	assert.NoError(t, pgBouncerMock.ExpectationsWereMet())
}

// Test QuerySessionMetrics error scenarios
func TestSQLClient_QuerySessionMetrics_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query failed"))

	metrics, err := client.QuerySessionMetrics(context.Background())

	assert.Error(t, err)
	assert.Nil(t, metrics)
	assert.Contains(t, err.Error(), "failed to query pg_stat_database sessions")
}

// Test QueryConflictMetrics error scenarios
func TestSQLClient_QueryConflictMetrics_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query failed"))

	metrics, err := client.QueryConflictMetrics(context.Background())

	assert.Error(t, err)
	assert.Nil(t, metrics)
	assert.Contains(t, err.Error(), "failed to query pg_stat_database_conflicts")
}

// Test QueryActivityMetrics error scenarios
func TestSQLClient_QueryActivityMetrics_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query failed"))

	metrics, err := client.QueryActivityMetrics(context.Background())

	assert.Error(t, err)
	assert.Nil(t, metrics)
	assert.Contains(t, err.Error(), "failed to query pg_stat_activity")
}

// Test QueryWaitEvents error scenarios
func TestSQLClient_QueryWaitEvents_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query failed"))

	metrics, err := client.QueryWaitEvents(context.Background())

	assert.Error(t, err)
	assert.Nil(t, metrics)
	assert.Contains(t, err.Error(), "failed to query pg_stat_activity wait events")
}

// Test QueryBuffercache error scenarios
func TestSQLClient_QueryBuffercache_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query failed"))

	metrics, err := client.QueryBuffercache(context.Background())

	assert.Error(t, err)
	assert.Nil(t, metrics)
	assert.Contains(t, err.Error(), "failed to query pg_buffercache")
}

// Test QueryPgStatStatementsDealloc error scenarios
func TestSQLClient_QueryPgStatStatementsDealloc_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT dealloc FROM pg_stat_statements_info").WillReturnError(errors.New("query failed"))

	metric, err := client.QueryPgStatStatementsDealloc(context.Background())

	assert.Error(t, err)
	assert.Nil(t, metric)
	assert.Contains(t, err.Error(), "failed to query pg_stat_statements_info")
}

// Test QueryReplicationMetrics error for PG9.6
func TestSQLClient_QueryReplicationMetrics_PG96_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query failed"))

	metrics, err := client.QueryReplicationMetrics(context.Background(), 90624)

	assert.Error(t, err)
	assert.Nil(t, metrics)
	assert.Contains(t, err.Error(), "failed to query pg_stat_replication")
}

// Test QueryReplicationSlots error for PG9.6
func TestSQLClient_QueryReplicationSlots_PG96_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query failed"))

	metrics, err := client.QueryReplicationSlots(context.Background(), 90624)

	assert.Error(t, err)
	assert.Nil(t, metrics)
	assert.Contains(t, err.Error(), "failed to query pg_replication_slots")
}

// Test QueryReplicationDelay error for PG9.6
func TestSQLClient_QueryReplicationDelay_PG96_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query failed"))

	metric, err := client.QueryReplicationDelay(context.Background(), 90624)

	assert.Error(t, err)
	assert.Nil(t, metric)
	assert.Contains(t, err.Error(), "failed to query replication delay")
}

// Test QueryAnalyzeProgress
func TestSQLClient_QueryAnalyzeProgress(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"database", "schema_name", "table_name", "phase",
		"sample_blks_total", "sample_blks_scanned",
		"ext_stats_total", "ext_stats_computed",
		"child_tables_total", "child_tables_done",
	}).AddRow(
		"testdb", "public", "users", "scanning",
		int64(1000), int64(500),
		int64(10), int64(5),
		int64(3), int64(1),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QueryAnalyzeProgress(context.Background())

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "testdb", metrics[0].Database)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test QueryClusterProgress
func TestSQLClient_QueryClusterProgress(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"database", "schema_name", "table_name", "command", "phase",
		"heap_blks_total", "heap_blks_scanned", "heap_tuples_scanned", "heap_tuples_written",
		"index_rebuild_count",
	}).AddRow(
		"testdb", "public", "orders", "CLUSTER", "seq scanning heap",
		int64(1000), int64(800), int64(5000), int64(4500),
		int64(1),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QueryClusterProgress(context.Background())

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "testdb", metrics[0].Database)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test QueryAnalyzeProgress error
func TestSQLClient_QueryAnalyzeProgress_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query failed"))

	metrics, err := client.QueryAnalyzeProgress(context.Background())

	assert.Error(t, err)
	assert.Nil(t, metrics)
	assert.Contains(t, err.Error(), "failed to query pg_stat_progress_analyze")
}

// Test QueryClusterProgress error
func TestSQLClient_QueryClusterProgress_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query failed"))

	metrics, err := client.QueryClusterProgress(context.Background())

	assert.Error(t, err)
	assert.Nil(t, metrics)
	assert.Contains(t, err.Error(), "failed to query pg_stat_progress_cluster")
}

// Test QueryCreateIndexProgress
func TestSQLClient_QueryCreateIndexProgress(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"database", "schema_name", "table_name", "index_name", "command", "phase",
		"lockers_total", "lockers_done", "blocks_total", "blocks_done",
		"tuples_total", "tuples_done", "partitions_total", "partitions_done",
	}).AddRow(
		"testdb", "public", "users", "users_email_idx", "CREATE INDEX", "building index",
		int64(0), int64(0), int64(10000), int64(5000),
		int64(100000), int64(50000), int64(1), int64(0),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QueryCreateIndexProgress(context.Background())

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "testdb", metrics[0].Database)
	assert.Equal(t, "users_email_idx", metrics[0].IndexName.String)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test QueryVacuumProgress
func TestSQLClient_QueryVacuumProgress(t *testing.T) {
	client, mock := setupMockDB(t)

	rows := sqlmock.NewRows([]string{
		"database", "schema_name", "table_name", "phase",
		"heap_blks_total", "heap_blks_scanned", "heap_blks_vacuumed",
		"index_vacuum_count", "max_dead_tuples", "num_dead_tuples",
	}).AddRow(
		"testdb", "public", "orders", "scanning heap",
		int64(5000), int64(2500), int64(2000),
		int64(1), int64(10000), int64(500),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	metrics, err := client.QueryVacuumProgress(context.Background())

	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "testdb", metrics[0].Database)
	assert.Equal(t, "orders", metrics[0].TableName)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test QueryCreateIndexProgress error
func TestSQLClient_QueryCreateIndexProgress_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query failed"))

	metrics, err := client.QueryCreateIndexProgress(context.Background())

	assert.Error(t, err)
	assert.Nil(t, metrics)
	assert.Contains(t, err.Error(), "failed to query pg_stat_progress_create_index")
}

// Test QueryVacuumProgress error
func TestSQLClient_QueryVacuumProgress_Error(t *testing.T) {
	client, mock := setupMockDB(t)

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query failed"))

	metrics, err := client.QueryVacuumProgress(context.Background())

	assert.Error(t, err)
	assert.Nil(t, metrics)
	assert.Contains(t, err.Error(), "failed to query pg_stat_progress_vacuum")
}
