// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/common"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

type mockInnoDBClient struct {
	stats        map[string]string
	statsErr     error
	getStatsFunc func() (map[string]string, error)
}

func (m *mockInnoDBClient) Connect() error {
	return nil
}

func (m *mockInnoDBClient) GetGlobalStats() (map[string]string, error) {
	if m.getStatsFunc != nil {
		return m.getStatsFunc()
	}
	return m.stats, m.statsErr
}

func (m *mockInnoDBClient) GetGlobalVariables() (map[string]string, error) {
	return nil, nil
}

func (m *mockInnoDBClient) GetReplicationStatus() (map[string]string, error) {
	return nil, nil
}

func (m *mockInnoDBClient) GetVersion() (string, error) {
	return "8.0.0", nil
}

func (m *mockInnoDBClient) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return nil, errors.New("QueryContext not implemented in mock")
}

func (m *mockInnoDBClient) Close() error {
	return nil
}

func TestNewInnoDBExtendedScraper(t *testing.T) {
	tests := []struct {
		name        string
		client      common.Client
		mb          *metadata.MetricsBuilder
		logger      *zap.Logger
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid_inputs",
			client:      &mockInnoDBClient{},
			mb:          metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType)),
			logger:      zap.NewNop(),
			expectError: false,
		},
		{
			name:        "nil_client",
			client:      nil,
			mb:          metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType)),
			logger:      zap.NewNop(),
			expectError: true,
			errorMsg:    "client cannot be nil",
		},
		{
			name:        "nil_metrics_builder",
			client:      &mockInnoDBClient{},
			mb:          nil,
			logger:      zap.NewNop(),
			expectError: true,
			errorMsg:    "metrics builder cannot be nil",
		},
		{
			name:        "nil_logger",
			client:      &mockInnoDBClient{},
			mb:          metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType)),
			logger:      nil,
			expectError: true,
			errorMsg:    "logger cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scraper, err := NewInnoDBExtendedScraper(tt.client, tt.mb, tt.logger)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, scraper)
			} else {
				require.NoError(t, err)
				require.NotNil(t, scraper)
			}
		})
	}
}

func TestInnoDBExtendedScraper_ScrapeMetrics_Success(t *testing.T) {
	mockClient := &mockInnoDBClient{
		stats: map[string]string{
			// Active Transactions
			"Innodb_active_transactions": "10",

			// Adaptive Hash Index
			"Innodb_adaptive_hash_hash_searches":     "1000000",
			"Innodb_adaptive_hash_non_hash_searches": "500000",
			"Innodb_adaptive_hash_pages_added":       "100",
			"Innodb_adaptive_hash_pages_removed":     "50",

			// Undo Logs
			"Innodb_available_undo_logs": "128",

			// Buffer Pool Extended
			"Innodb_buffer_pool_bytes_data":           "134217728",
			"Innodb_buffer_pool_bytes_dirty":          "8388608",
			"Innodb_buffer_pool_dirty":                "512",
			"Innodb_buffer_pool_free":                 "7680",
			"Innodb_buffer_pool_pages_data":           "8192",
			"Innodb_buffer_pool_pages_free":           "0",
			"Innodb_buffer_pool_pages_flushed":        "1000",
			"Innodb_buffer_pool_pages_LRU_flushed":    "100",
			"Innodb_buffer_pool_pages_made_not_young": "50",
			"Innodb_buffer_pool_pages_made_young":     "200",
			"Innodb_buffer_pool_pages_total":          "8192",
			"Innodb_buffer_pool_pages_misc":           "0",
			"Innodb_buffer_pool_pages_old":            "3000",
			"Innodb_buffer_pool_read_ahead":           "64",
			"Innodb_buffer_pool_read_ahead_evicted":   "10",
			"Innodb_buffer_pool_read_ahead_rnd":       "0",
			"Innodb_buffer_pool_read_requests":        "10000000",
			"Innodb_buffer_pool_reads":                "1000",
			"Innodb_buffer_pool_total":                "134217728",
			"Innodb_buffer_pool_used":                 "126828800",
			"Innodb_buffer_pool_utilization":          "95",
			"Innodb_buffer_pool_wait_free":            "0",
			"Innodb_buffer_pool_write_requests":       "5000000",

			// Checkpoint
			"Innodb_checkpoint_age": "1048576",

			// Current Row Locks
			"Innodb_current_row_locks": "5",

			// Current Transactions
			"Innodb_current_transactions": "15",

			// Data Operations
			"Innodb_data_fsyncs":         "5000",
			"Innodb_data_pending_fsyncs": "0",
			"Innodb_data_pending_reads":  "0",
			"Innodb_data_pending_writes": "0",
			"Innodb_data_read":           "536870912",
			"Innodb_data_reads":          "10000",
			"Innodb_data_writes":         "5000",
			"Innodb_data_written":        "268435456",

			// Doublewrite Buffer
			"Innodb_dblwr_pages_written": "2000",
			"Innodb_dblwr_writes":        "500",

			// Hash Index Cells
			"Innodb_hash_index_cells_total": "4194304",
			"Innodb_hash_index_cells_used":  "2097152",

			// History List
			"Innodb_history_list_length": "1000",

			// Change Buffer
			"Innodb_ibuf_free_list":           "100",
			"Innodb_ibuf_merged_delete_marks": "50",
			"Innodb_ibuf_merged_deletes":      "25",
			"Innodb_ibuf_merged_inserts":      "500",
			"Innodb_ibuf_merges":              "100",
			"Innodb_ibuf_segment_size":        "10",
			"Innodb_ibuf_size":                "1",

			// Lock Structs
			"Innodb_lock_structs": "10",

			// Locked Tables
			"Innodb_locked_tables": "2",

			// Locked Transactions
			"Innodb_locked_transactions": "1",

			// Log Operations
			"Innodb_log_waits":          "0",
			"Innodb_log_write_requests": "1000000",
			"Innodb_log_writes":         "500000",

			// LSN
			"Innodb_lsn_current":         "12345678901234",
			"Innodb_lsn_flushed":         "12345678900000",
			"Innodb_lsn_last_checkpoint": "12345678800000",

			// Master Thread
			"Innodb_master_thread_active_loops": "1000",
			"Innodb_master_thread_idle_loops":   "5000",

			// Memory
			"Innodb_mem_adaptive_hash":   "8388608",
			"Innodb_mem_additional_pool": "0",
			"Innodb_mem_dictionary":      "4194304",
			"Innodb_mem_file_system":     "2097152",
			"Innodb_mem_lock_system":     "1048576",
			"Innodb_mem_page_hash":       "524288",
			"Innodb_mem_recovery_system": "0",
			"Innodb_mem_thread_hash":     "262144",
			"Innodb_mem_total":           "150994944",

			// Mutex Operations
			"Innodb_mutex_os_waits":    "1000",
			"Innodb_mutex_spin_rounds": "50000",
			"Innodb_mutex_spin_waits":  "10000",

			// Open Files
			"Innodb_num_open_files": "50",

			// I/O Operations
			"Innodb_os_file_fsyncs": "10000",
			"Innodb_os_file_reads":  "5000",
			"Innodb_os_file_writes": "3000",

			// OS Log Operations
			"Innodb_os_log_fsyncs":         "5000",
			"Innodb_os_log_pending_fsyncs": "0",
			"Innodb_os_log_pending_writes": "0",
			"Innodb_os_log_written":        "52428800",

			// Page Operations
			"Innodb_page_size":     "16384",
			"Innodb_pages_created": "500",
			"Innodb_pages_read":    "10000",
			"Innodb_pages_written": "5000",

			// Pending Operations
			"Innodb_pending_aio_log_ios":         "0",
			"Innodb_pending_aio_sync_ios":        "0",
			"Innodb_pending_buffer_pool_flushes": "0",
			"Innodb_pending_checkpoint_writes":   "0",
			"Innodb_pending_ibuf_aio_reads":      "0",
			"Innodb_pending_log_flushes":         "0",
			"Innodb_pending_log_writes":          "0",
			"Innodb_pending_normal_aio_reads":    "0",
			"Innodb_pending_normal_aio_writes":   "0",

			// Purge Operations
			"Innodb_purge_trx_id":  "123456",
			"Innodb_purge_undo_no": "789012",

			// Queries
			"Innodb_queries_inside": "5",
			"Innodb_queries_queued": "0",

			// Read Views
			"Innodb_read_views": "2",

			// Redo Log
			"Innodb_redo_log_enabled": "1",

			// Row Lock Extended
			"Innodb_row_lock_current_waits": "0",
			"Innodb_row_lock_time":          "5000",
			"Innodb_row_lock_time_avg":      "50",
			"Innodb_row_lock_time_max":      "500",
			"Innodb_row_lock_waits":         "100",

			// Row Operations
			"Innodb_rows_inserted": "50000",
			"Innodb_rows_deleted":  "5000",
			"Innodb_rows_updated":  "10000",
			"Innodb_rows_read":     "1000000",

			// S-Lock
			"Innodb_s_lock_os_waits":    "100",
			"Innodb_s_lock_spin_rounds": "5000",
			"Innodb_s_lock_spin_waits":  "1000",

			// Semaphore Operations
			"Innodb_semaphore_wait_time": "1000",
			"Innodb_semaphore_waits":     "50",

			// System
			"Innodb_tables_in_use":           "10",
			"Innodb_truncated_status_writes": "0",

			// Undo Tablespaces
			"Innodb_undo_tablespaces_active":   "2",
			"Innodb_undo_tablespaces_explicit": "0",
			"Innodb_undo_tablespaces_implicit": "2",
			"Innodb_undo_tablespaces_total":    "2",

			// X-Lock
			"Innodb_x_lock_os_waits":    "200",
			"Innodb_x_lock_spin_rounds": "10000",
			"Innodb_x_lock_spin_waits":  "2000",
		},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewInnoDBExtendedScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	scraper.ScrapeMetrics(ctx, now, errs)

	assert.Nil(t, errs.Combine(), "Expected no errors")
}

func TestInnoDBExtendedScraper_ScrapeMetrics_ClientError(t *testing.T) {
	mockClient := &mockInnoDBClient{
		getStatsFunc: func() (map[string]string, error) {
			return nil, errors.New("database connection error")
		},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewInnoDBExtendedScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	scraper.ScrapeMetrics(ctx, now, errs)

	assert.NotNil(t, errs.Combine(), "Expected partial scrape error")
}

func TestInnoDBExtendedScraper_ScrapeMetrics_MissingMetrics(t *testing.T) {
	mockClient := &mockInnoDBClient{
		stats: map[string]string{
			// Only include a subset of metrics (simulating older MySQL version)
			"Innodb_buffer_pool_pages_data":    "8192",
			"Innodb_buffer_pool_pages_free":    "0",
			"Innodb_buffer_pool_read_requests": "10000000",
			"Innodb_rows_read":                 "1000000",
			"Innodb_rows_inserted":             "50000",
		},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewInnoDBExtendedScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	// Should not error even if some metrics are missing
	scraper.ScrapeMetrics(ctx, now, errs)

	assert.False(t, errs.Combine() != nil, "Expected no errors even with missing metrics")
}
