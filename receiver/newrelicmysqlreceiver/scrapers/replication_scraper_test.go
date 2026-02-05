// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
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

type mockReplicationClient struct {
	replicationStatus     map[string]string
	replicationStatusErr  error
	masterStatus          map[string]string
	masterStatusErr       error
	groupReplicationStats map[string]string
	groupReplicationErr   error
}

func (m *mockReplicationClient) Connect() error {
	return nil
}

func (m *mockReplicationClient) GetGlobalStats() (map[string]string, error) {
	return nil, nil
}

func (m *mockReplicationClient) GetGlobalVariables() (map[string]string, error) {
	return nil, nil
}

func (m *mockReplicationClient) GetReplicationStatus() (map[string]string, error) {
	return m.replicationStatus, m.replicationStatusErr
}

func (m *mockReplicationClient) GetMasterStatus() (map[string]string, error) {
	return m.masterStatus, m.masterStatusErr
}

func (m *mockReplicationClient) GetGroupReplicationStats() (map[string]string, error) {
	return m.groupReplicationStats, m.groupReplicationErr
}

func (m *mockReplicationClient) GetVersion() (string, error) {
	return "8.0.0", nil
}

func (m *mockReplicationClient) Close() error {
	return nil
}

func TestNewReplicationScraper(t *testing.T) {
	tests := []struct {
		name                    string
		client                  common.Client
		mb                      *metadata.MetricsBuilder
		logger                  *zap.Logger
		enableAdditionalMetrics bool
		expectError             bool
		errorMsg                string
	}{
		{
			name:                    "valid_inputs_with_additional_metrics",
			client:                  &mockReplicationClient{},
			mb:                      metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType)),
			logger:                  zap.NewNop(),
			enableAdditionalMetrics: true,
			expectError:             false,
		},
		{
			name:                    "valid_inputs_without_additional_metrics",
			client:                  &mockReplicationClient{},
			mb:                      metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType)),
			logger:                  zap.NewNop(),
			enableAdditionalMetrics: false,
			expectError:             false,
		},
		{
			name:                    "nil_client",
			client:                  nil,
			mb:                      metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType)),
			logger:                  zap.NewNop(),
			enableAdditionalMetrics: true,
			expectError:             true,
			errorMsg:                "client cannot be nil",
		},
		{
			name:                    "nil_metrics_builder",
			client:                  &mockReplicationClient{},
			mb:                      nil,
			logger:                  zap.NewNop(),
			enableAdditionalMetrics: true,
			expectError:             true,
			errorMsg:                "metrics builder cannot be nil",
		},
		{
			name:                    "nil_logger",
			client:                  &mockReplicationClient{},
			mb:                      metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType)),
			logger:                  nil,
			enableAdditionalMetrics: true,
			expectError:             true,
			errorMsg:                "logger cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scraper, err := NewReplicationScraper(tt.client, tt.mb, tt.logger, tt.enableAdditionalMetrics)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, scraper)
			} else {
				require.NoError(t, err)
				require.NotNil(t, scraper)
				assert.Equal(t, tt.enableAdditionalMetrics, scraper.enableAdditionalMetrics)
			}
		})
	}
}

func TestReplicationScraper_ScrapeMetrics_ReplicaNode(t *testing.T) {
	tests := []struct {
		name                    string
		replicationStatus       map[string]string
		enableAdditionalMetrics bool
		expectedMetrics         []string
	}{
		{
			name: "replica_with_core_metrics_only",
			replicationStatus: map[string]string{
				"Seconds_Behind_Master": "10",
				"Read_Master_Log_Pos":   "12345",
				"Exec_Master_Log_Pos":   "12340",
				"Last_IO_Errno":         "0",
				"Last_SQL_Errno":        "0",
				"Relay_Log_Space":       "4096",
				"Slave_IO_Running":      "Yes",
				"Slave_SQL_Running":     "Yes",
			},
			enableAdditionalMetrics: false,
			expectedMetrics: []string{
				"newrelicmysql.replication.seconds_behind_master",
				"newrelicmysql.replication.read_master_log_pos",
				"newrelicmysql.replication.exec_master_log_pos",
				"newrelicmysql.replication.slave_io_running",
				"newrelicmysql.replication.slave_sql_running",
				"newrelicmysql.replication.slave_running",
			},
		},
		{
			name: "replica_with_additional_metrics",
			replicationStatus: map[string]string{
				"Seconds_Behind_Master": "10",
				"Seconds_Behind_Source": "10",
				"Read_Master_Log_Pos":   "12345",
				"Exec_Master_Log_Pos":   "12340",
				"Last_IO_Errno":         "0",
				"Last_SQL_Errno":        "0",
				"Relay_Log_Space":       "4096",
				"Slave_IO_Running":      "Yes",
				"Slave_SQL_Running":     "Yes",
			},
			enableAdditionalMetrics: true,
			expectedMetrics: []string{
				"newrelicmysql.replication.seconds_behind_master",
				"newrelicmysql.replication.seconds_behind_source",
				"newrelicmysql.replication.read_master_log_pos",
				"newrelicmysql.replication.exec_master_log_pos",
				"newrelicmysql.replication.slave_io_running",
				"newrelicmysql.replication.slave_sql_running",
				"newrelicmysql.replication.slave_running",
			},
		},
		{
			name: "replica_with_null_seconds_behind",
			replicationStatus: map[string]string{
				"Seconds_Behind_Master": "NULL",
				"Slave_IO_Running":      "Yes",
				"Slave_SQL_Running":     "No",
			},
			enableAdditionalMetrics: false,
			expectedMetrics: []string{
				"newrelicmysql.replication.slave_io_running",
				"newrelicmysql.replication.slave_sql_running",
				"newrelicmysql.replication.slave_running",
			},
		},
		{
			name: "replica_with_mysql8_names",
			replicationStatus: map[string]string{
				"Seconds_Behind_Source": "5",
				"Read_Source_Log_Pos":   "54321",
				"Exec_Source_Log_Pos":   "54320",
				"Replica_IO_Running":    "Yes",
				"Replica_SQL_Running":   "Yes",
			},
			enableAdditionalMetrics: true,
			expectedMetrics: []string{
				"newrelicmysql.replication.seconds_behind_source",
				"newrelicmysql.replication.read_master_log_pos",
				"newrelicmysql.replication.exec_master_log_pos",
				"newrelicmysql.replication.slave_io_running",
				"newrelicmysql.replication.slave_sql_running",
				"newrelicmysql.replication.slave_running",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockReplicationClient{
				replicationStatus: tt.replicationStatus,
			}

			// Use default metrics config (metrics are enabled by default now)
			mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
			scraper, err := NewReplicationScraper(mockClient, mb, zap.NewNop(), tt.enableAdditionalMetrics)
			require.NoError(t, err)

			errs := &scrapererror.ScrapeErrors{}
			scraper.ScrapeMetrics(context.Background(), pcommon.NewTimestampFromTime(time.Now()), errs)

			assert.Nil(t, errs.Combine(), "Expected no errors")

			metrics := mb.Emit()
			rm := metrics.ResourceMetrics()
			require.Equal(t, 1, rm.Len())

			sm := rm.At(0).ScopeMetrics()
			require.Equal(t, 1, sm.Len())

			metricSlice := sm.At(0).Metrics()
			collectedMetricNames := make(map[string]bool)
			for i := 0; i < metricSlice.Len(); i++ {
				collectedMetricNames[metricSlice.At(i).Name()] = true
			}

			for _, expectedMetric := range tt.expectedMetrics {
				assert.True(t, collectedMetricNames[expectedMetric], "Expected metric %s not found", expectedMetric)
			}
		})
	}
}

func TestReplicationScraper_ScrapeMetrics_MasterNode(t *testing.T) {
	tests := []struct {
		name                    string
		masterStatus            map[string]string
		enableAdditionalMetrics bool
		expectMasterMetrics     bool
	}{
		{
			name: "master_with_additional_metrics_enabled",
			masterStatus: map[string]string{
				"Slaves_Connected":   "3",
				"Replicas_Connected": "3",
			},
			enableAdditionalMetrics: true,
			expectMasterMetrics:     true,
		},
		{
			name: "master_with_additional_metrics_disabled",
			masterStatus: map[string]string{
				"Slaves_Connected":   "3",
				"Replicas_Connected": "3",
			},
			enableAdditionalMetrics: false,
			expectMasterMetrics:     false,
		},
		{
			name:                    "master_no_slaves_connected",
			masterStatus:            map[string]string{},
			enableAdditionalMetrics: true,
			expectMasterMetrics:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockReplicationClient{
				replicationStatus: map[string]string{}, // Empty - not a replica
				masterStatus:      tt.masterStatus,
			}

			// Use default metrics config (metrics are enabled by default now)
			mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
			scraper, err := NewReplicationScraper(mockClient, mb, zap.NewNop(), tt.enableAdditionalMetrics)
			require.NoError(t, err)

			errs := &scrapererror.ScrapeErrors{}
			scraper.ScrapeMetrics(context.Background(), pcommon.NewTimestampFromTime(time.Now()), errs)

			assert.Nil(t, errs.Combine(), "Expected no errors")

			metrics := mb.Emit()
			rm := metrics.ResourceMetrics()

			if tt.expectMasterMetrics {
				require.Equal(t, 1, rm.Len())
			}
		})
	}
}

func TestReplicationScraper_ScrapeMetrics_GroupReplication(t *testing.T) {
	tests := []struct {
		name                    string
		groupReplicationStats   map[string]string
		enableAdditionalMetrics bool
		expectedMetrics         []string
	}{
		{
			name: "group_replication_with_additional_metrics",
			groupReplicationStats: map[string]string{
				"group_replication_transactions_certified":                "1000",
				"group_replication_transactions_conflicts_detected":       "5",
				"group_replication_transactions_in_validation_queue":      "2",
				"group_replication_transactions_in_applier_queue":         "3",
				"group_replication_transactions_committed":                "995",
				"group_replication_transactions_proposed":                 "1000",
				"group_replication_transactions_rollback":                 "5",
				"group_replication_certification_db_transactions_checked": "1000",
			},
			enableAdditionalMetrics: true,
			expectedMetrics: []string{
				"newrelicmysql.replication.group.transactions",
				"newrelicmysql.replication.group.conflicts_detected",
				"newrelicmysql.replication.group.transactions_validating",
				"newrelicmysql.replication.group.transactions_in_applier_queue",
				"newrelicmysql.replication.group.transactions_applied",
				"newrelicmysql.replication.group.transactions_proposed",
				"newrelicmysql.replication.group.transactions_rollback",
				"newrelicmysql.replication.group.transactions_check",
			},
		},
		{
			name: "group_replication_with_additional_metrics_disabled",
			groupReplicationStats: map[string]string{
				"group_replication_transactions_certified": "1000",
			},
			enableAdditionalMetrics: false,
			expectedMetrics:         []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockReplicationClient{
				replicationStatus:     map[string]string{},
				groupReplicationStats: tt.groupReplicationStats,
			}

			// Use default metrics config (metrics are enabled by default now)
			mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
			scraper, err := NewReplicationScraper(mockClient, mb, zap.NewNop(), tt.enableAdditionalMetrics)
			require.NoError(t, err)

			errs := &scrapererror.ScrapeErrors{}
			scraper.ScrapeMetrics(context.Background(), pcommon.NewTimestampFromTime(time.Now()), errs)

			assert.Nil(t, errs.Combine(), "Expected no errors")

			metrics := mb.Emit()

			if len(tt.expectedMetrics) > 0 {
				rm := metrics.ResourceMetrics()
				require.Equal(t, 1, rm.Len())

				sm := rm.At(0).ScopeMetrics()
				require.Equal(t, 1, sm.Len())

				metricSlice := sm.At(0).Metrics()
				collectedMetricNames := make(map[string]bool)
				for i := 0; i < metricSlice.Len(); i++ {
					collectedMetricNames[metricSlice.At(i).Name()] = true
				}

				for _, expectedMetric := range tt.expectedMetrics {
					assert.True(t, collectedMetricNames[expectedMetric], "Expected metric %s not found", expectedMetric)
				}
			}
		})
	}
}

func TestReplicationScraper_ScrapeMetrics_Errors(t *testing.T) {
	tests := []struct {
		name                    string
		replicationStatusErr    error
		masterStatusErr         error
		groupReplicationErr     error
		enableAdditionalMetrics bool
		expectedPartialErrors   int
	}{
		{
			name:                    "replication_status_error",
			replicationStatusErr:    errors.New("failed to fetch replication status"),
			enableAdditionalMetrics: false,
			expectedPartialErrors:   1,
		},
		{
			name:                    "master_status_error_with_additional_metrics",
			masterStatusErr:         errors.New("failed to fetch master status"),
			enableAdditionalMetrics: true,
			expectedPartialErrors:   1,
		},
		{
			name:                    "group_replication_error_with_additional_metrics",
			groupReplicationErr:     errors.New("failed to fetch group replication stats"),
			enableAdditionalMetrics: true,
			expectedPartialErrors:   1,
		},
		{
			name:                    "master_status_error_without_additional_metrics",
			masterStatusErr:         errors.New("failed to fetch master status"),
			enableAdditionalMetrics: false,
			expectedPartialErrors:   0, // Master metrics not collected
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockReplicationClient{
				replicationStatus:     map[string]string{},
				replicationStatusErr:  tt.replicationStatusErr,
				masterStatus:          map[string]string{},
				masterStatusErr:       tt.masterStatusErr,
				groupReplicationStats: map[string]string{},
				groupReplicationErr:   tt.groupReplicationErr,
			}

			mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
			scraper, err := NewReplicationScraper(mockClient, mb, zap.NewNop(), tt.enableAdditionalMetrics)
			require.NoError(t, err)

			errs := &scrapererror.ScrapeErrors{}
			scraper.ScrapeMetrics(context.Background(), pcommon.NewTimestampFromTime(time.Now()), errs)

			if tt.expectedPartialErrors > 0 {
				assert.NotNil(t, errs.Combine(), "Expected partial scrape error")
			} else {
				assert.Nil(t, errs.Combine(), "Expected no errors")
			}
		})
	}
}

func TestReplicationScraper_ScrapeMetrics_StandaloneNode(t *testing.T) {
	mockClient := &mockReplicationClient{
		replicationStatus: map[string]string{}, // Empty - not a replica
		masterStatus: map[string]string{
			"Slaves_Connected": "0",
		},
	}

	// Use default metrics config (metrics are enabled by default now)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	scraper, err := NewReplicationScraper(mockClient, mb, zap.NewNop(), true)
	require.NoError(t, err)

	errs := &scrapererror.ScrapeErrors{}
	scraper.ScrapeMetrics(context.Background(), pcommon.NewTimestampFromTime(time.Now()), errs)

	assert.Nil(t, errs.Combine(), "Expected no errors")

	metrics := mb.Emit()
	rm := metrics.ResourceMetrics()

	// Should have metrics for slaves_connected: 0
	require.Equal(t, 1, rm.Len())
}
