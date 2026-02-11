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
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/models"
)

// mockPostgreSQLClient implements PostgreSQLClient interface for testing
type mockPostgreSQLClient struct {
	slowQueries []models.PgSlowQueryMetric
	queryErr    error
}

// newMockPostgreSQLClient creates a new mock client
func newMockPostgreSQLClient() *mockPostgreSQLClient {
	return &mockPostgreSQLClient{}
}

// QuerySlowQueries returns mock slow queries
func (m *mockPostgreSQLClient) QuerySlowQueries(ctx context.Context, version, sqlRowLimit int) ([]models.PgSlowQueryMetric, error) {
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	return m.slowQueries, nil
}

// Stub implementations for unused interface methods
func (m *mockPostgreSQLClient) Close() error                                { return nil }
func (m *mockPostgreSQLClient) Ping(ctx context.Context) error              { return nil }
func (m *mockPostgreSQLClient) GetVersion(ctx context.Context) (int, error) { return 130000, nil }
func (m *mockPostgreSQLClient) QueryDatabaseMetrics(ctx context.Context, supportsPG12 bool) ([]models.PgStatDatabaseMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QuerySessionMetrics(ctx context.Context) ([]models.PgStatDatabaseSessionMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryConflictMetrics(ctx context.Context) ([]models.PgStatDatabaseConflictsMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryActivityMetrics(ctx context.Context) ([]models.PgStatActivity, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryWaitEvents(ctx context.Context) ([]models.PgStatActivityWaitEvents, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryPgStatStatementsDealloc(ctx context.Context) (*models.PgStatStatementsDealloc, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QuerySnapshot(ctx context.Context) (*models.PgSnapshot, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryBuffercache(ctx context.Context) ([]models.PgBuffercache, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryServerUptime(ctx context.Context) (*models.PgUptimeMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryDatabaseCount(ctx context.Context) (*models.PgDatabaseCountMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryRunningStatus(ctx context.Context) (*models.PgRunningStatusMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryConnectionStats(ctx context.Context) (*models.PgConnectionStatsMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryReplicationMetrics(ctx context.Context, version int) ([]models.PgStatReplicationMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryReplicationSlots(ctx context.Context, version int) ([]models.PgReplicationSlotMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryReplicationSlotStats(ctx context.Context) ([]models.PgStatReplicationSlotMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryReplicationDelay(ctx context.Context, version int) (*models.PgReplicationDelayMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryWalReceiverMetrics(ctx context.Context) (*models.PgStatWalReceiverMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryWalStatistics(ctx context.Context, version int) (*models.PgStatWalMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryWalFiles(ctx context.Context) (*models.PgWalFilesMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QuerySubscriptionStats(ctx context.Context) ([]models.PgStatSubscriptionMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryBgwriterMetrics(ctx context.Context, version int) (*models.PgStatBgwriterMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryControlCheckpoint(ctx context.Context) (*models.PgControlCheckpointMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryArchiverStats(ctx context.Context) (*models.PgStatArchiverMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QuerySLRUStats(ctx context.Context) ([]models.PgStatSLRUMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryRecoveryPrefetch(ctx context.Context) (*models.PgStatRecoveryPrefetchMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryUserTables(ctx context.Context, schemas, tables []string) ([]models.PgStatUserTablesMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryIOUserTables(ctx context.Context, schemas, tables []string) ([]models.PgStatIOUserTables, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryUserIndexes(ctx context.Context, schemas, tables []string) ([]models.PgStatUserIndexes, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryToastTables(ctx context.Context, schemas, tables []string) ([]models.PgStatToastTables, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryTableSizes(ctx context.Context, schemas, tables []string) ([]models.PgClassSizes, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryRelationStats(ctx context.Context, schemas, tables []string) ([]models.PgClassStats, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryAnalyzeProgress(ctx context.Context) ([]models.PgStatProgressAnalyze, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryClusterProgress(ctx context.Context) ([]models.PgStatProgressCluster, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryCreateIndexProgress(ctx context.Context) ([]models.PgStatProgressCreateIndex, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryVacuumProgress(ctx context.Context) ([]models.PgStatProgressVacuum, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryPgBouncerStats(ctx context.Context) ([]models.PgBouncerStatsMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryPgBouncerPools(ctx context.Context) ([]models.PgBouncerPoolsMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryLocks(ctx context.Context) (*models.PgLocksMetric, error) {
	return nil, nil
}

func (m *mockPostgreSQLClient) QueryIndexSize(ctx context.Context, indexOID int64) (int64, error) {
	return 0, nil
}

var _ client.PostgreSQLClient = (*mockPostgreSQLClient)(nil)

func TestNewSlowQueriesScraper(t *testing.T) {
	mockClient := newMockPostgreSQLClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	instanceName := "test-instance"
	pgVersion := 130000
	sqlRowLimit := 5000
	responseTimeThreshold := 1000
	countThreshold := 100
	enableIntervalCalculator := true
	cacheTTLMinutes := 10

	scraper := NewSlowQueriesScraper(
		mockClient,
		mb,
		logger,
		instanceName,
		config,
		pgVersion,
		sqlRowLimit,
		responseTimeThreshold,
		countThreshold,
		enableIntervalCalculator,
		cacheTTLMinutes,
	)

	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, instanceName, scraper.instanceName)
	assert.Equal(t, config, scraper.mbConfig)
	assert.Equal(t, pgVersion, scraper.pgVersion)
	assert.Equal(t, sqlRowLimit, scraper.sqlRowLimit)
	assert.Equal(t, responseTimeThreshold, scraper.responseTimeThreshold)
	assert.Equal(t, countThreshold, scraper.countThreshold)
	assert.True(t, scraper.enableIntervalCalculator)
	assert.NotNil(t, scraper.intervalCalculator)
}

func TestSlowQueriesScraper_ScrapeWithValidData(t *testing.T) {
	mockClient := newMockPostgreSQLClient()
	now := time.Now()
	mockClient.slowQueries = []models.PgSlowQueryMetric{
		{
			CollectionTimestamp: now,
			QueryID:             "test_query_1",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 150, Valid: true},
			QueryText:           sql.NullString{String: "SELECT * FROM users WHERE id = $1", Valid: true},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 1500.75, Valid: true},
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 225112.5, Valid: true},
			AvgCPUTimeMs:        sql.NullFloat64{Float64: 125.5, Valid: true},
			AvgDiskReads:        sql.NullFloat64{Float64: 50.2, Valid: true},
			TotalDiskReads:      sql.NullInt64{Int64: 7530, Valid: true},
		},
		{
			CollectionTimestamp: now,
			QueryID:             "test_query_2",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser2", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 200, Valid: true},
			QueryText:           sql.NullString{String: "SELECT * FROM orders", Valid: true},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 2000.0, Valid: true},
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 400000.0, Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.PostgresqlSlowQueriesExecutionCount.Enabled = true
	config.Metrics.PostgresqlSlowQueriesAvgElapsedTimeMs.Enabled = true
	config.Metrics.PostgresqlSlowQueriesAvgCPUTimeMs.Enabled = true
	config.Metrics.PostgresqlSlowQueriesAvgDiskReads.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewSlowQueriesScraper(
		mockClient,
		mb,
		zap.NewNop(),
		"test-instance",
		config,
		130000,
		5000,
		1000,
		100,
		false, // Disable interval calculator for simple test
		10,
	)

	errs := scraper.ScrapeSlowQueries(context.Background())

	assert.Empty(t, errs)
}

func TestSlowQueriesScraper_ScrapeWithEmptyResults(t *testing.T) {
	mockClient := newMockPostgreSQLClient()
	mockClient.slowQueries = []models.PgSlowQueryMetric{}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(
		mockClient,
		mb,
		zap.NewNop(),
		"test-instance",
		metadata.DefaultMetricsBuilderConfig(),
		130000,
		5000,
		1000,
		100,
		false,
		10,
	)

	errs := scraper.ScrapeSlowQueries(context.Background())

	assert.Empty(t, errs)
}

func TestSlowQueriesScraper_ScrapeWithQueryError(t *testing.T) {
	mockClient := newMockPostgreSQLClient()
	mockClient.queryErr = errors.New("database connection failed")

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(
		mockClient,
		mb,
		zap.NewNop(),
		"test-instance",
		metadata.DefaultMetricsBuilderConfig(),
		130000,
		5000,
		1000,
		100,
		false,
		10,
	)

	errs := scraper.ScrapeSlowQueries(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "database connection failed")
}

func TestSlowQueriesScraper_ScrapeWithInvalidData(t *testing.T) {
	mockClient := newMockPostgreSQLClient()
	now := time.Now()
	mockClient.slowQueries = []models.PgSlowQueryMetric{
		{
			// Missing required fields - should be skipped
			CollectionTimestamp: now,
			QueryID:             "", // Invalid: empty query ID
			DatabaseName:        sql.NullString{Valid: false},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 1500.0, Valid: true},
		},
		{
			// Valid query
			CollectionTimestamp: now,
			QueryID:             "valid_query",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "user", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 100, Valid: true},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 2000.0, Valid: true},
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 200000.0, Valid: true},
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(
		mockClient,
		mb,
		zap.NewNop(),
		"test-instance",
		metadata.DefaultMetricsBuilderConfig(),
		130000,
		5000,
		1000,
		100,
		false,
		10,
	)

	errs := scraper.ScrapeSlowQueries(context.Background())

	// Should complete without errors, invalid query should be skipped
	assert.Empty(t, errs)
}

func TestSlowQueriesScraper_RecordMetrics(t *testing.T) {
	mockClient := newMockPostgreSQLClient()
	now := time.Now()
	mockClient.slowQueries = []models.PgSlowQueryMetric{
		{
			CollectionTimestamp: now,
			QueryID:             "test_query",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 100, Valid: true},
			QueryText:           sql.NullString{String: "SELECT * FROM test", Valid: true},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 1000.0, Valid: true},
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 100000.0, Valid: true},
			AvgCPUTimeMs:        sql.NullFloat64{Float64: 50.5, Valid: true},
			AvgDiskReads:        sql.NullFloat64{Float64: 20.2, Valid: true},
			TotalDiskReads:      sql.NullInt64{Int64: 2020, Valid: true},
			AvgDiskWrites:       sql.NullFloat64{Float64: 5.3, Valid: true},
			TotalDiskWrites:     sql.NullInt64{Int64: 530, Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.PostgresqlSlowQueriesExecutionCount.Enabled = true
	config.Metrics.PostgresqlSlowQueriesAvgElapsedTimeMs.Enabled = true
	config.Metrics.PostgresqlSlowQueriesAvgCPUTimeMs.Enabled = true
	config.Metrics.PostgresqlSlowQueriesAvgDiskReads.Enabled = true
	config.Metrics.PostgresqlSlowQueriesAvgDiskWrites.Enabled = true
	config.Metrics.PostgresqlSlowQueriesQueryDetails.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewSlowQueriesScraper(
		mockClient,
		mb,
		zap.NewNop(),
		"test-instance",
		config,
		130000,
		5000,
		1000,
		100,
		false,
		10,
	)

	errs := scraper.ScrapeSlowQueries(context.Background())

	assert.Empty(t, errs)

	metrics := mb.Emit()
	require.Greater(t, metrics.ResourceMetrics().Len(), 0)
}

func TestSlowQueriesScraper_WithIntervalCalculator(t *testing.T) {
	mockClient := newMockPostgreSQLClient()
	now := time.Now()

	// First scrape data
	mockClient.slowQueries = []models.PgSlowQueryMetric{
		{
			CollectionTimestamp: now,
			QueryID:             "query_1",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 100, Valid: true},
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 100000.0, Valid: true},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 1000.0, Valid: true},
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(
		mockClient,
		mb,
		zap.NewNop(),
		"test-instance",
		metadata.DefaultMetricsBuilderConfig(),
		130000,
		5000,
		1000,
		100,
		true, // Enable interval calculator
		10,
	)

	// First scrape - establishes baseline
	errs := scraper.ScrapeSlowQueries(context.Background())
	assert.Empty(t, errs)

	// Second scrape - with increased values
	time.Sleep(100 * time.Millisecond) // Small delay to simulate time passing
	mockClient.slowQueries = []models.PgSlowQueryMetric{
		{
			CollectionTimestamp: now.Add(1 * time.Second),
			QueryID:             "query_1",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 150, Valid: true},          // +50 executions
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 175000.0, Valid: true}, // +75000ms
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 1166.67, Valid: true},
		},
	}

	errs = scraper.ScrapeSlowQueries(context.Background())
	assert.Empty(t, errs)

	// Verify interval calculator populated delta metrics
	assert.NotNil(t, scraper.intervalCalculator)
}

func TestSlowQueriesScraper_NoNewExecutions(t *testing.T) {
	// Test scenario: Query with no new executions should be skipped
	mockClient := newMockPostgreSQLClient()
	now := time.Now()

	// First scrape with initial data
	mockClient.slowQueries = []models.PgSlowQueryMetric{
		{
			CollectionTimestamp: now,
			QueryID:             "query_1",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 100, Valid: true},
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 100000.0, Valid: true},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 1000.0, Valid: true},
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(
		mockClient,
		mb,
		zap.NewNop(),
		"test_instance",
		metadata.DefaultMetricsBuilderConfig(),
		130000,
		5000,
		1000,
		100,
		true, // Enable interval calculator
		10,
	)

	errs := scraper.ScrapeSlowQueries(context.Background())
	assert.Empty(t, errs)

	// Second scrape with SAME execution count (no new executions)
	time.Sleep(100 * time.Millisecond)
	mockClient.slowQueries = []models.PgSlowQueryMetric{
		{
			CollectionTimestamp: now.Add(1 * time.Second),
			QueryID:             "query_1",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 100, Valid: true},          // Same count
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 100000.0, Valid: true}, // Same time
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 1000.0, Valid: true},
		},
	}

	errs = scraper.ScrapeSlowQueries(context.Background())
	assert.Empty(t, errs)
	// Query should be skipped due to no new executions
}

func TestSlowQueriesScraper_InvalidMetricsSkipped(t *testing.T) {
	// Test scenario: Queries that fail IsValidForMetrics() should be skipped with warning
	mockClient := newMockPostgreSQLClient()
	now := time.Now()

	mockClient.slowQueries = []models.PgSlowQueryMetric{
		{
			CollectionTimestamp: now,
			QueryID:             "", // Invalid: empty query ID
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 100, Valid: true},
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 100000.0, Valid: true},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 1000.0, Valid: true},
		},
		{
			CollectionTimestamp: now,
			QueryID:             "query_2",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser", Valid: true},
			ExecutionCount:      sql.NullInt64{Valid: false},   // Invalid: no execution count
			TotalElapsedTimeMs:  sql.NullFloat64{Valid: false}, // Invalid: no elapsed time
			AvgElapsedTimeMs:    sql.NullFloat64{Valid: false},
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(
		mockClient,
		mb,
		zap.NewNop(),
		"test_instance",
		metadata.DefaultMetricsBuilderConfig(),
		130000,
		5000,
		1000,
		100,
		false, // Disable interval calculator to test raw validation
		10,
	)

	errs := scraper.ScrapeSlowQueries(context.Background())
	assert.Empty(t, errs)
	// Both queries should be skipped due to invalid data
}

func TestSlowQueriesScraper_SortingWithMixedIntervalMetrics(t *testing.T) {
	// Test scenario: Sorting queries when some have nil interval metrics
	mockClient := newMockPostgreSQLClient()
	now := time.Now()

	mockClient.slowQueries = []models.PgSlowQueryMetric{
		{
			CollectionTimestamp: now,
			QueryID:             "query_1",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 100, Valid: true},
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 200000.0, Valid: true},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 2000.0, Valid: true},
		},
		{
			CollectionTimestamp: now,
			QueryID:             "query_2",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 50, Valid: true},
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 150000.0, Valid: true},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 3000.0, Valid: true},
		},
		{
			CollectionTimestamp: now,
			QueryID:             "query_3",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 75, Valid: true},
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 112500.0, Valid: true},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 1500.0, Valid: true},
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(
		mockClient,
		mb,
		zap.NewNop(),
		"test_instance",
		metadata.DefaultMetricsBuilderConfig(),
		130000,
		5000,
		1000,
		2,    // Low count threshold to test TOP N selection with sorting
		true, // Enable interval calculator
		10,
	)

	// First scrape establishes baseline
	errs := scraper.ScrapeSlowQueries(context.Background())
	assert.Empty(t, errs)

	// Second scrape with updated values to trigger sorting
	time.Sleep(100 * time.Millisecond)
	mockClient.slowQueries = []models.PgSlowQueryMetric{
		{
			CollectionTimestamp: now.Add(1 * time.Second),
			QueryID:             "query_1",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 150, Valid: true},          // +50 executions
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 300000.0, Valid: true}, // +100000ms (slower)
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 2000.0, Valid: true},
		},
		{
			CollectionTimestamp: now.Add(1 * time.Second),
			QueryID:             "query_2",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 100, Valid: true},          // +50 executions
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 300000.0, Valid: true}, // +150000ms (fastest delta)
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 3000.0, Valid: true},
		},
		{
			CollectionTimestamp: now.Add(1 * time.Second),
			QueryID:             "query_3",
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			UserName:            sql.NullString{String: "testuser", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 125, Valid: true},          // +50 executions
			TotalElapsedTimeMs:  sql.NullFloat64{Float64: 237500.0, Valid: true}, // +125000ms (medium)
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 1900.0, Valid: true},
		},
	}

	errs = scraper.ScrapeSlowQueries(context.Background())
	assert.Empty(t, errs)
	// Should sort by interval metrics and take top 2
}
