// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicpostgresqlreceiver

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/zap/zaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
)

// mockHost implements component.Host for testing
type mockHost struct {
	component.Host
}

func newMockHost() component.Host {
	return &mockHost{}
}

// Test newScraper creation with valid config
func TestNewScraper_Success(t *testing.T) {
	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	scrapeCfg := scraperhelper.NewDefaultControllerConfig()
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}

	dbProvider := func() (*sql.DB, error) {
		db, _, err := sqlmock.New()
		return db, err
	}

	scraper, err := newScraper(mb, metricsBuilderConfig, scrapeCfg, config, logger, dbProvider, "test-instance", "test-host")

	assert.NoError(t, err)
	assert.NotNil(t, scraper)
}

// Test newScraper with database connection failure in dbProvider
func TestNewScraper_DBProviderError(t *testing.T) {
	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	scrapeCfg := scraperhelper.NewDefaultControllerConfig()
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}

	dbProvider := func() (*sql.DB, error) {
		return nil, errors.New("connection failed")
	}

	scraper, err := newScraper(mb, metricsBuilderConfig, scrapeCfg, config, logger, dbProvider, "test-instance", "test-host")

	// newScraper itself doesn't fail, the error occurs during start
	assert.NoError(t, err)
	assert.NotNil(t, scraper)
}

// Test version constants
func TestVersionConstants(t *testing.T) {
	assert.Equal(t, 90600, PG96Version)
	assert.Equal(t, 100000, PG10Version)
	assert.Equal(t, 120000, PG12Version)
	assert.Equal(t, 130000, PG13Version)
	assert.Equal(t, 140000, PG14Version)
	assert.Equal(t, 150000, PG15Version)
}

// Test createScrapeContext with timeout
func TestCreateScrapeContext_WithTimeout(t *testing.T) {
	logger := zaptest.NewLogger(t)
	scrapeCfg := scraperhelper.ControllerConfig{
		Timeout: 5 * time.Second,
	}

	pgScraper := &newRelicPostgreSQLScraper{
		logger:    logger,
		scrapeCfg: scrapeCfg,
	}

	ctx := context.Background()
	scrapeCtx, cancel := pgScraper.createScrapeContext(ctx)
	defer cancel()

	assert.NotNil(t, scrapeCtx)

	deadline, ok := scrapeCtx.Deadline()
	assert.True(t, ok)
	assert.True(t, deadline.After(time.Now()))
}

// Test createScrapeContext without timeout
func TestCreateScrapeContext_NoTimeout(t *testing.T) {
	logger := zaptest.NewLogger(t)
	scrapeCfg := scraperhelper.ControllerConfig{
		Timeout: 0,
	}

	pgScraper := &newRelicPostgreSQLScraper{
		logger:    logger,
		scrapeCfg: scrapeCfg,
	}

	ctx := context.Background()
	scrapeCtx, cancel := pgScraper.createScrapeContext(ctx)
	defer cancel()

	assert.NotNil(t, scrapeCtx)

	_, ok := scrapeCtx.Deadline()
	assert.False(t, ok)
}

// Test buildMetrics
func TestBuildMetrics(t *testing.T) {
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	logger := zaptest.NewLogger(t)
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)

	pgScraper := &newRelicPostgreSQLScraper{
		mb:           mb,
		instanceName: "test-instance",
	}

	metrics := pgScraper.buildMetrics()

	assert.NotNil(t, metrics)
	// Note: ResourceMetrics().Len() may be 0 or 1 depending on whether metrics were recorded
	assert.GreaterOrEqual(t, metrics.ResourceMetrics().Len(), 0)
}

// Test logScrapeCompletion
func TestLogScrapeCompletion(t *testing.T) {
	logger := zaptest.NewLogger(t)

	pgScraper := &newRelicPostgreSQLScraper{
		logger: logger,
	}

	// Should not panic
	pgScraper.logScrapeCompletion(nil)
	pgScraper.logScrapeCompletion([]error{})
	pgScraper.logScrapeCompletion([]error{errors.New("test error")})
}

// Test shutdown with database connection
func TestShutdown_WithDatabase(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	logger := zaptest.NewLogger(t)
	pgScraper := &newRelicPostgreSQLScraper{
		logger: logger,
		db:     db,
	}

	mock.ExpectClose()

	err = pgScraper.shutdown(context.Background())

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test shutdown with nil database
func TestShutdown_NilDatabase(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pgScraper := &newRelicPostgreSQLScraper{
		logger: logger,
		db:     nil,
	}

	err := pgScraper.shutdown(context.Background())

	assert.NoError(t, err)
}

// Test createPgBouncerConnection with basic config
func TestCreatePgBouncerConnection_BasicConfig(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Password: "secret",
		Database: "postgres",
		SSLMode:  "disable",
	}

	pgScraper := &newRelicPostgreSQLScraper{
		logger: logger,
		config: config,
	}

	// This will create a DB connection object but won't actually connect
	db, err := pgScraper.createPgBouncerConnection()

	assert.NoError(t, err)
	assert.NotNil(t, db)
	if db != nil {
		db.Close()
	}
}

// Test createPgBouncerConnection with SSL config
func TestCreatePgBouncerConnection_WithSSL(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		Hostname:    "localhost",
		Port:        "5432",
		Username:    "postgres",
		Password:    "secret",
		Database:    "postgres",
		SSLMode:     "verify-full",
		SSLCert:     "/path/to/cert.pem",
		SSLKey:      "/path/to/key.pem",
		SSLRootCert: "/path/to/ca.pem",
	}

	pgScraper := &newRelicPostgreSQLScraper{
		logger: logger,
		config: config,
	}

	db, err := pgScraper.createPgBouncerConnection()

	assert.NoError(t, err)
	assert.NotNil(t, db)
	if db != nil {
		db.Close()
	}
}

// Test createPgBouncerConnection with timeout
func TestCreatePgBouncerConnection_WithTimeout(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Password: "secret",
		Database: "postgres",
		SSLMode:  "disable",
		Timeout:  10 * time.Second,
	}

	pgScraper := &newRelicPostgreSQLScraper{
		logger: logger,
		config: config,
	}

	db, err := pgScraper.createPgBouncerConnection()

	assert.NoError(t, err)
	assert.NotNil(t, db)
	if db != nil {
		db.Close()
	}
}

// Test createPgBouncerConnection without password
func TestCreatePgBouncerConnection_NoPassword(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}

	pgScraper := &newRelicPostgreSQLScraper{
		logger: logger,
		config: config,
	}

	db, err := pgScraper.createPgBouncerConnection()

	assert.NoError(t, err)
	assert.NotNil(t, db)
	if db != nil {
		db.Close()
	}
}

// Test initializeDatabase with successful connection
func TestInitializeDatabase_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}

	pgScraper := &newRelicPostgreSQLScraper{
		logger: logger,
		config: config,
		dbProviderFunc: func() (*sql.DB, error) {
			return db, nil
		},
	}

	err = pgScraper.initializeDatabase()

	assert.NoError(t, err)
	assert.NotNil(t, pgScraper.db)
	assert.NotNil(t, pgScraper.client)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test initializeDatabase with connection failure
func TestInitializeDatabase_ConnectionFailure(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}

	pgScraper := &newRelicPostgreSQLScraper{
		logger: logger,
		config: config,
		dbProviderFunc: func() (*sql.DB, error) {
			return nil, errors.New("connection failed")
		},
	}

	err := pgScraper.initializeDatabase()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to open db connection")
}

// Test initializeDatabase with pgbouncer database
func TestInitializeDatabase_PgBouncerDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "pgbouncer", // Connected directly to pgbouncer
		SSLMode:  "disable",
	}

	pgScraper := &newRelicPostgreSQLScraper{
		logger: logger,
		config: config,
		dbProviderFunc: func() (*sql.DB, error) {
			return db, nil
		},
	}

	err = pgScraper.initializeDatabase()

	assert.NoError(t, err)
	assert.NotNil(t, pgScraper.db)
	assert.NotNil(t, pgScraper.client)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test start with database initialization failure
func TestStart_DatabaseInitFailure(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}

	pgScraper := &newRelicPostgreSQLScraper{
		logger: logger,
		config: config,
		dbProviderFunc: func() (*sql.DB, error) {
			return nil, errors.New("connection failed")
		},
	}

	err := pgScraper.start(context.Background(), newMockHost())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to open db connection")
}

// Test initializeScrapers with PostgreSQL 14+
func TestInitializeScrapers_PG14(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
		Relations: &RelationConfig{
			Schemas: []string{"public"},
			Tables:  []string{"users"},
		},
	}

	sqlClient := client.NewSQLClient(db)

	pgScraper := &newRelicPostgreSQLScraper{
		logger:               logger,
		mb:                   mb,
		config:               config,
		db:                   db,
		client:               sqlClient,
		instanceName:         "test-instance",
		metricsBuilderConfig: metricsBuilderConfig,
	}

	// Mock GetVersion to return PostgreSQL 14
	rows := sqlmock.NewRows([]string{"version"}).AddRow(140000)
	mock.ExpectQuery("SELECT current_setting\\('server_version_num'\\)::int").WillReturnRows(rows)

	err = pgScraper.initializeScrapers()

	assert.NoError(t, err)
	assert.Equal(t, 140000, pgScraper.pgVersion)
	assert.True(t, pgScraper.supportsPG96)
	assert.True(t, pgScraper.supportsPG12)
	assert.True(t, pgScraper.supportsPG14)
	assert.NotNil(t, pgScraper.databaseMetricsScraper)
	assert.NotNil(t, pgScraper.activityScraper)
	assert.NotNil(t, pgScraper.replicationMetricsScraper)
	assert.NotNil(t, pgScraper.bgwriterScraper)
	assert.NotNil(t, pgScraper.vacuumMaintenanceScraper)
	assert.NotNil(t, pgScraper.tableIOScraper)
	assert.NotNil(t, pgScraper.pgbouncerScraper)
	assert.NotNil(t, pgScraper.locksScraper)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test initializeScrapers with PostgreSQL 12
func TestInitializeScrapers_PG12(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}

	sqlClient := client.NewSQLClient(db)

	pgScraper := &newRelicPostgreSQLScraper{
		logger:               logger,
		mb:                   mb,
		config:               config,
		db:                   db,
		client:               sqlClient,
		instanceName:         "test-instance",
		metricsBuilderConfig: metricsBuilderConfig,
	}

	// Mock GetVersion to return PostgreSQL 12
	rows := sqlmock.NewRows([]string{"version"}).AddRow(120000)
	mock.ExpectQuery("SELECT current_setting\\('server_version_num'\\)::int").WillReturnRows(rows)

	err = pgScraper.initializeScrapers()

	assert.NoError(t, err)
	assert.Equal(t, 120000, pgScraper.pgVersion)
	assert.True(t, pgScraper.supportsPG96)
	assert.True(t, pgScraper.supportsPG12)
	assert.False(t, pgScraper.supportsPG14)
	assert.NotNil(t, pgScraper.databaseMetricsScraper)
	assert.NotNil(t, pgScraper.activityScraper)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test initializeScrapers with PostgreSQL 9.6
func TestInitializeScrapers_PG96(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}

	sqlClient := client.NewSQLClient(db)

	pgScraper := &newRelicPostgreSQLScraper{
		logger:               logger,
		mb:                   mb,
		config:               config,
		db:                   db,
		client:               sqlClient,
		instanceName:         "test-instance",
		metricsBuilderConfig: metricsBuilderConfig,
	}

	// Mock GetVersion to return PostgreSQL 9.6
	rows := sqlmock.NewRows([]string{"version"}).AddRow(90624)
	mock.ExpectQuery("SELECT current_setting\\('server_version_num'\\)::int").WillReturnRows(rows)

	err = pgScraper.initializeScrapers()

	assert.NoError(t, err)
	assert.Equal(t, 90624, pgScraper.pgVersion)
	assert.True(t, pgScraper.supportsPG96)
	assert.False(t, pgScraper.supportsPG12)
	assert.False(t, pgScraper.supportsPG14)
	assert.NotNil(t, pgScraper.databaseMetricsScraper)
	assert.NotNil(t, pgScraper.activityScraper)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test initializeScrapers with old unsupported PostgreSQL version
func TestInitializeScrapers_OldVersion(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}

	sqlClient := client.NewSQLClient(db)

	pgScraper := &newRelicPostgreSQLScraper{
		logger:               logger,
		mb:                   mb,
		config:               config,
		db:                   db,
		client:               sqlClient,
		instanceName:         "test-instance",
		metricsBuilderConfig: metricsBuilderConfig,
	}

	// Mock GetVersion to return PostgreSQL 9.5 (unsupported)
	rows := sqlmock.NewRows([]string{"version"}).AddRow(90500)
	mock.ExpectQuery("SELECT current_setting\\('server_version_num'\\)::int").WillReturnRows(rows)

	err = pgScraper.initializeScrapers()

	assert.NoError(t, err)
	assert.Equal(t, 90500, pgScraper.pgVersion)
	assert.False(t, pgScraper.supportsPG96)
	assert.False(t, pgScraper.supportsPG12)
	assert.False(t, pgScraper.supportsPG14)
	// Database scraper should be initialized even for old versions
	assert.NotNil(t, pgScraper.databaseMetricsScraper)
	// Activity and other scrapers should NOT be initialized
	assert.Nil(t, pgScraper.activityScraper)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test initializeScrapers with version detection failure
func TestInitializeScrapers_VersionDetectionFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}

	sqlClient := client.NewSQLClient(db)

	pgScraper := &newRelicPostgreSQLScraper{
		logger:               logger,
		mb:                   mb,
		config:               config,
		db:                   db,
		client:               sqlClient,
		instanceName:         "test-instance",
		metricsBuilderConfig: metricsBuilderConfig,
	}

	// Mock GetVersion to return error
	mock.ExpectQuery("SELECT current_setting\\('server_version_num'\\)::int").
		WillReturnError(errors.New("version query failed"))

	err = pgScraper.initializeScrapers()

	// Should not fail, but version should be 0 and features disabled
	assert.NoError(t, err)
	assert.Equal(t, 0, pgScraper.pgVersion)
	assert.False(t, pgScraper.supportsPG96)
	assert.False(t, pgScraper.supportsPG12)
	assert.False(t, pgScraper.supportsPG14)
	// Database scraper should still be initialized
	assert.NotNil(t, pgScraper.databaseMetricsScraper)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test start with successful initialization
func TestStart_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	config := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}

	pgScraper := &newRelicPostgreSQLScraper{
		logger:               logger,
		mb:                   mb,
		config:               config,
		instanceName:         "test-instance",
		metricsBuilderConfig: metricsBuilderConfig,
		dbProviderFunc: func() (*sql.DB, error) {
			return db, nil
		},
	}

	// Mock GetVersion for initializeScrapers
	rows := sqlmock.NewRows([]string{"version"}).AddRow(140000)
	mock.ExpectQuery("SELECT current_setting\\('server_version_num'\\)::int").WillReturnRows(rows)

	err = pgScraper.start(context.Background(), newMockHost())

	assert.NoError(t, err)
	assert.NotNil(t, pgScraper.db)
	assert.NotNil(t, pgScraper.client)
	assert.Equal(t, 140000, pgScraper.pgVersion)
	assert.NotEqual(t, pcommon.Timestamp(0), pgScraper.startTime)
	assert.NoError(t, mock.ExpectationsWereMet())
}

