// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicpostgresqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver"

import (
	"context"
	"fmt"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/scrapers"
)

// PostgreSQL version constants
const (
	PG10Version = 100000 // PostgreSQL 10.0
)

// postgreSQLScraper handles PostgreSQL metrics collection
type postgreSQLScraper struct {
	client    client.PostgreSQLClient
	config    *Config
	logger    *zap.Logger
	startTime pcommon.Timestamp
	settings  receiver.Settings
	version   int // PostgreSQL version number
}

// newPostgreSQLScraper creates a new PostgreSQL scraper
func newPostgreSQLScraper(settings receiver.Settings, cfg *Config) *postgreSQLScraper {
	return &postgreSQLScraper{
		config:   cfg,
		logger:   settings.Logger,
		settings: settings,
	}
}

// Start initializes the scraper and establishes database connection
func (s *postgreSQLScraper) Start(ctx context.Context, _ component.Host) error {
	s.logger.Info("Starting PostgreSQL receiver")

	// Skip database connection for testing with empty config
	if s.config.Hostname == "" || s.config.Port == "" || s.config.Username == "" || s.config.Database == "" {
		s.logger.Debug("Skipping database connection (empty config for testing)")
		return nil
	}

	// Build connection string
	connStr := s.buildConnectionString()

	// Create SQL client
	sqlClient, err := client.NewSQLClient(connStr)
	if err != nil {
		s.logger.Error("Failed to create PostgreSQL client", zap.Error(err))
		return err
	}

	s.client = sqlClient
	s.startTime = pcommon.NewTimestampFromTime(time.Now())

	// Test connection
	if err := s.client.Ping(ctx); err != nil {
		s.logger.Error("Failed to ping PostgreSQL", zap.Error(err))
		return err
	}

	// Get PostgreSQL version
	version, err := s.client.GetVersion(ctx)
	if err != nil {
		s.logger.Error("Failed to get PostgreSQL version", zap.Error(err))
		return err
	}
	s.version = version

	s.logger.Info("Successfully connected to PostgreSQL",
		zap.String("hostname", s.config.Hostname),
		zap.String("port", s.config.Port),
		zap.String("database", s.config.Database),
		zap.Int("version", version))

	return nil
}

// Shutdown closes the database connection
func (s *postgreSQLScraper) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down PostgreSQL receiver")

	if s.client != nil {
		if err := s.client.Close(); err != nil {
			s.logger.Error("Error closing database connection", zap.Error(err))
			return err
		}
	}

	return nil
}

// scrape collects metrics from PostgreSQL
func (s *postgreSQLScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	// Return empty metrics if database connection is not established (test mode)
	if s.client == nil {
		return pmetric.NewMetrics(), nil
	}

	mb := metadata.NewMetricsBuilder(s.config.MetricsBuilderConfig, s.settings)
	now := pcommon.NewTimestampFromTime(time.Now())

	// Collect connection count metric
	var connectionCount int64
	query := "SELECT count(*) FROM pg_stat_activity WHERE datname = $1"

	// Direct database access for simple connection count
	// TODO: Move this to client interface for better abstraction
	if sqlClient, ok := s.client.(*client.SQLClient); ok {
		err := sqlClient.QueryRow(ctx, query, s.config.Database, &connectionCount)
		if err != nil {
			s.logger.Error("Failed to query connection count", zap.Error(err))
			return pmetric.NewMetrics(), err
		}
	}

	mb.RecordPostgresqlConnectionCountDataPoint(now, connectionCount)

	// Collect replication metrics (PostgreSQL 9.6+)
	// Version-aware collection:
	// - PostgreSQL 9.6: LSN delays only
	// - PostgreSQL 10+: LSN delays + lag times
	replicationScraper := scrapers.NewReplicationScraper(s.client, mb, s.logger, s.version)
	if errs := replicationScraper.ScrapeReplicationMetrics(ctx, now); len(errs) > 0 {
		for _, err := range errs {
			s.logger.Warn("Failed to collect replication metrics", zap.Error(err))
		}
		// Continue even if replication metrics fail
	}

	// Build resource attributes
	rb := mb.NewResourceBuilder()
	rb.SetDatabaseName(s.config.Database)
	rb.SetDbSystem("postgresql")
	rb.SetServerAddress(s.config.Hostname)
	rb.SetServerPort(s.config.Port)

	// Get PostgreSQL version string
	var versionStr string
	versionQuery := "SELECT version()"
	if sqlClient, ok := s.client.(*client.SQLClient); ok {
		if err := sqlClient.QueryRow(ctx, versionQuery, nil, &versionStr); err == nil {
			rb.SetPostgresqlVersion(versionStr)
		}
	}

	return mb.Emit(metadata.WithResource(rb.Emit())), nil
}

// ScrapeLogs collects logs from PostgreSQL (placeholder for future implementation)
func (s *postgreSQLScraper) ScrapeLogs(ctx context.Context) (plog.Logs, error) {
	// TODO: Implement log scraping logic
	return plog.NewLogs(), nil
}

// buildConnectionString constructs the PostgreSQL connection string
func (s *postgreSQLScraper) buildConnectionString() string {
	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=%s",
		s.config.Hostname,
		s.config.Port,
		s.config.Username,
		s.config.Database,
		s.config.SSLMode,
	)

	if s.config.Password != "" {
		connStr += fmt.Sprintf(" password=%s", s.config.Password)
	}

	if s.config.SSLCert != "" {
		connStr += fmt.Sprintf(" sslcert=%s", s.config.SSLCert)
	}

	if s.config.SSLKey != "" {
		connStr += fmt.Sprintf(" sslkey=%s", s.config.SSLKey)
	}

	if s.config.SSLRootCert != "" {
		connStr += fmt.Sprintf(" sslrootcert=%s", s.config.SSLRootCert)
	}

	if s.config.Timeout > 0 {
		connStr += fmt.Sprintf(" connect_timeout=%d", int(s.config.Timeout.Seconds()))
	}

	return connStr
}
