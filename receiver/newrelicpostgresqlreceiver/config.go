// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicpostgresqlreceiver

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/queries"
)

const (
	// Query Performance Monitoring defaults
	// defaultEnableQueryMonitoring                = false
	defaultQueryMonitoringSQLRowLimit           = 5000 // Fetch top 5000 candidates by historical average
	defaultQueryMonitoringResponseTimeThreshold = queries.DefaultQueryMonitoringResponseTimeThreshold
	defaultQueryMonitoringCountThreshold        = queries.DefaultQueryMonitoringCountThreshold

	// Interval Calculator defaults
	defaultEnableIntervalBasedAveraging      = true // Enable by default for better slow query detection
	defaultIntervalCalculatorCacheTTLMinutes = 10   // 10 minutes cache TTL

	// Validation ranges
	minQueryMonitoringSQLRowLimit           = 100   // Minimum SQL row limit
	maxQueryMonitoringSQLRowLimit           = 50000 // Maximum SQL row limit
	minQueryMonitoringResponseTimeThreshold = queries.MinQueryMonitoringResponseTimeThreshold
	maxQueryMonitoringResponseTimeThreshold = queries.MaxQueryMonitoringResponseTimeThreshold
	minQueryMonitoringCountThreshold        = queries.MinQueryMonitoringCountThreshold
	maxQueryMonitoringCountThreshold        = queries.MaxQueryMonitoringCountThreshold
	minIntervalCalculatorCacheTTLMinutes    = 1  // Minimum cache TTL (1 minute)
	maxIntervalCalculatorCacheTTLMinutes    = 60 // Maximum cache TTL (60 minutes)
)

// RelationConfig configures which tables to collect per-table metrics from
// This is an opt-in feature to prevent cardinality explosion
type RelationConfig struct {
	// Schemas to collect from (e.g., ["public", "myapp"])
	// If empty, defaults to ["public"]
	Schemas []string `mapstructure:"schemas"`

	// Tables to collect metrics from
	// Can use exact names: ["users", "orders"]
	// If empty, no per-table metrics will be collected
	Tables []string `mapstructure:"tables"`
}

// Config represents the receiver config settings within the collector's config.yaml
type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`
	metadata.MetricsBuilderConfig  `mapstructure:",squash"`

	// Connection configuration
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Hostname string `mapstructure:"hostname"`
	Port     string `mapstructure:"port"`
	Database string `mapstructure:"database"`

	// SSL configuration
	SSLMode     string `mapstructure:"ssl_mode"`      // disable, require, verify-ca, verify-full
	SSLCert     string `mapstructure:"ssl_cert"`      // Path to SSL certificate
	SSLKey      string `mapstructure:"ssl_key"`       // Path to SSL key
	SSLRootCert string `mapstructure:"ssl_root_cert"` // Path to SSL root certificate

	// Timeout for database operations
	Timeout time.Duration `mapstructure:"timeout"`

	// Relations configuration for per-table metrics (OPTIONAL)
	// If nil (not configured), per-table metrics will NOT be collected
	// This prevents cardinality explosion in databases with many tables
	// Example:
	//   relations:
	//     schemas: [public, myapp]
	//     tables: [users, orders, payments]
	Relations *RelationConfig `mapstructure:"relations"`

	// Slow Query Monitoring Configuration (similar to Oracle's query_monitoring)
	// Controls slow query detection from pg_stat_statements extension
	// Default: false (opt-in feature - must be explicitly enabled)
	EnableQueryMonitoring bool `mapstructure:"enable_query_monitoring"`

	// SQL row limit for pre-filtering (replaces Oracle's interval_seconds)
	// Fetches top N queries by historical average to reduce memory usage
	// Default: 5000 queries
	QueryMonitoringSQLRowLimit int `mapstructure:"query_monitoring_sql_row_limit"`

	// Response time threshold in milliseconds (applied AFTER delta calculation)
	// Only emit queries with interval average > threshold
	// Default: 1000ms
	QueryMonitoringResponseTimeThreshold int `mapstructure:"query_monitoring_response_time_threshold"`

	// Count threshold for top N selection (applied AFTER delta calculation and threshold filtering)
	// Limits cardinality for monitoring system
	// Default: 100 queries
	QueryMonitoringCountThreshold int `mapstructure:"query_monitoring_count_threshold"`

	// Interval Calculator Configuration (same as Oracle)
	// Enable interval-based delta calculation for immediate performance change detection
	// Default: true
	EnableIntervalBasedAveraging bool `mapstructure:"enable_interval_based_averaging"`

	// Cache TTL for interval calculator in minutes
	// Removes inactive queries from cache after this period
	// Default: 10 minutes
	IntervalCalculatorCacheTTLMinutes int `mapstructure:"interval_calculator_cache_ttl_minutes"`
}

// SetDefaults sets default values for configuration fields that are not explicitly set
func (cfg *Config) SetDefaults() {
	// Note: EnableQueryMonitoring defaults to false (opt-in feature)
	// User must explicitly set enable_query_monitoring: true in config

	// Set slow query monitoring defaults (only used if slow query monitoring is enabled)
	if cfg.QueryMonitoringSQLRowLimit == 0 {
		cfg.QueryMonitoringSQLRowLimit = defaultQueryMonitoringSQLRowLimit
	}
	if cfg.QueryMonitoringResponseTimeThreshold == 0 {
		cfg.QueryMonitoringResponseTimeThreshold = defaultQueryMonitoringResponseTimeThreshold
	}
	if cfg.QueryMonitoringCountThreshold == 0 {
		cfg.QueryMonitoringCountThreshold = defaultQueryMonitoringCountThreshold
	}

	// Set interval calculator defaults
	// Note: EnableIntervalBasedAveraging defaults to true if EnableQueryMonitoring is true
	if cfg.EnableQueryMonitoring && !cfg.EnableIntervalBasedAveraging {
		cfg.EnableIntervalBasedAveraging = defaultEnableIntervalBasedAveraging
	}
	if cfg.IntervalCalculatorCacheTTLMinutes == 0 {
		cfg.IntervalCalculatorCacheTTLMinutes = defaultIntervalCalculatorCacheTTLMinutes
	}
}

// Validate checks if the receiver configuration is valid
func (cfg *Config) Validate() error {
	if cfg.Hostname == "" {
		return errors.New("hostname must be specified")
	}

	if cfg.Port == "" {
		return errors.New("port must be specified")
	}

	if cfg.Username == "" {
		return errors.New("username must be specified")
	}

	if cfg.Database == "" {
		return errors.New("database must be specified")
	}

	// Validate SSL mode
	validSSLModes := map[string]bool{
		"disable":     true,
		"require":     true,
		"verify-ca":   true,
		"verify-full": true,
	}

	if !validSSLModes[cfg.SSLMode] {
		return fmt.Errorf("invalid ssl_mode: %s. Valid values are: disable, require, verify-ca, verify-full", cfg.SSLMode)
	}

	// Timeout is optional, no validation needed

	// Validate slow query monitoring settings
	if cfg.EnableQueryMonitoring {
		if cfg.QueryMonitoringSQLRowLimit < minQueryMonitoringSQLRowLimit ||
			cfg.QueryMonitoringSQLRowLimit > maxQueryMonitoringSQLRowLimit {
			return fmt.Errorf("query_monitoring_sql_row_limit must be between %d and %d, got %d",
				minQueryMonitoringSQLRowLimit, maxQueryMonitoringSQLRowLimit, cfg.QueryMonitoringSQLRowLimit)
		}

		if cfg.QueryMonitoringResponseTimeThreshold < minQueryMonitoringResponseTimeThreshold ||
			cfg.QueryMonitoringResponseTimeThreshold > maxQueryMonitoringResponseTimeThreshold {
			return fmt.Errorf("query_monitoring_response_time_threshold must be between %d and %d ms, got %d",
				minQueryMonitoringResponseTimeThreshold, maxQueryMonitoringResponseTimeThreshold, cfg.QueryMonitoringResponseTimeThreshold)
		}

		if cfg.QueryMonitoringCountThreshold < minQueryMonitoringCountThreshold ||
			cfg.QueryMonitoringCountThreshold > maxQueryMonitoringCountThreshold {
			return fmt.Errorf("query_monitoring_count_threshold must be between %d and %d, got %d",
				minQueryMonitoringCountThreshold, maxQueryMonitoringCountThreshold, cfg.QueryMonitoringCountThreshold)
		}

		if cfg.IntervalCalculatorCacheTTLMinutes < minIntervalCalculatorCacheTTLMinutes ||
			cfg.IntervalCalculatorCacheTTLMinutes > maxIntervalCalculatorCacheTTLMinutes {
			return fmt.Errorf("interval_calculator_cache_ttl_minutes must be between %d and %d, got %d",
				minIntervalCalculatorCacheTTLMinutes, maxIntervalCalculatorCacheTTLMinutes, cfg.IntervalCalculatorCacheTTLMinutes)
		}
	}

	return nil
}

// Unmarshal handles custom unmarshaling logic for the config
func (cfg *Config) Unmarshal(componentParser *confmap.Conf) error {
	if componentParser == nil {
		return nil
	}

	// Unmarshal the base config
	if err := componentParser.Unmarshal(cfg); err != nil {
		return err
	}

	return nil
}
