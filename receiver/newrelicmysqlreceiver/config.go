// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver"

import (
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

// Config defines configuration for New Relic MySQL receiver.
type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`
	Username                       string              `mapstructure:"username,omitempty"`
	Password                       configopaque.String `mapstructure:"password,omitempty"`
	Database                       string              `mapstructure:"database,omitempty"`
	AllowNativePasswords           bool                `mapstructure:"allow_native_passwords,omitempty"`
	confignet.AddrConfig           `mapstructure:",squash"`
	TLS                            configtls.ClientConfig        `mapstructure:"tls,omitempty"`
	MetricsBuilderConfig           metadata.MetricsBuilderConfig `mapstructure:",squash"`

	// ===========================================
	// SLOW QUERY MONITORING CONFIGURATION
	// ===========================================
	// These settings control Top N slow query monitoring using MySQL performance_schema

	// QueryMonitoringEnabled enables/disables slow query monitoring
	// Default: false (feature is opt-in)
	// When enabled, collector will query performance_schema.events_statements_summary_by_digest
	QueryMonitoringEnabled bool `mapstructure:"query_monitoring_enabled"`

	// QueryMonitoringIntervalSeconds defines the time window for fetching queries
	// Default: 60 seconds
	// Example: 60 = fetch queries executed in the last 60 seconds
	// Recommendation: Match this to collection_interval for best results
	// SQL filter: WHERE LAST_SEEN >= DATE_SUB(NOW(), INTERVAL ? SECOND)
	QueryMonitoringIntervalSeconds int `mapstructure:"query_monitoring_interval_seconds"`

	// QueryMonitoringTopN defines how many slow queries to report
	// Default: 10
	// Example: 10 = report top 10 slowest queries based on interval average
	// Applied after delta calculation and sorting by interval_avg_elapsed_time
	QueryMonitoringTopN int `mapstructure:"query_monitoring_top_n"`

	// QueryMonitoringElapsedTimeThreshold defines minimum elapsed time threshold in milliseconds
	// Default: 100 ms
	// Example: 100 = only report queries with interval_avg_elapsed_time >= 100ms
	// Applied AFTER delta calculation (not on historical average)
	// This filters out fast queries that don't need optimization
	QueryMonitoringElapsedTimeThreshold int `mapstructure:"query_monitoring_elapsed_time_threshold"`

	// QueryMonitoringEnableIntervalCalc enables interval-based delta calculation
	// Default: true (strongly recommended)
	// When true: Uses MySQLIntervalCalculator to compute delta metrics
	// When false: Uses historical averages from database (less accurate)
	// Delta calculation is essential for accurate Top N selection
	QueryMonitoringEnableIntervalCalc bool `mapstructure:"query_monitoring_enable_interval_calc"`

	// QueryMonitoringIntervalCacheTTL defines how long to cache query state (minutes)
	// Default: 60 minutes
	// Example: 60 = keep query state for 60 minutes after last seen
	// Used by interval calculator for TTL-based cleanup
	// Prevents memory leaks from queries that are no longer executed
	QueryMonitoringIntervalCacheTTL int `mapstructure:"query_monitoring_interval_cache_ttl"`
}

func (cfg *Config) Unmarshal(componentParser *confmap.Conf) error {
	if componentParser == nil {
		return nil
	}

	// Default to insecure TLS
	if !componentParser.IsSet("tls") {
		cfg.TLS = configtls.ClientConfig{}
		cfg.TLS.Insecure = true
	}

	// Set default values for slow query monitoring configuration
	// These defaults provide sensible starting points for production use

	// Default: Feature disabled (opt-in)
	if !componentParser.IsSet("query_monitoring_enabled") {
		cfg.QueryMonitoringEnabled = false
	}

	// Default: 60 second time window (matches typical collection_interval)
	if !componentParser.IsSet("query_monitoring_interval_seconds") {
		cfg.QueryMonitoringIntervalSeconds = 60
	}

	// Default: Top 10 slow queries (reasonable cardinality for most systems)
	if !componentParser.IsSet("query_monitoring_top_n") {
		cfg.QueryMonitoringTopN = 10
	}

	// Default: 100ms threshold (filters out fast queries)
	if !componentParser.IsSet("query_monitoring_elapsed_time_threshold") {
		cfg.QueryMonitoringElapsedTimeThreshold = 100
	}

	// Default: Interval calculation enabled (strongly recommended for accuracy)
	if !componentParser.IsSet("query_monitoring_enable_interval_calc") {
		cfg.QueryMonitoringEnableIntervalCalc = true
	}

	// Default: 60 minute cache TTL (prevents memory leaks while allowing reasonable tracking)
	if !componentParser.IsSet("query_monitoring_interval_cache_ttl") {
		cfg.QueryMonitoringIntervalCacheTTL = 60
	}

	return componentParser.Unmarshal(cfg)
}
