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
	// Optional metric collection flags
	ExtraInnoDBMetrics   bool `mapstructure:"extra_innodb_metrics,omitempty"`
	ExtraStatusMetrics   bool `mapstructure:"extra_status_metrics,omitempty"`
	confignet.AddrConfig `mapstructure:",squash"`
	TLS                  configtls.ClientConfig        `mapstructure:"tls,omitempty"`
	MetricsBuilderConfig metadata.MetricsBuilderConfig `mapstructure:",squash"`
	// Slow Query monitoring configuration
	SlowQuery SlowQueryConfig `mapstructure:"slow_query,omitempty"`
}

// SlowQueryConfig defines configuration for slow query monitoring
type SlowQueryConfig struct {
	// Enabled controls whether slow query monitoring is active
	Enabled bool `mapstructure:"enabled"`
	// ResponseTimeThreshold is the minimum average elapsed time in milliseconds
	// Only queries with avg elapsed time >= this value will be collected
	ResponseTimeThreshold int `mapstructure:"response_time_threshold"`
	// CountThreshold is the maximum number of slow queries to collect per scrape (top N)
	CountThreshold int `mapstructure:"count_threshold"`
	// IntervalSeconds is the time window in seconds for fetching queries from performance_schema
	IntervalSeconds int `mapstructure:"interval_seconds"`
	// EnableIntervalCalculator enables delta calculation for interval-based metrics
	EnableIntervalCalculator bool `mapstructure:"enable_interval_calculator"`
	// CacheTTLMinutes is the cache TTL in minutes for the interval calculator
	CacheTTLMinutes int `mapstructure:"cache_ttl_minutes"`
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

	return componentParser.Unmarshal(cfg)
}
