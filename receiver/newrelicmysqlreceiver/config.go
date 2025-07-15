package newrelicmysqlreceiver

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/multierr"
)

// Config represents the configuration for the New Relic MySQL receiver
type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`

	// MySQL connection configuration
	Endpoint string `mapstructure:"endpoint"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`

	// Transport configuration
	Transport      string        `mapstructure:"transport"`
	TLSConfig      TLSConfig     `mapstructure:"tls"`
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`
	ReadTimeout    time.Duration `mapstructure:"read_timeout"`
	WriteTimeout   time.Duration `mapstructure:"write_timeout"`

	// Query performance monitoring
	QueryPerformanceConfig QueryPerformanceConfig `mapstructure:"query_performance"`

	// Wait events monitoring
	WaitEventsConfig WaitEventsConfig `mapstructure:"wait_events"`

	// Blocking sessions monitoring
	BlockingSessionsConfig BlockingSessionsConfig `mapstructure:"blocking_sessions"`

	// InnoDB enhanced metrics
	InnoDBConfig InnoDBConfig `mapstructure:"innodb"`

	// Replication monitoring
	ReplicationConfig ReplicationConfig `mapstructure:"replication"`

	// Performance schema configuration
	PerformanceSchemaConfig PerformanceSchemaConfig `mapstructure:"performance_schema"`

	// Slow query log monitoring
	SlowQueryConfig SlowQueryConfig `mapstructure:"slow_query"`

	// Feature flags
	EnabledFeatures []string `mapstructure:"enabled_features"`
}

// TLSConfig represents TLS configuration for MySQL connection
type TLSConfig struct {
	Insecure           bool   `mapstructure:"insecure"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	CAFile             string `mapstructure:"ca_file"`
	ServerName         string `mapstructure:"server_name"`
}

// QueryPerformanceConfig represents query performance monitoring configuration
type QueryPerformanceConfig struct {
	Enabled              bool          `mapstructure:"enabled"`
	MaxDigests           int           `mapstructure:"max_digests"`
	DigestTextMaxLength  int           `mapstructure:"digest_text_max_length"`
	CollectionInterval   time.Duration `mapstructure:"collection_interval"`
	IncludeQueryExamples bool          `mapstructure:"include_query_examples"`
	ExcludeSystemQueries bool          `mapstructure:"exclude_system_queries"`
	MinQueryTime         time.Duration `mapstructure:"min_query_time"`
}

// WaitEventsConfig represents wait events monitoring configuration
type WaitEventsConfig struct {
	Enabled            bool          `mapstructure:"enabled"`
	CollectionInterval time.Duration `mapstructure:"collection_interval"`
	MaxEvents          int           `mapstructure:"max_events"`
	EventTypes         []string      `mapstructure:"event_types"`
}

// BlockingSessionsConfig represents blocking sessions monitoring configuration
type BlockingSessionsConfig struct {
	Enabled            bool          `mapstructure:"enabled"`
	CollectionInterval time.Duration `mapstructure:"collection_interval"`
	MaxSessions        int           `mapstructure:"max_sessions"`
	MinWaitTime        time.Duration `mapstructure:"min_wait_time"`
}

// InnoDBConfig represents InnoDB enhanced metrics configuration
type InnoDBConfig struct {
	Enabled            bool          `mapstructure:"enabled"`
	CollectionInterval time.Duration `mapstructure:"collection_interval"`
	DetailedMetrics    bool          `mapstructure:"detailed_metrics"`
}

// ReplicationConfig represents replication monitoring configuration
type ReplicationConfig struct {
	Enabled            bool          `mapstructure:"enabled"`
	CollectionInterval time.Duration `mapstructure:"collection_interval"`
	MonitorSlaveStatus bool          `mapstructure:"monitor_slave_status"`
}

// PerformanceSchemaConfig represents performance schema configuration
type PerformanceSchemaConfig struct {
	Enabled            bool          `mapstructure:"enabled"`
	CollectionInterval time.Duration `mapstructure:"collection_interval"`
	TableIOStats       bool          `mapstructure:"table_io_stats"`
	IndexIOStats       bool          `mapstructure:"index_io_stats"`
	FileIOStats        bool          `mapstructure:"file_io_stats"`
}

// SlowQueryConfig represents slow query log monitoring configuration
type SlowQueryConfig struct {
	Enabled            bool          `mapstructure:"enabled"`
	CollectionInterval time.Duration `mapstructure:"collection_interval"`
	MaxQueries         int           `mapstructure:"max_queries"`
	MinQueryTime       time.Duration `mapstructure:"min_query_time"`
}

// createDefaultConfig creates a default configuration for the receiver
func createDefaultConfig() component.Config {
	return &Config{
		ControllerConfig: scraperhelper.NewDefaultControllerConfig(),
		Transport:        "tcp",
		ConnectTimeout:   10 * time.Second,
		ReadTimeout:      30 * time.Second,
		WriteTimeout:     30 * time.Second,
		QueryPerformanceConfig: QueryPerformanceConfig{
			Enabled:              true,
			MaxDigests:           1000,
			DigestTextMaxLength:  1024,
			CollectionInterval:   60 * time.Second,
			IncludeQueryExamples: false,
			ExcludeSystemQueries: true,
			MinQueryTime:         1 * time.Millisecond,
		},
		WaitEventsConfig: WaitEventsConfig{
			Enabled:            true,
			CollectionInterval: 60 * time.Second,
			MaxEvents:          1000,
			EventTypes:         []string{"io", "lock", "sync"},
		},
		BlockingSessionsConfig: BlockingSessionsConfig{
			Enabled:            true,
			CollectionInterval: 30 * time.Second,
			MaxSessions:        100,
			MinWaitTime:        1 * time.Second,
		},
		InnoDBConfig: InnoDBConfig{
			Enabled:            true,
			CollectionInterval: 60 * time.Second,
			DetailedMetrics:    true,
		},
		ReplicationConfig: ReplicationConfig{
			Enabled:            true,
			CollectionInterval: 60 * time.Second,
			MonitorSlaveStatus: true,
		},
		PerformanceSchemaConfig: PerformanceSchemaConfig{
			Enabled:            true,
			CollectionInterval: 60 * time.Second,
			TableIOStats:       true,
			IndexIOStats:       true,
			FileIOStats:        false,
		},
		SlowQueryConfig: SlowQueryConfig{
			Enabled:            true,
			CollectionInterval: 300 * time.Second,
			MaxQueries:         100,
			MinQueryTime:       1 * time.Second,
		},
		EnabledFeatures: []string{
			"query_performance",
			"wait_events",
			"blocking_sessions",
			"innodb",
			"replication",
			"performance_schema",
			"slow_query",
		},
	}
}

// Validate validates the configuration
func (cfg *Config) Validate() error {
	var err error

	// Validate endpoint
	if cfg.Endpoint == "" {
		err = multierr.Append(err, errors.New("endpoint is required"))
	} else {
		// Parse and validate the endpoint URL
		if _, parseErr := url.Parse(cfg.Endpoint); parseErr != nil {
			err = multierr.Append(err, fmt.Errorf("invalid endpoint: %w", parseErr))
		}
	}

	// Validate username
	if cfg.Username == "" {
		err = multierr.Append(err, errors.New("username is required"))
	}

	// Validate collection intervals from ControllerConfig
	if cfg.ControllerConfig.CollectionInterval <= 0 {
		err = multierr.Append(err, errors.New("collection_interval must be positive"))
	}

	// Validate feature-specific configurations
	if cfg.QueryPerformanceConfig.Enabled {
		if cfg.QueryPerformanceConfig.MaxDigests <= 0 {
			err = multierr.Append(err, errors.New("query_performance.max_digests must be positive"))
		}
		if cfg.QueryPerformanceConfig.DigestTextMaxLength <= 0 {
			err = multierr.Append(err, errors.New("query_performance.digest_text_max_length must be positive"))
		}
	}

	if cfg.WaitEventsConfig.Enabled {
		if cfg.WaitEventsConfig.MaxEvents <= 0 {
			err = multierr.Append(err, errors.New("wait_events.max_events must be positive"))
		}
	}

	if cfg.BlockingSessionsConfig.Enabled {
		if cfg.BlockingSessionsConfig.MaxSessions <= 0 {
			err = multierr.Append(err, errors.New("blocking_sessions.max_sessions must be positive"))
		}
	}

	if cfg.SlowQueryConfig.Enabled {
		if cfg.SlowQueryConfig.MaxQueries <= 0 {
			err = multierr.Append(err, errors.New("slow_query.max_queries must be positive"))
		}
	}

	// Validate timeouts
	if cfg.ConnectTimeout <= 0 {
		err = multierr.Append(err, errors.New("connect_timeout must be positive"))
	}

	if cfg.ReadTimeout <= 0 {
		err = multierr.Append(err, errors.New("read_timeout must be positive"))
	}

	if cfg.WriteTimeout <= 0 {
		err = multierr.Append(err, errors.New("write_timeout must be positive"))
	}

	return err
}

// GetDSN constructs a MySQL DSN from the configuration
func (cfg *Config) GetDSN() string {
	dsn := fmt.Sprintf("%s:%s@%s(%s)/%s",
		cfg.Username,
		cfg.Password,
		cfg.Transport,
		cfg.Endpoint,
		cfg.Database)

	// Add timeout parameters
	params := url.Values{}
	if cfg.ConnectTimeout > 0 {
		params.Add("timeout", cfg.ConnectTimeout.String())
	}
	if cfg.ReadTimeout > 0 {
		params.Add("readTimeout", cfg.ReadTimeout.String())
	}
	if cfg.WriteTimeout > 0 {
		params.Add("writeTimeout", cfg.WriteTimeout.String())
	}

	// Add TLS configuration
	if !cfg.TLSConfig.Insecure {
		params.Add("tls", "true")
		if cfg.TLSConfig.InsecureSkipVerify {
			params.Add("tls", "skip-verify")
		}
	}

	if len(params) > 0 {
		dsn += "?" + params.Encode()
	}

	return dsn
}

// IsFeatureEnabled checks if a feature is enabled
func (cfg *Config) IsFeatureEnabled(feature string) bool {
	for _, f := range cfg.EnabledFeatures {
		if f == feature {
			return true
		}
	}
	return false
}
