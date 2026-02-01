// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicsqlserverreceiver

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig().(*Config)

	assert.Equal(t, "127.0.0.1", cfg.Hostname)
	assert.Equal(t, "1433", cfg.Port)
	assert.Equal(t, 10, cfg.MaxConcurrentWorkers)
	assert.Equal(t, 30*time.Second, cfg.Timeout)
	assert.Equal(t, false, cfg.EnableSSL)
	assert.Equal(t, false, cfg.TrustServerCertificate)
	assert.Equal(t, true, cfg.EnableQueryMonitoring)
	assert.Equal(t, 0, cfg.QueryMonitoringResponseTimeThreshold)
	assert.Equal(t, 20, cfg.QueryMonitoringCountThreshold)
	assert.Equal(t, 15, cfg.QueryMonitoringFetchInterval)
	assert.Equal(t, 4094, cfg.QueryMonitoringTextTruncateLimit)
	assert.Equal(t, true, cfg.EnableActiveRunningQueries)
	assert.Equal(t, 0, cfg.ActiveRunningQueriesElapsedTimeThreshold)
	assert.Equal(t, false, cfg.EnableSlowQuerySmoothing)
	assert.Equal(t, 0.3, cfg.SlowQuerySmoothingFactor)
	assert.Equal(t, 3, cfg.SlowQuerySmoothingDecayThreshold)
	assert.Equal(t, 5, cfg.SlowQuerySmoothingMaxAgeMinutes)
	assert.Equal(t, true, cfg.EnableIntervalBasedAveraging)
	assert.Equal(t, 10, cfg.IntervalCalculatorCacheTTLMinutes)
	assert.Equal(t, true, cfg.EnableWaitResourceEnrichment)
	assert.Equal(t, 5, cfg.WaitResourceMetadataRefreshMinutes)
	assert.Equal(t, true, cfg.EnableDatabaseBufferMetrics)
	assert.Equal(t, 15*time.Second, cfg.ControllerConfig.CollectionInterval)
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config with port",
			config: &Config{
				Hostname:             "localhost",
				Port:                 "1433",
				MaxConcurrentWorkers: 5,
				Timeout:              30 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "valid config with instance",
			config: &Config{
				Hostname:             "localhost",
				Instance:             "SQLEXPRESS",
				MaxConcurrentWorkers: 5,
				Timeout:              30 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "empty hostname",
			config: &Config{
				Port:                 "1433",
				MaxConcurrentWorkers: 5,
				Timeout:              30 * time.Second,
			},
			wantErr: true,
			errMsg:  "hostname cannot be empty",
		},
		{
			name: "both port and instance specified",
			config: &Config{
				Hostname:             "localhost",
				Port:                 "1433",
				Instance:             "SQLEXPRESS",
				MaxConcurrentWorkers: 5,
				Timeout:              30 * time.Second,
			},
			wantErr: true,
			errMsg:  "specify either port or instance but not both",
		},
		{
			name: "neither port nor instance - should default to port 1433",
			config: &Config{
				Hostname:             "localhost",
				MaxConcurrentWorkers: 5,
				Timeout:              30 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "negative timeout",
			config: &Config{
				Hostname:             "localhost",
				Port:                 "1433",
				MaxConcurrentWorkers: 5,
				Timeout:              -10 * time.Second,
			},
			wantErr: true,
			errMsg:  "timeout must be positive",
		},
		{
			name: "zero timeout",
			config: &Config{
				Hostname:             "localhost",
				Port:                 "1433",
				MaxConcurrentWorkers: 5,
				Timeout:              0,
			},
			wantErr: true,
			errMsg:  "timeout must be positive",
		},
		{
			name: "negative max concurrent workers",
			config: &Config{
				Hostname:             "localhost",
				Port:                 "1433",
				MaxConcurrentWorkers: -5,
				Timeout:              30 * time.Second,
			},
			wantErr: true,
			errMsg:  "max_concurrent_workers must be positive",
		},
		{
			name: "zero max concurrent workers",
			config: &Config{
				Hostname:             "localhost",
				Port:                 "1433",
				MaxConcurrentWorkers: 0,
				Timeout:              30 * time.Second,
			},
			wantErr: true,
			errMsg:  "max_concurrent_workers must be positive",
		},
		{
			name: "query monitoring enabled with negative threshold",
			config: &Config{
				Hostname:                             "localhost",
				Port:                                 "1433",
				MaxConcurrentWorkers:                 5,
				Timeout:                              30 * time.Second,
				EnableQueryMonitoring:                true,
				QueryMonitoringResponseTimeThreshold: -1,
				QueryMonitoringCountThreshold:        20,
				QueryMonitoringTextTruncateLimit:     4094,
			},
			wantErr: true,
			errMsg:  "query_monitoring_response_time_threshold must be >= 0",
		},
		{
			name: "query monitoring enabled with zero count threshold",
			config: &Config{
				Hostname:                             "localhost",
				Port:                                 "1433",
				MaxConcurrentWorkers:                 5,
				Timeout:                              30 * time.Second,
				EnableQueryMonitoring:                true,
				QueryMonitoringResponseTimeThreshold: 0,
				QueryMonitoringCountThreshold:        0,
				QueryMonitoringTextTruncateLimit:     4094,
			},
			wantErr: true,
			errMsg:  "query_monitoring_count_threshold must be positive",
		},
		{
			name: "query monitoring enabled with zero text truncate limit",
			config: &Config{
				Hostname:                             "localhost",
				Port:                                 "1433",
				MaxConcurrentWorkers:                 5,
				Timeout:                              30 * time.Second,
				EnableQueryMonitoring:                true,
				QueryMonitoringResponseTimeThreshold: 0,
				QueryMonitoringCountThreshold:        20,
				QueryMonitoringTextTruncateLimit:     0,
			},
			wantErr: true,
			errMsg:  "query_monitoring_text_truncate_limit must be positive",
		},
		{
			name: "SSL enabled without trust and without certificate",
			config: &Config{
				Hostname:               "localhost",
				Port:                   "1433",
				MaxConcurrentWorkers:   5,
				Timeout:                30 * time.Second,
				EnableSSL:              true,
				TrustServerCertificate: false,
				CertificateLocation:    "",
			},
			wantErr: true,
			errMsg:  "must specify a certificate file when using SSL",
		},
		{
			name: "SSL enabled with trust certificate",
			config: &Config{
				Hostname:               "localhost",
				Port:                   "1433",
				MaxConcurrentWorkers:   5,
				Timeout:                30 * time.Second,
				EnableSSL:              true,
				TrustServerCertificate: true,
			},
			wantErr: false,
		},
		{
			name: "both custom query and config file specified",
			config: &Config{
				Hostname:            "localhost",
				Port:                "1433",
				MaxConcurrentWorkers: 5,
				Timeout:             30 * time.Second,
				CustomMetricsQuery:  "SELECT 1",
				CustomMetricsConfig: "/path/to/config.yaml",
			},
			wantErr: true,
			errMsg:  "cannot specify both custom_metrics_query and custom_metrics_config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfigValidateDefaultsPort(t *testing.T) {
	cfg := &Config{
		Hostname:             "localhost",
		MaxConcurrentWorkers: 5,
		Timeout:              30 * time.Second,
	}

	err := cfg.Validate()
	assert.NoError(t, err)
	assert.Equal(t, "1433", cfg.Port, "Should default to port 1433")
}

func TestConfigValidateCustomMetricsConfigFileNotFound(t *testing.T) {
	cfg := &Config{
		Hostname:             "localhost",
		Port:                 "1433",
		MaxConcurrentWorkers: 5,
		Timeout:              30 * time.Second,
		CustomMetricsConfig:  "/non/existent/file.yaml",
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "custom_metrics_config file error")
}

func TestConfigValidateCustomMetricsConfigFileExists(t *testing.T) {
	// Create a temporary file
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "custom_metrics.yaml")
	err := os.WriteFile(tmpFile, []byte("test: data"), 0644)
	require.NoError(t, err)

	cfg := &Config{
		Hostname:             "localhost",
		Port:                 "1433",
		MaxConcurrentWorkers: 5,
		Timeout:              30 * time.Second,
		CustomMetricsConfig:  tmpFile,
	}

	err = cfg.Validate()
	assert.NoError(t, err)
}

func TestConfigUnmarshal(t *testing.T) {
	cfg := &Config{}
	conf := confmap.NewFromStringMap(map[string]interface{}{
		"hostname":               "testhost",
		"port":                   "1435",
		"username":               "testuser",
		"password":               "testpass",
		"max_concurrent_workers": 10,
		"timeout":                "60s",
	})

	err := cfg.Unmarshal(conf)
	assert.NoError(t, err)
	assert.Equal(t, "testhost", cfg.Hostname)
	assert.Equal(t, "1435", cfg.Port)
	assert.Equal(t, "testuser", cfg.Username)
	assert.Equal(t, "testpass", cfg.Password)
	assert.Equal(t, 10, cfg.MaxConcurrentWorkers)
	assert.Equal(t, 60*time.Second, cfg.Timeout)
}

func TestConfigUnmarshalInvalid(t *testing.T) {
	cfg := &Config{}
	conf := confmap.NewFromStringMap(map[string]interface{}{
		"hostname":               "", // Empty hostname should fail validation
		"port":                   "1433",
		"max_concurrent_workers": 5,
		"timeout":                "30s",
	})

	err := cfg.Unmarshal(conf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "hostname cannot be empty")
}

func TestGetMaxConcurrentWorkers(t *testing.T) {
	tests := []struct {
		name     string
		workers  int
		expected int
	}{
		{
			name:     "positive value",
			workers:  5,
			expected: 5,
		},
		{
			name:     "zero defaults to 10",
			workers:  0,
			expected: 10,
		},
		{
			name:     "negative defaults to 10",
			workers:  -5,
			expected: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				MaxConcurrentWorkers: tt.workers,
			}
			assert.Equal(t, tt.expected, cfg.GetMaxConcurrentWorkers())
		})
	}
}

func TestIsAzureADAuth(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		expected bool
	}{
		{
			name: "all azure ad fields present",
			config: &Config{
				ClientID:     "client-id",
				TenantID:     "tenant-id",
				ClientSecret: "client-secret",
			},
			expected: true,
		},
		{
			name: "missing client id",
			config: &Config{
				TenantID:     "tenant-id",
				ClientSecret: "client-secret",
			},
			expected: false,
		},
		{
			name: "missing tenant id",
			config: &Config{
				ClientID:     "client-id",
				ClientSecret: "client-secret",
			},
			expected: false,
		},
		{
			name: "missing client secret",
			config: &Config{
				ClientID: "client-id",
				TenantID: "tenant-id",
			},
			expected: false,
		},
		{
			name:     "no azure ad fields",
			config:   &Config{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.config.IsAzureADAuth())
		})
	}
}

func TestCreateConnectionURL(t *testing.T) {
	tests := []struct {
		name           string
		config         *Config
		dbName         string
		shouldContain  []string
		shouldNotContain []string
	}{
		{
			name: "basic connection with port",
			config: &Config{
				Hostname: "localhost",
				Port:     "1433",
				Username: "sa",
				Password: "pass123",
				Timeout:  30 * time.Second,
			},
			dbName: "testdb",
			shouldContain: []string{
				"server=localhost",
				"port=1433",
				"user id=sa",
				"password=pass123",
				"database=testdb",
				"dial timeout=30",
				"connection timeout=30",
				"encrypt=disable",
			},
		},
		{
			name: "connection with instance",
			config: &Config{
				Hostname: "localhost",
				Instance: "SQLEXPRESS",
				Username: "sa",
				Password: "pass123",
				Timeout:  30 * time.Second,
			},
			dbName: "master",
			shouldContain: []string{
				"server=localhost\\SQLEXPRESS",
				"user id=sa",
				"password=pass123",
				"database=master",
			},
			shouldNotContain: []string{"port="},
		},
		{
			name: "connection with SSL enabled",
			config: &Config{
				Hostname:  "localhost",
				Port:      "1433",
				Username:  "sa",
				Password:  "pass123",
				Timeout:   30 * time.Second,
				EnableSSL: true,
			},
			dbName: "testdb",
			shouldContain: []string{
				"encrypt=true",
			},
			shouldNotContain: []string{"encrypt=disable"},
		},
		{
			name: "connection with SSL and trust certificate",
			config: &Config{
				Hostname:               "localhost",
				Port:                   "1433",
				Username:               "sa",
				Password:               "pass123",
				Timeout:                30 * time.Second,
				EnableSSL:              true,
				TrustServerCertificate: true,
			},
			dbName: "testdb",
			shouldContain: []string{
				"encrypt=true",
				"TrustServerCertificate=true",
			},
		},
		{
			name: "connection with SSL and certificate location",
			config: &Config{
				Hostname:            "localhost",
				Port:                "1433",
				Username:            "sa",
				Password:            "pass123",
				Timeout:             30 * time.Second,
				EnableSSL:           true,
				CertificateLocation: "/path/to/cert.pem",
			},
			dbName: "testdb",
			shouldContain: []string{
				"encrypt=true",
				"certificate=/path/to/cert.pem",
			},
		},
		{
			name: "connection without dbName defaults to master",
			config: &Config{
				Hostname: "localhost",
				Port:     "1433",
				Username: "sa",
				Password: "pass123",
				Timeout:  30 * time.Second,
			},
			dbName: "",
			shouldContain: []string{
				"database=master",
			},
		},
		{
			name: "connection with extra args",
			config: &Config{
				Hostname:               "localhost",
				Port:                   "1433",
				Username:               "sa",
				Password:               "pass123",
				Timeout:                30 * time.Second,
				ExtraConnectionURLArgs: "app+name=myapp&log=255",
			},
			dbName: "testdb",
			shouldContain: []string{
				"app name=myapp",
				"log=255",
			},
		},
		{
			name: "connection without credentials",
			config: &Config{
				Hostname: "localhost",
				Port:     "1433",
				Timeout:  30 * time.Second,
			},
			dbName: "testdb",
			shouldContain: []string{
				"server=localhost",
				"database=testdb",
			},
			shouldNotContain: []string{"user id=", "password="},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connURL := tt.config.CreateConnectionURL(tt.dbName)

			for _, expected := range tt.shouldContain {
				assert.Contains(t, connURL, expected,
					"Connection URL should contain '%s'", expected)
			}

			for _, forbidden := range tt.shouldNotContain {
				assert.NotContains(t, connURL, forbidden,
					"Connection URL should NOT contain '%s'", forbidden)
			}
		})
	}
}

func TestCreateAzureADConnectionURL(t *testing.T) {
	tests := []struct {
		name          string
		config        *Config
		dbName        string
		shouldContain []string
	}{
		{
			name: "basic azure ad connection",
			config: &Config{
				Hostname:     "myserver.database.windows.net",
				Port:         "1433",
				ClientID:     "my-client-id",
				ClientSecret: "my-client-secret",
				TenantID:     "my-tenant-id",
			},
			dbName: "mydb",
			shouldContain: []string{
				"server=myserver.database.windows.net",
				"port=1433",
				"fedauth=ActiveDirectoryServicePrincipal",
				"applicationclientid=my-client-id",
				"clientsecret=my-client-secret",
				"database=mydb",
			},
		},
		{
			name: "azure ad with SSL",
			config: &Config{
				Hostname:     "myserver.database.windows.net",
				Port:         "1433",
				ClientID:     "my-client-id",
				ClientSecret: "my-client-secret",
				TenantID:     "my-tenant-id",
				EnableSSL:    true,
			},
			dbName: "mydb",
			shouldContain: []string{
				"encrypt=true",
			},
		},
		{
			name: "azure ad with SSL and trust certificate",
			config: &Config{
				Hostname:               "myserver.database.windows.net",
				Port:                   "1433",
				ClientID:               "my-client-id",
				ClientSecret:           "my-client-secret",
				TenantID:               "my-tenant-id",
				EnableSSL:              true,
				TrustServerCertificate: true,
			},
			dbName: "mydb",
			shouldContain: []string{
				"encrypt=true",
				"TrustServerCertificate=true",
			},
		},
		{
			name: "azure ad with extra args",
			config: &Config{
				Hostname:               "myserver.database.windows.net",
				Port:                   "1433",
				ClientID:               "my-client-id",
				ClientSecret:           "my-client-secret",
				TenantID:               "my-tenant-id",
				ExtraConnectionURLArgs: "app+name=myapp",
			},
			dbName: "mydb",
			shouldContain: []string{
				"app name=myapp",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connURL := tt.config.CreateAzureADConnectionURL(tt.dbName)

			for _, expected := range tt.shouldContain {
				assert.Contains(t, connURL, expected,
					"Azure AD Connection URL should contain '%s'", expected)
			}
		})
	}
}

func TestConfigStructTags(t *testing.T) {
	// Verify that the Config struct has proper mapstructure tags
	cfg := &Config{}

	// Test that confmap can unmarshal into the Config struct
	conf := confmap.NewFromStringMap(map[string]interface{}{
		"hostname":                          "testhost",
		"port":                              "1433",
		"username":                          "user",
		"password":                          "pass",
		"client_id":                         "client",
		"tenant_id":                         "tenant",
		"client_secret":                     "secret",
		"enable_ssl":                        true,
		"trust_server_certificate":          true,
		"certificate_location":              "/path/to/cert",
		"max_concurrent_workers":            5,
		"timeout":                           "30s",
		"custom_metrics_query":              "SELECT 1",
		"custom_metrics_config":             "",
		"extra_connection_url_args":         "app=test",
		"enable_query_monitoring":           true,
		"query_monitoring_response_time_threshold": 100,
		"query_monitoring_count_threshold":         50,
		"query_monitoring_fetch_interval":          30,
		"query_monitoring_text_truncate_limit":     2048,
		"enable_active_running_queries":                true,
		"active_running_queries_elapsed_time_threshold": 1000,
		"enable_slow_query_smoothing":                   true,
		"slow_query_smoothing_factor":                   0.5,
		"slow_query_smoothing_decay_threshold":          5,
		"slow_query_smoothing_max_age_minutes":          10,
		"enable_interval_based_averaging":               true,
		"interval_calculator_cache_ttl_minutes":         15,
		"enable_wait_resource_enrichment":               true,
		"wait_resource_metadata_refresh_minutes":        10,
		"monitored_databases":                           []interface{}{"db1", "db2"},
		"enable_database_buffer_metrics":                true,
	})

	err := conf.Unmarshal(cfg)
	require.NoError(t, err)

	// Verify all fields were properly unmarshaled
	assert.Equal(t, "testhost", cfg.Hostname)
	assert.Equal(t, "1433", cfg.Port)
	assert.Equal(t, "user", cfg.Username)
	assert.Equal(t, "pass", cfg.Password)
	assert.Equal(t, "client", cfg.ClientID)
	assert.Equal(t, "tenant", cfg.TenantID)
	assert.Equal(t, "secret", cfg.ClientSecret)
	assert.Equal(t, true, cfg.EnableSSL)
	assert.Equal(t, true, cfg.TrustServerCertificate)
	assert.Equal(t, "/path/to/cert", cfg.CertificateLocation)
	assert.Equal(t, 5, cfg.MaxConcurrentWorkers)
	assert.Equal(t, 30*time.Second, cfg.Timeout)
	assert.Equal(t, "SELECT 1", cfg.CustomMetricsQuery)
	assert.Equal(t, "app=test", cfg.ExtraConnectionURLArgs)
	assert.Equal(t, true, cfg.EnableQueryMonitoring)
	assert.Equal(t, 100, cfg.QueryMonitoringResponseTimeThreshold)
	assert.Equal(t, 50, cfg.QueryMonitoringCountThreshold)
	assert.Equal(t, 30, cfg.QueryMonitoringFetchInterval)
	assert.Equal(t, 2048, cfg.QueryMonitoringTextTruncateLimit)
	assert.Equal(t, true, cfg.EnableActiveRunningQueries)
	assert.Equal(t, 1000, cfg.ActiveRunningQueriesElapsedTimeThreshold)
	assert.Equal(t, true, cfg.EnableSlowQuerySmoothing)
	assert.Equal(t, 0.5, cfg.SlowQuerySmoothingFactor)
	assert.Equal(t, 5, cfg.SlowQuerySmoothingDecayThreshold)
	assert.Equal(t, 10, cfg.SlowQuerySmoothingMaxAgeMinutes)
	assert.Equal(t, true, cfg.EnableIntervalBasedAveraging)
	assert.Equal(t, 15, cfg.IntervalCalculatorCacheTTLMinutes)
	assert.Equal(t, true, cfg.EnableWaitResourceEnrichment)
	assert.Equal(t, 10, cfg.WaitResourceMetadataRefreshMinutes)
	assert.Equal(t, []string{"db1", "db2"}, cfg.MonitoredDatabases)
	assert.Equal(t, true, cfg.EnableDatabaseBufferMetrics)
}
