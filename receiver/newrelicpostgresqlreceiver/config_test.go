// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicpostgresqlreceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
)

func TestConfig_Validate_Success(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
	}{
		{
			name: "valid minimal config",
			config: &Config{
				Hostname: "localhost",
				Port:     "5432",
				Username: "postgres",
				Database: "postgres",
				SSLMode:  "disable",
			},
		},
		{
			name: "valid config with password",
			config: &Config{
				Hostname: "localhost",
				Port:     "5432",
				Username: "postgres",
				Password: "secret",
				Database: "testdb",
				SSLMode:  "disable",
			},
		},
		{
			name: "valid config with SSL require",
			config: &Config{
				Hostname: "postgres.example.com",
				Port:     "5432",
				Username: "app_user",
				Password: "password",
				Database: "production",
				SSLMode:  "require",
			},
		},
		{
			name: "valid config with SSL verify-ca",
			config: &Config{
				Hostname:    "postgres.example.com",
				Port:        "5432",
				Username:    "app_user",
				Database:    "production",
				SSLMode:     "verify-ca",
				SSLRootCert: "/path/to/ca.pem",
			},
		},
		{
			name: "valid config with SSL verify-full",
			config: &Config{
				Hostname:    "postgres.example.com",
				Port:        "5432",
				Username:    "app_user",
				Database:    "production",
				SSLMode:     "verify-full",
				SSLCert:     "/path/to/client-cert.pem",
				SSLKey:      "/path/to/client-key.pem",
				SSLRootCert: "/path/to/ca.pem",
			},
		},
		{
			name: "valid config with timeout",
			config: &Config{
				Hostname: "localhost",
				Port:     "5432",
				Username: "postgres",
				Database: "postgres",
				SSLMode:  "disable",
				Timeout:  30 * time.Second,
			},
		},
		{
			name: "valid config with relations",
			config: &Config{
				Hostname: "localhost",
				Port:     "5432",
				Username: "postgres",
				Database: "postgres",
				SSLMode:  "disable",
				Relations: &RelationConfig{
					Schemas: []string{"public", "myapp"},
					Tables:  []string{"users", "orders"},
				},
			},
		},
		{
			name: "valid config with empty relations (no per-table metrics)",
			config: &Config{
				Hostname:  "localhost",
				Port:      "5432",
				Username:  "postgres",
				Database:  "postgres",
				SSLMode:   "disable",
				Relations: &RelationConfig{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			assert.NoError(t, err)
		})
	}
}

func TestConfig_Validate_MissingHostname(t *testing.T) {
	cfg := &Config{
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hostname must be specified")
}

func TestConfig_Validate_MissingPort(t *testing.T) {
	cfg := &Config{
		Hostname: "localhost",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "port must be specified")
}

func TestConfig_Validate_MissingUsername(t *testing.T) {
	cfg := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Database: "postgres",
		SSLMode:  "disable",
	}

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "username must be specified")
}

func TestConfig_Validate_MissingDatabase(t *testing.T) {
	cfg := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		SSLMode:  "disable",
	}

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database must be specified")
}

func TestConfig_Validate_InvalidSSLMode(t *testing.T) {
	tests := []struct {
		name    string
		sslMode string
	}{
		{
			name:    "invalid ssl mode - empty",
			sslMode: "",
		},
		{
			name:    "invalid ssl mode - invalid value",
			sslMode: "invalid",
		},
		{
			name:    "invalid ssl mode - allow (not supported)",
			sslMode: "allow",
		},
		{
			name:    "invalid ssl mode - prefer (not supported)",
			sslMode: "prefer",
		},
		{
			name:    "invalid ssl mode - case sensitive",
			sslMode: "Disable",
		},
		{
			name:    "invalid ssl mode - case sensitive uppercase",
			sslMode: "REQUIRE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Hostname: "localhost",
				Port:     "5432",
				Username: "postgres",
				Database: "postgres",
				SSLMode:  tt.sslMode,
			}

			err := cfg.Validate()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid ssl_mode")
		})
	}
}

func TestConfig_Validate_AllSSLModes(t *testing.T) {
	validSSLModes := []string{"disable", "require", "verify-ca", "verify-full"}

	for _, sslMode := range validSSLModes {
		t.Run("valid ssl mode: "+sslMode, func(t *testing.T) {
			cfg := &Config{
				Hostname: "localhost",
				Port:     "5432",
				Username: "postgres",
				Database: "postgres",
				SSLMode:  sslMode,
			}

			err := cfg.Validate()
			assert.NoError(t, err)
		})
	}
}

func TestConfig_Validate_MultipleErrors(t *testing.T) {
	// Test that validation returns the first error encountered
	cfg := &Config{
		// Missing everything except SSLMode
		SSLMode: "disable",
	}

	err := cfg.Validate()
	assert.Error(t, err)
	// Should fail on hostname first
	assert.Contains(t, err.Error(), "hostname must be specified")
}

func TestConfig_Unmarshal_Success(t *testing.T) {
	configMap := map[string]interface{}{
		"hostname": "localhost",
		"port":     "5432",
		"username": "postgres",
		"password": "secret",
		"database": "testdb",
		"ssl_mode": "disable",
		"timeout":  "30s",
	}

	conf := confmap.NewFromStringMap(configMap)
	cfg := &Config{}

	err := cfg.Unmarshal(conf)
	require.NoError(t, err)

	assert.Equal(t, "localhost", cfg.Hostname)
	assert.Equal(t, "5432", cfg.Port)
	assert.Equal(t, "postgres", cfg.Username)
	assert.Equal(t, "secret", cfg.Password)
	assert.Equal(t, "testdb", cfg.Database)
	assert.Equal(t, "disable", cfg.SSLMode)
	assert.Equal(t, 30*time.Second, cfg.Timeout)
}

func TestConfig_Unmarshal_WithRelations(t *testing.T) {
	configMap := map[string]interface{}{
		"hostname": "localhost",
		"port":     "5432",
		"username": "postgres",
		"database": "testdb",
		"ssl_mode": "disable",
		"relations": map[string]interface{}{
			"schemas": []interface{}{"public", "myapp"},
			"tables":  []interface{}{"users", "orders", "payments"},
		},
	}

	conf := confmap.NewFromStringMap(configMap)
	cfg := &Config{}

	err := cfg.Unmarshal(conf)
	require.NoError(t, err)

	assert.NotNil(t, cfg.Relations)
	assert.Equal(t, []string{"public", "myapp"}, cfg.Relations.Schemas)
	assert.Equal(t, []string{"users", "orders", "payments"}, cfg.Relations.Tables)
}

func TestConfig_Unmarshal_WithSSLConfig(t *testing.T) {
	configMap := map[string]interface{}{
		"hostname":      "postgres.example.com",
		"port":          "5432",
		"username":      "app_user",
		"database":      "production",
		"ssl_mode":      "verify-full",
		"ssl_cert":      "/path/to/client-cert.pem",
		"ssl_key":       "/path/to/client-key.pem",
		"ssl_root_cert": "/path/to/ca.pem",
	}

	conf := confmap.NewFromStringMap(configMap)
	cfg := &Config{}

	err := cfg.Unmarshal(conf)
	require.NoError(t, err)

	assert.Equal(t, "postgres.example.com", cfg.Hostname)
	assert.Equal(t, "verify-full", cfg.SSLMode)
	assert.Equal(t, "/path/to/client-cert.pem", cfg.SSLCert)
	assert.Equal(t, "/path/to/client-key.pem", cfg.SSLKey)
	assert.Equal(t, "/path/to/ca.pem", cfg.SSLRootCert)
}

func TestConfig_Unmarshal_NilConf(t *testing.T) {
	cfg := &Config{}
	err := cfg.Unmarshal(nil)
	assert.NoError(t, err)
}

func TestConfig_Unmarshal_EmptyConf(t *testing.T) {
	conf := confmap.New()
	cfg := &Config{}

	err := cfg.Unmarshal(conf)
	// Should not error, but fields will be empty
	require.NoError(t, err)
}

func TestConfig_Unmarshal_InvalidData(t *testing.T) {
	// Test with invalid timeout format
	configMap := map[string]interface{}{
		"hostname": "localhost",
		"port":     "5432",
		"username": "postgres",
		"database": "testdb",
		"ssl_mode": "disable",
		"timeout":  "invalid-duration",
	}

	conf := confmap.NewFromStringMap(configMap)
	cfg := &Config{}

	err := cfg.Unmarshal(conf)
	assert.Error(t, err)
}

func TestRelationConfig_EmptySchemas(t *testing.T) {
	// Test that empty schemas is valid (will default to ["public"] in usage)
	cfg := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
		Relations: &RelationConfig{
			Schemas: []string{},
			Tables:  []string{"users"},
		},
	}

	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestRelationConfig_EmptyTables(t *testing.T) {
	// Test that empty tables is valid (no per-table metrics will be collected)
	cfg := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
		Relations: &RelationConfig{
			Schemas: []string{"public"},
			Tables:  []string{},
		},
	}

	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestRelationConfig_NilRelations(t *testing.T) {
	// Test that nil relations is valid (no per-table metrics)
	cfg := &Config{
		Hostname:  "localhost",
		Port:      "5432",
		Username:  "postgres",
		Database:  "postgres",
		SSLMode:   "disable",
		Relations: nil,
	}

	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestRelationConfig_MultipleSchemas(t *testing.T) {
	cfg := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
		Relations: &RelationConfig{
			Schemas: []string{"public", "app", "analytics", "audit"},
			Tables:  []string{"users", "events"},
		},
	}

	err := cfg.Validate()
	assert.NoError(t, err)
	assert.Len(t, cfg.Relations.Schemas, 4)
	assert.Len(t, cfg.Relations.Tables, 2)
}

func TestConfig_DefaultValues(t *testing.T) {
	// Test that Config can be created with zero values
	cfg := &Config{}

	// Should fail validation due to missing required fields
	err := cfg.Validate()
	assert.Error(t, err)

	// But the struct should be valid Go code
	assert.NotNil(t, cfg)
	assert.Equal(t, "", cfg.Hostname)
	assert.Equal(t, "", cfg.Port)
	assert.Equal(t, time.Duration(0), cfg.Timeout)
	assert.Nil(t, cfg.Relations)
}

func TestConfig_WithControllerConfig(t *testing.T) {
	cfg := &Config{
		ControllerConfig: scraperhelper.ControllerConfig{
			CollectionInterval: 15 * time.Second,
			InitialDelay:       time.Second,
			Timeout:            30 * time.Second,
		},
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}

	err := cfg.Validate()
	assert.NoError(t, err)
	assert.Equal(t, 15*time.Second, cfg.ControllerConfig.CollectionInterval)
}

func TestConfig_WithMetricsBuilderConfig(t *testing.T) {
	cfg := &Config{
		MetricsBuilderConfig: metadata.MetricsBuilderConfig{},
		Hostname:             "localhost",
		Port:                 "5432",
		Username:             "postgres",
		Database:             "postgres",
		SSLMode:              "disable",
	}

	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestConfig_PortFormats(t *testing.T) {
	tests := []struct {
		name string
		port string
	}{
		{"standard port", "5432"},
		{"custom port", "15432"},
		{"port 1", "1"},
		{"max port", "65535"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Hostname: "localhost",
				Port:     tt.port,
				Username: "postgres",
				Database: "postgres",
				SSLMode:  "disable",
			}

			err := cfg.Validate()
			assert.NoError(t, err)
		})
	}
}

func TestConfig_SpecialCharactersInFields(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		username string
		database string
		password string
	}{
		{
			name:     "special chars in password",
			hostname: "localhost",
			username: "postgres",
			database: "testdb",
			password: "p@ssw0rd!#$%",
		},
		{
			name:     "underscores in database name",
			hostname: "localhost",
			username: "postgres",
			database: "test_database_name",
			password: "password",
		},
		{
			name:     "hyphens in hostname",
			hostname: "postgres-primary.example.com",
			username: "app_user",
			database: "production",
			password: "password",
		},
		{
			name:     "underscores in username",
			hostname: "localhost",
			username: "app_user_readonly",
			database: "testdb",
			password: "password",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Hostname: tt.hostname,
				Port:     "5432",
				Username: tt.username,
				Database: tt.database,
				Password: tt.password,
				SSLMode:  "disable",
			}

			err := cfg.Validate()
			assert.NoError(t, err)
		})
	}
}

func TestConfig_Timeout_Values(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{"zero timeout", 0},
		{"1 second", time.Second},
		{"30 seconds", 30 * time.Second},
		{"1 minute", time.Minute},
		{"5 minutes", 5 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Hostname: "localhost",
				Port:     "5432",
				Username: "postgres",
				Database: "postgres",
				SSLMode:  "disable",
				Timeout:  tt.timeout,
			}

			err := cfg.Validate()
			assert.NoError(t, err)
			assert.Equal(t, tt.timeout, cfg.Timeout)
		})
	}
}

func TestConfig_EmptyPassword(t *testing.T) {
	// Empty password should be valid (trust authentication, .pgpass, etc.)
	cfg := &Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
		Password: "",
	}

	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestConfig_LongValues(t *testing.T) {
	// Test with long but valid values
	cfg := &Config{
		Hostname: "very-long-postgres-hostname-that-is-technically-valid.example.com",
		Port:     "5432",
		Username: "very_long_username_that_might_exist_in_some_systems",
		Database: "very_long_database_name_with_many_underscores_and_numbers_123",
		SSLMode:  "disable",
	}

	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestRelationConfig_Unmarshal(t *testing.T) {
	configMap := map[string]interface{}{
		"hostname": "localhost",
		"port":     "5432",
		"username": "postgres",
		"database": "testdb",
		"ssl_mode": "disable",
		"relations": map[string]interface{}{
			"schemas": []interface{}{"schema1", "schema2"},
			"tables":  []interface{}{"table1", "table2", "table3"},
		},
	}

	conf := confmap.NewFromStringMap(configMap)
	cfg := &Config{}

	err := cfg.Unmarshal(conf)
	require.NoError(t, err)

	require.NotNil(t, cfg.Relations)
	assert.Equal(t, 2, len(cfg.Relations.Schemas))
	assert.Equal(t, 3, len(cfg.Relations.Tables))
	assert.Contains(t, cfg.Relations.Schemas, "schema1")
	assert.Contains(t, cfg.Relations.Schemas, "schema2")
	assert.Contains(t, cfg.Relations.Tables, "table1")
	assert.Contains(t, cfg.Relations.Tables, "table2")
	assert.Contains(t, cfg.Relations.Tables, "table3")
}

func TestConfig_Unmarshal_PartialConfig(t *testing.T) {
	// Test unmarshaling with only some fields
	configMap := map[string]interface{}{
		"hostname": "localhost",
		"port":     "5432",
	}

	conf := confmap.NewFromStringMap(configMap)
	cfg := &Config{}

	err := cfg.Unmarshal(conf)
	require.NoError(t, err)

	assert.Equal(t, "localhost", cfg.Hostname)
	assert.Equal(t, "5432", cfg.Port)
	assert.Equal(t, "", cfg.Username) // Should be empty, not error
	assert.Equal(t, "", cfg.Database)
}

func TestConfig_ValidateBeforeUnmarshal(t *testing.T) {
	// Test that validation before unmarshal fails appropriately
	cfg := &Config{
		Hostname: "localhost",
		// Missing other required fields
	}

	err := cfg.Validate()
	assert.Error(t, err)
}

func TestConfig_FullConfiguration(t *testing.T) {
	// Test a fully populated configuration
	cfg := &Config{
		ControllerConfig: scraperhelper.ControllerConfig{
			CollectionInterval: 15 * time.Second,
			InitialDelay:       time.Second,
			Timeout:            30 * time.Second,
		},
		MetricsBuilderConfig: metadata.MetricsBuilderConfig{},
		Hostname:             "postgres-primary.prod.example.com",
		Port:                 "5432",
		Username:             "monitoring_user",
		Password:             "secure_password_123!",
		Database:             "production_database",
		SSLMode:              "verify-full",
		SSLCert:              "/etc/ssl/certs/client-cert.pem",
		SSLKey:               "/etc/ssl/private/client-key.pem",
		SSLRootCert:          "/etc/ssl/certs/ca-cert.pem",
		Timeout:              30 * time.Second,
		Relations: &RelationConfig{
			Schemas: []string{"public", "app", "analytics"},
			Tables:  []string{"users", "orders", "payments", "events"},
		},
	}

	err := cfg.Validate()
	assert.NoError(t, err)

	// Verify all fields are set as expected
	assert.Equal(t, "postgres-primary.prod.example.com", cfg.Hostname)
	assert.Equal(t, "5432", cfg.Port)
	assert.Equal(t, "monitoring_user", cfg.Username)
	assert.Equal(t, "production_database", cfg.Database)
	assert.Equal(t, "verify-full", cfg.SSLMode)
	assert.Equal(t, 30*time.Second, cfg.Timeout)
	assert.NotNil(t, cfg.Relations)
	assert.Len(t, cfg.Relations.Schemas, 3)
	assert.Len(t, cfg.Relations.Tables, 4)
}
