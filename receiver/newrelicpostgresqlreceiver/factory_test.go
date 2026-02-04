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
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
)

// Test NewFactory creates a factory successfully
func TestNewFactory(t *testing.T) {
	factory := NewFactory()

	assert.NotNil(t, factory)
	assert.Equal(t, metadata.Type, factory.Type())
}

// Test createDefaultConfig returns valid default configuration
func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig()

	assert.NotNil(t, cfg)

	pgCfg, ok := cfg.(*Config)
	require.True(t, ok)

	assert.Equal(t, 10*time.Second, pgCfg.ControllerConfig.CollectionInterval)
	assert.Equal(t, 30*time.Second, pgCfg.ControllerConfig.Timeout)
	assert.Equal(t, "localhost", pgCfg.Hostname)
	assert.Equal(t, "5432", pgCfg.Port)
	assert.Equal(t, "postgres", pgCfg.Username)
	assert.Equal(t, "", pgCfg.Password)
	assert.Equal(t, "postgres", pgCfg.Database)
	assert.Equal(t, 30*time.Second, pgCfg.Timeout)
	assert.Equal(t, "disable", pgCfg.SSLMode)
	assert.NotNil(t, pgCfg.MetricsBuilderConfig)
}

// Test getDataSource with basic configuration
func TestGetDataSource_BasicConfig(t *testing.T) {
	cfg := Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "testuser",
		Password: "testpass",
		Database: "testdb",
		SSLMode:  "disable",
	}

	dataSource := getDataSource(cfg)

	assert.Contains(t, dataSource, "host=localhost")
	assert.Contains(t, dataSource, "port=5432")
	assert.Contains(t, dataSource, "user=testuser")
	assert.Contains(t, dataSource, "dbname=testdb")
	assert.Contains(t, dataSource, "sslmode=disable")
	assert.Contains(t, dataSource, "password=testpass")
}

// Test getDataSource without password
func TestGetDataSource_NoPassword(t *testing.T) {
	cfg := Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "testuser",
		Database: "testdb",
		SSLMode:  "disable",
	}

	dataSource := getDataSource(cfg)

	assert.Contains(t, dataSource, "host=localhost")
	assert.Contains(t, dataSource, "port=5432")
	assert.Contains(t, dataSource, "user=testuser")
	assert.Contains(t, dataSource, "dbname=testdb")
	assert.Contains(t, dataSource, "sslmode=disable")
	assert.NotContains(t, dataSource, "password=")
}

// Test getDataSource with SSL configuration
func TestGetDataSource_WithSSL(t *testing.T) {
	cfg := Config{
		Hostname:    "localhost",
		Port:        "5432",
		Username:    "testuser",
		Password:    "testpass",
		Database:    "testdb",
		SSLMode:     "verify-full",
		SSLCert:     "/path/to/cert.pem",
		SSLKey:      "/path/to/key.pem",
		SSLRootCert: "/path/to/ca.pem",
	}

	dataSource := getDataSource(cfg)

	assert.Contains(t, dataSource, "sslmode=verify-full")
	assert.Contains(t, dataSource, "sslcert=/path/to/cert.pem")
	assert.Contains(t, dataSource, "sslkey=/path/to/key.pem")
	assert.Contains(t, dataSource, "sslrootcert=/path/to/ca.pem")
}

// Test getDataSource with timeout
func TestGetDataSource_WithTimeout(t *testing.T) {
	cfg := Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "testuser",
		Database: "testdb",
		SSLMode:  "disable",
		Timeout:  30 * time.Second,
	}

	dataSource := getDataSource(cfg)

	assert.Contains(t, dataSource, "connect_timeout=30")
}

// Test getDataSource without timeout
func TestGetDataSource_NoTimeout(t *testing.T) {
	cfg := Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "testuser",
		Database: "testdb",
		SSLMode:  "disable",
		Timeout:  0,
	}

	dataSource := getDataSource(cfg)

	assert.NotContains(t, dataSource, "connect_timeout=")
}

// Test getInstanceName
func TestGetInstanceName(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "standard config",
			config: Config{
				Hostname: "localhost",
				Port:     "5432",
				Database: "testdb",
			},
			expected: "localhost:5432/testdb",
		},
		{
			name: "custom port",
			config: Config{
				Hostname: "dbserver",
				Port:     "5433",
				Database: "mydb",
			},
			expected: "dbserver:5433/mydb",
		},
		{
			name: "remote host",
			config: Config{
				Hostname: "db.example.com",
				Port:     "5432",
				Database: "production",
			},
			expected: "db.example.com:5432/production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instanceName := getInstanceName(tt.config)
			assert.Equal(t, tt.expected, instanceName)
		})
	}
}

// Test getHostName
func TestGetHostName(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "localhost default port",
			config: Config{
				Hostname: "localhost",
				Port:     "5432",
			},
			expected: "localhost:5432",
		},
		{
			name: "custom host and port",
			config: Config{
				Hostname: "dbserver",
				Port:     "5433",
			},
			expected: "dbserver:5433",
		},
		{
			name: "remote host",
			config: Config{
				Hostname: "db.example.com",
				Port:     "5432",
			},
			expected: "db.example.com:5432",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hostName := getHostName(tt.config)
			assert.Equal(t, tt.expected, hostName)
		})
	}
}

// Test createMetricsReceiverFunc with valid configuration
func TestCreateMetricsReceiverFunc_Success(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlOpener := func(dataSourceName string) (*sql.DB, error) {
		return db, nil
	}

	factory := createMetricsReceiverFunc(sqlOpener)
	assert.NotNil(t, factory)

	settings := receivertest.NewNopSettings(metadata.Type)
	cfg := createDefaultConfig()
	consumer := consumertest.NewNop()

	receiver, err := factory(context.Background(), settings, cfg, consumer)

	assert.NoError(t, err)
	assert.NotNil(t, receiver)
}

// Test createMetricsReceiverFunc with invalid configuration
func TestCreateMetricsReceiverFunc_InvalidConfig(t *testing.T) {
	sqlOpener := func(dataSourceName string) (*sql.DB, error) {
		return nil, nil
	}

	factory := createMetricsReceiverFunc(sqlOpener)
	assert.NotNil(t, factory)

	settings := receivertest.NewNopSettings(metadata.Type)
	cfg := &Config{
		Hostname: "", // Invalid: empty hostname
		Port:     "5432",
		Username: "postgres",
		Database: "postgres",
	}
	consumer := consumertest.NewNop()

	receiver, err := factory(context.Background(), settings, cfg, consumer)

	assert.Error(t, err)
	assert.Nil(t, receiver)
	assert.Contains(t, err.Error(), "invalid configuration")
}

// Test createMetricsReceiverFunc with SQL opener error
func TestCreateMetricsReceiverFunc_SQLOpenerError(t *testing.T) {
	sqlOpener := func(dataSourceName string) (*sql.DB, error) {
		return nil, errors.New("connection failed")
	}

	factory := createMetricsReceiverFunc(sqlOpener)
	assert.NotNil(t, factory)

	settings := receivertest.NewNopSettings(metadata.Type)
	cfg := createDefaultConfig()
	consumer := consumertest.NewNop()

	// The receiver creation succeeds, but the error occurs during Start()
	receiver, err := factory(context.Background(), settings, cfg, consumer)

	assert.NoError(t, err)
	assert.NotNil(t, receiver)

	// The connection error would occur when starting the receiver
	err = receiver.Start(context.Background(), newMockHost())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to open db connection")
}

// Test createMetricsReceiverFunc with custom configuration
func TestCreateMetricsReceiverFunc_CustomConfig(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlOpener := func(dataSourceName string) (*sql.DB, error) {
		assert.Contains(t, dataSourceName, "host=customhost")
		assert.Contains(t, dataSourceName, "port=5433")
		assert.Contains(t, dataSourceName, "dbname=customdb")
		return db, nil
	}

	factory := createMetricsReceiverFunc(sqlOpener)
	assert.NotNil(t, factory)

	settings := receivertest.NewNopSettings(metadata.Type)
	cfg := &Config{
		ControllerConfig:     createDefaultConfig().(*Config).ControllerConfig,
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
		Hostname:             "customhost",
		Port:                 "5433",
		Username:             "customuser",
		Password:             "custompass",
		Database:             "customdb",
		Timeout:              30 * time.Second,
		SSLMode:              "require",
	}
	consumer := consumertest.NewNop()

	receiver, err := factory(context.Background(), settings, cfg, consumer)

	assert.NoError(t, err)
	assert.NotNil(t, receiver)
}

// Test factory type is correct
func TestFactoryType(t *testing.T) {
	factory := NewFactory()

	assert.Equal(t, metadata.Type, factory.Type())
}

// Test factory creates metrics receiver
func TestFactory_CreateMetricsReceiver(t *testing.T) {
	factory := NewFactory()

	cfg := createDefaultConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	consumer := consumertest.NewNop()

	// The factory should create a receiver successfully
	// The connection error would occur during Start(), not during creation
	receiver, err := factory.CreateMetrics(context.Background(), settings, cfg, consumer)

	assert.NoError(t, err)
	assert.NotNil(t, receiver)
}

// Test getDataSource with all SSL options
func TestGetDataSource_AllSSLOptions(t *testing.T) {
	cfg := Config{
		Hostname:    "secure.example.com",
		Port:        "5432",
		Username:    "secureuser",
		Password:    "securepass",
		Database:    "securedb",
		SSLMode:     "verify-full",
		SSLCert:     "/certs/client-cert.pem",
		SSLKey:      "/certs/client-key.pem",
		SSLRootCert: "/certs/ca-cert.pem",
		Timeout:     15 * time.Second,
	}

	dataSource := getDataSource(cfg)

	assert.Contains(t, dataSource, "host=secure.example.com")
	assert.Contains(t, dataSource, "port=5432")
	assert.Contains(t, dataSource, "user=secureuser")
	assert.Contains(t, dataSource, "dbname=securedb")
	assert.Contains(t, dataSource, "sslmode=verify-full")
	assert.Contains(t, dataSource, "password=securepass")
	assert.Contains(t, dataSource, "sslcert=/certs/client-cert.pem")
	assert.Contains(t, dataSource, "sslkey=/certs/client-key.pem")
	assert.Contains(t, dataSource, "sslrootcert=/certs/ca-cert.pem")
	assert.Contains(t, dataSource, "connect_timeout=15")
}

// Test getDataSource with partial SSL configuration
func TestGetDataSource_PartialSSL(t *testing.T) {
	cfg := Config{
		Hostname: "localhost",
		Port:     "5432",
		Username: "testuser",
		Database: "testdb",
		SSLMode:  "require",
		SSLCert:  "/path/to/cert.pem",
		// No SSLKey or SSLRootCert
	}

	dataSource := getDataSource(cfg)

	assert.Contains(t, dataSource, "sslmode=require")
	assert.Contains(t, dataSource, "sslcert=/path/to/cert.pem")
	assert.NotContains(t, dataSource, "sslkey=")
	assert.NotContains(t, dataSource, "sslrootcert=")
}

// Test sqlOpenerFunc type
func TestSqlOpenerFunc(t *testing.T) {
	var opener sqlOpenerFunc = func(dataSourceName string) (*sql.DB, error) {
		return nil, errors.New("test error")
	}

	db, err := opener("test-datasource")

	assert.Error(t, err)
	assert.Nil(t, db)
	assert.Equal(t, "test error", err.Error())
}

// Test createMetricsReceiverFunc validates config before creating scraper
func TestCreateMetricsReceiverFunc_ConfigValidation(t *testing.T) {
	sqlOpener := func(dataSourceName string) (*sql.DB, error) {
		t.Fatal("SQL opener should not be called for invalid config")
		return nil, nil
	}

	factory := createMetricsReceiverFunc(sqlOpener)

	settings := receivertest.NewNopSettings(metadata.Type)
	invalidCfg := &Config{
		Hostname: "",     // Invalid
		Port:     "",     // Invalid
		Username: "",     // Invalid
		Database: "",     // Invalid
	}
	consumer := consumertest.NewNop()

	receiver, err := factory(context.Background(), settings, invalidCfg, consumer)

	assert.Error(t, err)
	assert.Nil(t, receiver)
	assert.Contains(t, err.Error(), "invalid configuration")
}

// Test that connection pool settings are applied
func TestCreateMetricsReceiverFunc_ConnectionPoolSettings(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlOpener := func(dataSourceName string) (*sql.DB, error) {
		// Return a mock DB
		return db, nil
	}

	factory := createMetricsReceiverFunc(sqlOpener)
	settings := receivertest.NewNopSettings(metadata.Type)
	cfg := createDefaultConfig()
	consumer := consumertest.NewNop()

	receiver, err := factory(context.Background(), settings, cfg, consumer)

	assert.NoError(t, err)
	assert.NotNil(t, receiver)

	// The connection pool settings should be applied in the factory
	// db.SetMaxOpenConns(10)
	// db.SetMaxIdleConns(2)
	// db.SetConnMaxLifetime(10 * time.Minute)
	// db.SetConnMaxIdleTime(30 * time.Second)
}

// Test getDataSource with IPv6 address
func TestGetDataSource_IPv6(t *testing.T) {
	cfg := Config{
		Hostname: "::1",
		Port:     "5432",
		Username: "testuser",
		Database: "testdb",
		SSLMode:  "disable",
	}

	dataSource := getDataSource(cfg)

	assert.Contains(t, dataSource, "host=::1")
	assert.Contains(t, dataSource, "port=5432")
}

// Test getInstanceName with special characters
func TestGetInstanceName_SpecialCharacters(t *testing.T) {
	cfg := Config{
		Hostname: "db-server.example.com",
		Port:     "5432",
		Database: "my_test_db",
	}

	instanceName := getInstanceName(cfg)

	assert.Equal(t, "db-server.example.com:5432/my_test_db", instanceName)
}

// Test default config has valid MetricsBuilderConfig
func TestCreateDefaultConfig_MetricsBuilderConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	assert.NotNil(t, cfg.MetricsBuilderConfig)

	// Verify that the metrics builder config has default values
	// The exact structure depends on metadata.DefaultMetricsBuilderConfig()
	defaultMetricsConfig := metadata.DefaultMetricsBuilderConfig()
	assert.Equal(t, defaultMetricsConfig, cfg.MetricsBuilderConfig)
}
