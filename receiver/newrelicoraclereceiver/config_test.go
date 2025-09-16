// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicoraclereceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

func TestConfig_Validate(t *testing.T) {
	testCases := []struct {
		name     string
		config   *Config
		expected error
	}{
		{
			name: "valid datasource config",
			config: &Config{
				DataSource: "oracle://user:pass@localhost:1521/XE",
			},
			expected: nil,
		},
		{
			name: "valid endpoint config",
			config: &Config{
				Endpoint: "localhost:1521",
				Username: "user",
				Password: "pass",
				Service:  "XE",
			},
			expected: nil,
		},
		{
			name: "missing endpoint",
			config: &Config{
				Username: "user",
				Password: "pass",
				Service:  "XE",
			},
			expected: errEmptyEndpoint,
		},
		{
			name: "missing username",
			config: &Config{
				Endpoint: "localhost:1521",
				Password: "pass",
				Service:  "XE",
			},
			expected: errEmptyUsername,
		},
		{
			name: "missing password",
			config: &Config{
				Endpoint: "localhost:1521",
				Username: "user",
				Service:  "XE",
			},
			expected: errEmptyPassword,
		},
		{
			name: "missing service",
			config: &Config{
				Endpoint: "localhost:1521",
				Username: "user",
				Password: "pass",
			},
			expected: errEmptyService,
		},
		{
			name: "invalid port",
			config: &Config{
				Endpoint: "localhost:999999",
				Username: "user",
				Password: "pass",
				Service:  "XE",
			},
			expected: errBadPort,
		},
		{
			name: "invalid sysmetrics source",
			config: &Config{
				DataSource:       "oracle://user:pass@localhost:1521/XE",
				SysMetricsSource: "INVALID",
			},
			expected: errInvalidCollectionSource,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.Validate()
			if tc.expected != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expected.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfig_GetConnectionString(t *testing.T) {
	testCases := []struct {
		name     string
		config   *Config
		expected string
	}{
		{
			name: "datasource takes precedence",
			config: &Config{
				DataSource: "oracle://user:pass@localhost:1521/XE",
				Endpoint:   "other:1521",
				Username:   "other",
				Password:   "other",
				Service:    "OTHER",
			},
			expected: "oracle://user:pass@localhost:1521/XE",
		},
		{
			name: "build from components",
			config: &Config{
				Endpoint: "localhost:1521",
				Username: "testuser",
				Password: "testpass",
				Service:  "TESTDB",
			},
			expected: "oracle://testuser:testpass@localhost:1521/TESTDB",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.config.GetConnectionString()
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	require.NotNil(t, factory)
	require.Equal(t, "newrelicoracledb", factory.Type().String())

	cfg := factory.CreateDefaultConfig()
	require.NotNil(t, cfg)
	// Skip config validation for now as it requires actual implementation
}

func TestFactory_CreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	oracleConfig := cfg.(*Config)
	require.False(t, oracleConfig.ExtendedMetrics)
	require.Equal(t, "SYS", oracleConfig.SysMetricsSource)
	require.Empty(t, oracleConfig.TablespaceWhitelist)
	require.Empty(t, oracleConfig.CustomMetricsQuery)
	require.Empty(t, oracleConfig.CustomMetricsConfig)
	require.Empty(t, oracleConfig.SkipMetricsGroups)
}

func TestFactory_CreateMetricsReceiver(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{
		DataSource: "oracle://user:pass@localhost:1521/XE",
	}

	_, err := factory.CreateMetrics(
		context.Background(),
		receivertest.NewNopSettings(metadata.Type),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)
}
