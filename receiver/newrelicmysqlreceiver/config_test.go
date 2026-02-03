// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

func TestConfig(t *testing.T) {
	cfg := &Config{}
	require.NotNil(t, cfg)
}

func TestConfigExtraStatusMetrics(t *testing.T) {
	t.Run("default_false", func(t *testing.T) {
		cfg := &Config{}
		require.False(t, cfg.ExtraStatusMetrics, "ExtraStatusMetrics should default to false")
	})

	t.Run("enabled", func(t *testing.T) {
		cfg := &Config{ExtraStatusMetrics: true}
		require.True(t, cfg.ExtraStatusMetrics, "ExtraStatusMetrics should be enabled")
	})
}

func TestConfig_ExtraInnoDBMetrics(t *testing.T) {
	tests := []struct {
		name     string
		config   map[string]interface{}
		expected bool
	}{
		{
			name:     "set_to_true",
			config:   map[string]interface{}{"extra_innodb_metrics": true},
			expected: true,
		},
		{
			name:     "set_to_false",
			config:   map[string]interface{}{"extra_innodb_metrics": false},
			expected: false,
		},
		{
			name:     "not_set_defaults_to_false",
			config:   map[string]interface{}{"endpoint": "localhost:3306"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{}
			err := cfg.Unmarshal(confmap.NewFromStringMap(tt.config))
			require.NoError(t, err)
			assert.Equal(t, tt.expected, cfg.ExtraInnoDBMetrics)
		})
	}
}

func TestConfig_Unmarshal_NilParser(t *testing.T) {
	cfg := &Config{}
	err := cfg.Unmarshal(nil)
	require.NoError(t, err)
}
