// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectedErr string
	}{
		{
			name: "valid config",
			config: Config{
				ControllerConfig: scraperhelper.ControllerConfig{
					CollectionInterval: 60 * time.Second,
					InitialDelay:       time.Second,
					Timeout:            30 * time.Second,
				},
				Endpoint:       "root:password@tcp(localhost:3306)/mysql",
				Username:       "root",
				ConnectTimeout: 10 * time.Second,
				ReadTimeout:    30 * time.Second,
				WriteTimeout:   30 * time.Second,
			},
			expectedErr: "",
		},
		{
			name: "missing endpoint",
			config: Config{
				ControllerConfig: scraperhelper.ControllerConfig{
					CollectionInterval: 60 * time.Second,
					InitialDelay:       time.Second,
					Timeout:            30 * time.Second,
				},
				Username:       "root",
				ConnectTimeout: 10 * time.Second,
				ReadTimeout:    30 * time.Second,
				WriteTimeout:   30 * time.Second,
			},
			expectedErr: "endpoint is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectedErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			}
		})
	}
}

func TestConfigDefaultValues(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)

	// Test that config has expected default values
	assert.Equal(t, "tcp", cfg.Transport)
	assert.Equal(t, 10*time.Second, cfg.ConnectTimeout)
	assert.Equal(t, 30*time.Second, cfg.ReadTimeout)
	assert.Equal(t, 30*time.Second, cfg.WriteTimeout)
	assert.True(t, cfg.QueryPerformanceConfig.Enabled)
	assert.True(t, cfg.WaitEventsConfig.Enabled)
	assert.True(t, cfg.BlockingSessionsConfig.Enabled)
	assert.True(t, cfg.ReplicationConfig.Enabled)
	assert.True(t, cfg.SlowQueryConfig.Enabled)
}
