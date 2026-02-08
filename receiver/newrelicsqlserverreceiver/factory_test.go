// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicsqlserverreceiver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/internal/metadata"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	require.NotNil(t, factory)
	assert.Equal(t, metadata.Type, factory.Type())
}

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")

	sqlCfg, ok := cfg.(*Config)
	require.True(t, ok, "config should be of type *Config")

	// Verify default values
	assert.Equal(t, "localhost", sqlCfg.Hostname)
	assert.Equal(t, "1433", sqlCfg.Port)
	assert.Equal(t, "", sqlCfg.Username)
	assert.Equal(t, "", sqlCfg.Password)
	assert.Equal(t, 5, sqlCfg.MaxConcurrentWorkers)
	assert.Equal(t, 30*time.Second, sqlCfg.Timeout)
	assert.Equal(t, 10*time.Second, sqlCfg.ControllerConfig.CollectionInterval)

	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestCreateMetricsReceiver(t *testing.T) {
	factory := NewFactory()

	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				Hostname:             "localhost",
				Port:                 "1433",
				Username:             "sa",
				Password:             "password",
				MaxConcurrentWorkers: 5,
				Timeout:              30 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "minimal config",
			config: &Config{
				Hostname:             "localhost",
				Port:                 "1433",
				MaxConcurrentWorkers: 1,
				Timeout:              10 * time.Second,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			receiver, err := factory.CreateMetrics(
				context.Background(),
				receivertest.NewNopSettings(metadata.Type),
				tt.config,
				consumertest.NewNop(),
			)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, receiver)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, receiver)
			}
		})
	}
}

func TestCreateLogsReceiver(t *testing.T) {
	factory := NewFactory()

	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				Hostname:             "localhost",
				Port:                 "1433",
				Username:             "sa",
				Password:             "password",
				MaxConcurrentWorkers: 5,
				Timeout:              30 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "minimal config",
			config: &Config{
				Hostname:             "localhost",
				Port:                 "1433",
				MaxConcurrentWorkers: 1,
				Timeout:              10 * time.Second,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			receiver, err := factory.CreateLogs(
				context.Background(),
				receivertest.NewNopSettings(metadata.Type),
				tt.config,
				consumertest.NewNop(),
			)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, receiver)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, receiver)
			}
		})
	}
}

func TestFactoryMetricsStability(t *testing.T) {
	factory := NewFactory()

	// The metrics receiver should have defined stability level
	assert.NotNil(t, factory)

	// Verify we can create a metrics receiver with default config
	cfg := factory.CreateDefaultConfig()
	receiver, err := factory.CreateMetrics(
		context.Background(),
		receivertest.NewNopSettings(metadata.Type),
		cfg,
		consumertest.NewNop(),
	)

	assert.NoError(t, err)
	assert.NotNil(t, receiver)
}

func TestFactoryLogsStability(t *testing.T) {
	factory := NewFactory()

	// The logs receiver should have development stability level
	assert.NotNil(t, factory)

	// Verify we can create a logs receiver with default config
	cfg := factory.CreateDefaultConfig()
	receiver, err := factory.CreateLogs(
		context.Background(),
		receivertest.NewNopSettings(metadata.Type),
		cfg,
		consumertest.NewNop(),
	)

	assert.NoError(t, err)
	assert.NotNil(t, receiver)
}

func TestFactoryType(t *testing.T) {
	factory := NewFactory()

	// Verify factory type matches metadata type
	assert.Equal(t, metadata.Type, factory.Type())
	assert.Equal(t, component.MustNewType("newrelicsqlserver"), factory.Type())
}
