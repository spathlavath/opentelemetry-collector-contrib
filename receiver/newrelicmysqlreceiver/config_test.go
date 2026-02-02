// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver

import (
	"testing"

	"github.com/stretchr/testify/require"
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
