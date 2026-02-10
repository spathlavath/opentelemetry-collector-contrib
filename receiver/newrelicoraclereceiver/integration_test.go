// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package newrelicoraclereceiver // import "github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver"

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/internal/metadata"
)

func TestNewRelicOracleReceiverIntegration(t *testing.T) {
	t.Skip("Skipping integration test - requires Oracle database")

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)

	// Use real Oracle connection for integration testing
	cfg.DataSource = "oracle://testuser:testpass@localhost:1521/XE"

	set := receivertest.NewNopSettings(metadata.Type)
	consumer := consumertest.NewNop()

	// Create receiver
	receiver, err := factory.CreateMetrics(t.Context(), set, cfg, consumer)

	require.NoError(t, err)
	require.NotNil(t, receiver)

	// Test start and shutdown
	err = receiver.Start(t.Context(), componenttest.NewNopHost())
	assert.NoError(t, err)

	// Let it run briefly
	time.Sleep(100 * time.Millisecond)

	err = receiver.Shutdown(t.Context())
	assert.NoError(t, err)
}
