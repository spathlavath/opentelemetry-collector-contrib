// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package oraclesqlidentifierconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/oraclesqlidentifierconnector"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/connector/xconnector"
	"go.opentelemetry.io/collector/consumer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/oraclesqlidentifierconnector/internal/metadata"
)

// NewFactory returns a ConnectorFactory.
func NewFactory() connector.Factory {
	return xconnector.NewFactory(
		metadata.Type,
		createDefaultConfig,
		xconnector.WithMetricsToLogs(createMetricsToLogs, metadata.MetricsToLogsStability),
	)
}

// createDefaultConfig creates the default configuration.
func createDefaultConfig() component.Config {
	return &Config{}
}

// createMetricsToLogs creates a metrics to logs connector based on provided config.
func createMetricsToLogs(
	_ context.Context,
	set connector.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (connector.Metrics, error) {
	c := cfg.(*Config)
	return &oracleConnector{
		logsConsumer: nextConsumer,
		config:       c,
		logger:       set.Logger,
	}, nil
}
