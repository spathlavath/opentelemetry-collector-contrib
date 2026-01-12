// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package querycache // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/querycache"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/querycache/internal/metadata"
)

// NewFactory creates a factory for the query cache extension
func NewFactory() extension.Factory {
	return extension.NewFactory(
		component.MustNewType(metadata.Type),
		createDefaultConfig,
		createExtension,
		metadata.ExtensionStability,
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createExtension(
	_ context.Context,
	params extension.Settings,
	_ component.Config,
) (extension.Extension, error) {
	return NewExtension(params.TelemetrySettings), nil
}
