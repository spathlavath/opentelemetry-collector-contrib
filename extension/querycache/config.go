// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package querycache // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/querycache"

// Config defines configuration for the query cache extension
type Config struct {
	// No configuration options needed - extension manages cache internally
}

// Validate checks if the extension configuration is valid
func (cfg *Config) Validate() error {
	return nil
}
