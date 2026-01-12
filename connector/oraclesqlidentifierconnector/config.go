// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package oraclesqlidentifierconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/oraclesqlidentifierconnector"

// Config represents the receiver config settings within the collector's config.yaml
type Config struct {
	// SQLIdentifierAttribute is the attribute name where SQL identifiers are stored in metrics
	SQLIdentifierAttribute string `mapstructure:"sql_identifier_attribute"`

	// SQLIDAttribute is the attribute name for SQL ID (default: "query_id")
	SQLIDAttribute string `mapstructure:"sql_id_attribute"`

	// ChildAddressAttribute is the attribute name for child address
	ChildAddressAttribute string `mapstructure:"child_address_attribute"`

	// ChildNumberAttribute is the attribute name for child number (default: "sql_child_number")
	ChildNumberAttribute string `mapstructure:"child_number_attribute"`
}

// Validate checks the receiver configuration is valid
func (cfg *Config) Validate() error {
	return nil
}
