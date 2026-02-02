// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver"

import (
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

// Config defines configuration for New Relic MySQL receiver.
type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`
	Username                       string              `mapstructure:"username,omitempty"`
	Password                       configopaque.String `mapstructure:"password,omitempty"`
	Database                       string              `mapstructure:"database,omitempty"`
	AllowNativePasswords           bool                `mapstructure:"allow_native_passwords,omitempty"`
	ExtraStatusMetrics             bool                `mapstructure:"extra_status_metrics,omitempty"`
	confignet.AddrConfig           `mapstructure:",squash"`
	TLS                            configtls.ClientConfig `mapstructure:"tls,omitempty"`

	// Optional metric collection flags
	ExtraInnoDBMetrics bool `mapstructure:"extra_innodb_metrics,omitempty"`

	MetricsBuilderConfig metadata.MetricsBuilderConfig `mapstructure:",squash"`
}

func (cfg *Config) Unmarshal(componentParser *confmap.Conf) error {
	if componentParser == nil {
		return nil
	}

	// Default to insecure TLS
	if !componentParser.IsSet("tls") {
		cfg.TLS = configtls.ClientConfig{}
		cfg.TLS.Insecure = true
	}

	return componentParser.Unmarshal(cfg)
}
