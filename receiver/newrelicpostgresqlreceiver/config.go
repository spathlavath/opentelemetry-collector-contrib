// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicpostgresqlreceiver

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
)

// Config represents the receiver config settings within the collector's config.yaml
type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`
	metadata.MetricsBuilderConfig  `mapstructure:",squash"`

	// Connection configuration
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Hostname string `mapstructure:"hostname"`
	Port     string `mapstructure:"port"`
	Database string `mapstructure:"database"`

	// SSL configuration
	SSLMode     string `mapstructure:"ssl_mode"`      // disable, require, verify-ca, verify-full
	SSLCert     string `mapstructure:"ssl_cert"`      // Path to SSL certificate
	SSLKey      string `mapstructure:"ssl_key"`       // Path to SSL key
	SSLRootCert string `mapstructure:"ssl_root_cert"` // Path to SSL root certificate
	SSLPassword string `mapstructure:"ssl_password"`  // SSL certificate password

	// Timeout for database operations
	Timeout time.Duration `mapstructure:"timeout"`
}

// Validate checks if the receiver configuration is valid
func (cfg *Config) Validate() error {
	if cfg.Hostname == "" {
		return errors.New("hostname must be specified")
	}

	if cfg.Port == "" {
		return errors.New("port must be specified")
	}

	if cfg.Username == "" {
		return errors.New("username must be specified")
	}

	if cfg.Database == "" {
		return errors.New("database must be specified")
	}

	// Validate SSL mode
	validSSLModes := map[string]bool{
		"disable":     true,
		"require":     true,
		"verify-ca":   true,
		"verify-full": true,
	}

	if !validSSLModes[cfg.SSLMode] {
		return fmt.Errorf("invalid ssl_mode: %s. Valid values are: disable, require, verify-ca, verify-full", cfg.SSLMode)
	}

	// Timeout is optional, no validation needed

	return nil
}

// Unmarshal handles custom unmarshaling logic for the config
func (cfg *Config) Unmarshal(componentParser *confmap.Conf) error {
	if componentParser == nil {
		return nil
	}

	// Unmarshal the base config
	if err := componentParser.Unmarshal(cfg); err != nil {
		return err
	}

	return nil
}
