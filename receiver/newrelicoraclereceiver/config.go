// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicoraclereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver"

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/multierr"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

var (
	errBadDataSource           = errors.New("datasource is invalid")
	errBadEndpoint             = errors.New("endpoint must be specified as host:port")
	errBadPort                 = errors.New("invalid port in endpoint")
	errEmptyEndpoint           = errors.New("endpoint must be specified")
	errEmptyPassword           = errors.New("password must be set")
	errEmptyService            = errors.New("service must be specified")
	errEmptyUsername           = errors.New("username must be set")
	errInvalidCollectionSource = errors.New("sysmetrics_source must be 'SYS', 'PDB', or 'ALL'")
)

// Config represents the receiver config settings within the collector's config.yaml
type Config struct {
	DataSource                     string        `mapstructure:"datasource"`
	Endpoint                       string        `mapstructure:"endpoint"`
	Password                       string        `mapstructure:"password"`
	Service                        string        `mapstructure:"service"`
	Username                       string        `mapstructure:"username"`
	Timeout                        time.Duration `mapstructure:"timeout"`
	ExtendedMetrics                bool          `mapstructure:"extended_metrics"`
	SysMetricsSource               string        `mapstructure:"sysmetrics_source"`
	TablespaceWhitelist            []string      `mapstructure:"tablespace_whitelist"`
	CustomMetricsQuery             string        `mapstructure:"custom_metrics_query"`
	CustomMetricsConfig            string        `mapstructure:"custom_metrics_config"`
	SkipMetricsGroups              []string      `mapstructure:"skip_metrics_groups"`
	scraperhelper.ControllerConfig `mapstructure:",squash"`
	metadata.MetricsBuilderConfig  `mapstructure:",squash"`
}

// Validate checks the receiver configuration is valid
func (cfg *Config) Validate() error {
	var allErrs error

	// If DataSource is defined it takes precedence over the rest of the connection options.
	if cfg.DataSource == "" {
		if cfg.Endpoint == "" {
			allErrs = multierr.Append(allErrs, errEmptyEndpoint)
		}

		host, portStr, err := net.SplitHostPort(cfg.Endpoint)
		if err != nil {
			return multierr.Append(allErrs, fmt.Errorf("%w: %s", errBadEndpoint, err.Error()))
		}

		if host == "" {
			allErrs = multierr.Append(allErrs, errBadEndpoint)
		}

		port, err := strconv.ParseInt(portStr, 10, 32)
		if err != nil {
			allErrs = multierr.Append(allErrs, fmt.Errorf("%w: %s", errBadPort, err.Error()))
		}

		if port < 0 || port > 65535 {
			allErrs = multierr.Append(allErrs, fmt.Errorf("%w: %d", errBadPort, port))
		}

		if cfg.Username == "" {
			allErrs = multierr.Append(allErrs, errEmptyUsername)
		}

		if cfg.Password == "" {
			allErrs = multierr.Append(allErrs, errEmptyPassword)
		}

		if cfg.Service == "" {
			allErrs = multierr.Append(allErrs, errEmptyService)
		}
	} else {
		if _, err := url.Parse(cfg.DataSource); err != nil {
			allErrs = multierr.Append(allErrs, fmt.Errorf("%w: %s", errBadDataSource, err.Error()))
		}
	}

	// Validate sysmetrics source
	if cfg.SysMetricsSource != "" {
		source := cfg.SysMetricsSource
		if source != "SYS" && source != "PDB" && source != "ALL" {
			allErrs = multierr.Append(allErrs, errInvalidCollectionSource)
		}
	}

	return allErrs
}

// GetConnectionString returns the connection string for Oracle DB
func (cfg *Config) GetConnectionString() string {
	if cfg.DataSource != "" {
		return cfg.DataSource
	}

	// Build connection string from individual components
	return fmt.Sprintf("oracle://%s:%s@%s/%s",
		cfg.Username, cfg.Password, cfg.Endpoint, cfg.Service)
}
