// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package newrelicoraclereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver"

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	_ "github.com/godror/godror"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// NewFactory creates a new New Relic Oracle receiver factory.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiverFunc(func(dataSourceName string) (*sql.DB, error) {
			return sql.Open("godror", dataSourceName)
		}), metadata.MetricsStability))
}

func createDefaultConfig() component.Config {
	cfg := scraperhelper.NewDefaultControllerConfig()
	cfg.CollectionInterval = defaultCollectionInterval
	cfg.Timeout = 30 * time.Second // Increased from default to handle Oracle database timeouts

	config := &Config{
		ControllerConfig:      cfg,
		MetricsBuilderConfig:  metadata.DefaultMetricsBuilderConfig(),
		DisableConnectionPool: false,
		// Query Performance Monitoring defaults
		EnableQueryMonitoring:        defaultEnableQueryMonitoring,
		EnableIntervalBasedAveraging: defaultEnableIntervalBasedAveraging,
		// Set feature-level scraper flags to enabled by default
		EnableSessionScraper:      defaultEnableSessionScraper,
		EnableTablespaceScraper:   defaultEnableTablespaceScraper,
		EnableCoreScraper:         defaultEnableCoreScraper,
		EnablePdbScraper:          defaultEnablePdbScraper,
		EnableSystemScraper:       defaultEnableSystemScraper,
		EnableConnectionScraper:   defaultEnableConnectionScraper,
		EnableContainerScraper:    defaultEnableContainerScraper,
		EnableRacScraper:          defaultEnableRacScraper,
		EnableDatabaseInfoScraper: defaultEnableDatabaseInfoScraper,
	}

	// Apply defaults
	config.SetDefaults()

	return config
}

type sqlOpenerFunc func(dataSourceName string) (*sql.DB, error)

func createMetricsReceiverFunc(sqlOpenerFunc sqlOpenerFunc) receiver.CreateMetricsFunc {
	return func(
		_ context.Context,
		settings receiver.Settings,
		cfg component.Config,
		consumer consumer.Metrics,
	) (receiver.Metrics, error) {
		sqlCfg := cfg.(*Config)

		// Ensure defaults are set and configuration is valid
		sqlCfg.SetDefaults()
		if err := sqlCfg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid configuration: %w", err)
		}

		metricsBuilder := metadata.NewMetricsBuilder(sqlCfg.MetricsBuilderConfig, settings)

		hostAddress, hostPort, err := getHostAndPort(getDataSource(*sqlCfg))
		if err != nil {
			return nil, err
		}
		serviceName, serviceNameErr := getServiceName(getDataSource(*sqlCfg))
		if serviceNameErr != nil {
			return nil, serviceNameErr
		}

		// Determine which services to monitor based on pdb_services configuration
		monitoredServices, err := determineMonitoredServices(sqlOpenerFunc, getDataSource(*sqlCfg), serviceName, sqlCfg.PdbServices, settings.Logger)
		if err != nil {
			return nil, fmt.Errorf("failed to determine monitored services: %w", err)
		}

		// Create scrapers for each service (CDB or PDB)
		var opts []scraperhelper.ControllerOption
		for _, service := range monitoredServices {
			serviceName := service // Capture loop variable
			mp, err := newScraper(metricsBuilder, sqlCfg.MetricsBuilderConfig, sqlCfg.ControllerConfig, sqlCfg, settings.Logger,
				createDBProviderFunc(sqlOpenerFunc, *sqlCfg, serviceName),
				hostAddress, hostPort, serviceName)
			if err != nil {
				return nil, fmt.Errorf("failed to create scraper for service %s: %w", serviceName, err)
			}
			opts = append(opts, scraperhelper.AddScraper(metadata.Type, mp))
		}

		return scraperhelper.NewMetricsController(
			&sqlCfg.ControllerConfig,
			settings,
			consumer,
			opts...,
		)
	}
}

func getDataSource(cfg Config) string {
	if cfg.DataSource != "" {
		return cfg.DataSource
	}

	// Build godror connection string format
	// Format: user/password@host:port/service_name
	host, portStr, _ := net.SplitHostPort(cfg.Endpoint)
	port, _ := strconv.ParseInt(portStr, 10, 32)

	return fmt.Sprintf("%s/%s@%s:%d/%s", cfg.Username, cfg.Password, host, port, cfg.Service)
}

func getHostAndPort(datasource string) (string, int64, error) {
	// For godror format: user/password@host:port/service_name
	// Extract the host:port part
	if atIndex := strings.Index(datasource, "@"); atIndex != -1 {
		hostPart := datasource[atIndex+1:]
		if slashIndex := strings.Index(hostPart, "/"); slashIndex != -1 {
			hostPart = hostPart[:slashIndex]
		}
		// Split host:port
		host, portStr, err := net.SplitHostPort(hostPart)
		if err != nil {
			return "", 0, fmt.Errorf("failed to parse host:port from datasource: %w", err)
		}
		port, err := strconv.ParseInt(portStr, 10, 64)
		if err != nil {
			return "", 0, fmt.Errorf("failed to parse port: %w", err)
		}
		return host, port, nil
	}

	// Fallback to URL parsing for oracle:// format
	datasourceURL, err := url.Parse(datasource)
	if err != nil {
		return "", 0, err
	}
	host, portStr, err := net.SplitHostPort(datasourceURL.Host)
	if err != nil {
		return "", 0, fmt.Errorf("failed to parse host:port from URL: %w", err)
	}
	port, err := strconv.ParseInt(portStr, 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("failed to parse port from URL: %w", err)
	}
	return host, port, nil
}

func getServiceName(datasource string) (string, error) {
	// For godror format: user/password@host:port/service_name
	// Extract the service name part after /
	if atIndex := strings.Index(datasource, "@"); atIndex != -1 {
		hostPart := datasource[atIndex+1:]
		if slashIndex := strings.Index(hostPart, "/"); slashIndex != -1 {
			return hostPart[slashIndex+1:], nil
		}
		return "", nil
	}

	// Fallback to URL parsing for oracle:// format
	datasourceURL, err := url.Parse(datasource)
	if err != nil {
		return "", err
	}
	// Remove leading / from path
	serviceName := strings.TrimPrefix(datasourceURL.Path, "/")
	return serviceName, nil
}

// getDataSourceWithService builds a connection string with a specific service name
func getDataSourceWithService(cfg Config, serviceName string) string {
	if cfg.DataSource != "" {
		// If DataSource is provided, replace the service name in it
		datasource := cfg.DataSource
		if atIndex := strings.Index(datasource, "@"); atIndex != -1 {
			hostPart := datasource[atIndex+1:]
			if slashIndex := strings.Index(hostPart, "/"); slashIndex != -1 {
				// Replace service name
				return datasource[:atIndex+1+slashIndex+1] + serviceName
			}
		}
		return datasource
	}

	// Build godror connection string format with specified service
	// Format: user/password@host:port/service_name
	host, portStr, _ := net.SplitHostPort(cfg.Endpoint)
	port, _ := strconv.ParseInt(portStr, 10, 32)

	return fmt.Sprintf("%s/%s@%s:%d/%s", cfg.Username, cfg.Password, host, port, serviceName)
}

// determineMonitoredServices determines which services to monitor based on pdb_services configuration
func determineMonitoredServices(sqlOpenerFunc sqlOpenerFunc, dataSource, configuredService string, pdbServices []string, logger *zap.Logger) ([]string, error) {
	// Case 1: Empty pdb_services - collect only configured service data (CDB or PDB)
	if len(pdbServices) == 0 {
		logger.Info("PDB services not configured, collecting configured service data only", zap.String("service", configuredService))
		return []string{configuredService}, nil
	}

	// Case 2: pdb_services=['ALL'] - fetch all available services from database
	if len(pdbServices) == 1 && strings.EqualFold(pdbServices[0], "ALL") {
		logger.Info("Fetching all available services from database")
		allServices, err := getAvailableServiceNames(sqlOpenerFunc, dataSource, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to query all services: %w", err)
		}
		if len(allServices) == 0 {
			logger.Warn("No services found in database")
		}
		return allServices, nil
	}

	// Case 3: pdb_services=['pdb1', 'pdb2'] - fetch only specified services
	logger.Info("Filtering services based on configuration", zap.Strings("requested_services", pdbServices))
	allServices, err := getAvailableServiceNames(sqlOpenerFunc, dataSource, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to query services: %w", err)
	}

	// Filter services based on configuration (case-insensitive matching)
	filteredServices := filterServices(allServices, pdbServices, logger)
	if len(filteredServices) == 0 {
		return nil, fmt.Errorf("none of the requested services found: %v", pdbServices)
	}

	return filteredServices, nil
}

// filterServices filters services based on requested names (case-insensitive)
func filterServices(allServices, requestedServices []string, logger *zap.Logger) []string {
	// Create a map for case-insensitive lookup
	requestMap := make(map[string]string) // lowercase -> original
	for _, req := range requestedServices {
		requestMap[strings.ToLower(req)] = req
	}

	var filtered []string
	for _, service := range allServices {
		// Extract just the service name (before the first dot)
		serviceName := service
		if dotIndex := strings.Index(service, "."); dotIndex != -1 {
			serviceName = service[:dotIndex]
		}

		// Check if this service is in the requested list (case-insensitive)
		if _, found := requestMap[strings.ToLower(serviceName)]; found {
			filtered = append(filtered, service)
			logger.Info("Including service", zap.String("service_name", service))
		} else if _, found := requestMap[strings.ToLower(service)]; found {
			// Also check the full FQDN
			filtered = append(filtered, service)
			logger.Info("Including service", zap.String("service_name", service))
		}
	}

	return filtered
}

// getAvailableServiceNames queries the database for all available service names
func getAvailableServiceNames(sqlOpenerFunc sqlOpenerFunc, dataSource string, logger *zap.Logger) ([]string, error) {
	// Open a temporary connection to query service names
	db, err := sqlOpenerFunc(dataSource)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection for service query: %w", err)
	}
	defer db.Close()

	// Set a timeout for the query
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Query to get service names (PDB services)
	rows, err := db.QueryContext(ctx, queries.PDBServiceNamesSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query service names: %w", err)
	}
	defer rows.Close()

	var serviceNames []string
	for rows.Next() {
		var fqdn sql.NullString
		if err := rows.Scan(&fqdn); err != nil {
			logger.Warn("Failed to scan service name", zap.Error(err))
			continue
		}
		if fqdn.Valid && fqdn.String != "" {
			serviceNames = append(serviceNames, fqdn.String)
			logger.Info("Found service", zap.String("service_name", fqdn.String))
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating service names: %w", err)
	}

	logger.Info("Retrieved service names", zap.Int("count", len(serviceNames)), zap.Strings("services", serviceNames))
	return serviceNames, nil
}

// createDBProviderFunc creates a database provider function for a specific service (CDB or PDB)
func createDBProviderFunc(sqlOpenerFunc sqlOpenerFunc, cfg Config, serviceName string) dbProviderFunc {
	return func() (*sql.DB, error) {
		// Build service-specific connection string
		dataSource := getDataSourceWithService(cfg, serviceName)
		db, err := sqlOpenerFunc(dataSource)
		if err != nil {
			return nil, err
		}

		// Configure connection pool settings
		if !cfg.DisableConnectionPool {
			db.SetMaxOpenConns(cfg.MaxOpenConnections)
		} else {
			// Disable connection pooling
			db.SetMaxOpenConns(1)
		}

		// Set connection timeouts to ensure proper cancellation
		// MaxIdleConns should be reasonable to prevent too many idle connections
		db.SetMaxIdleConns(2)
		// ConnMaxLifetime ensures connections are refreshed periodically
		db.SetConnMaxLifetime(10 * time.Minute)
		// ConnMaxIdleTime closes idle connections
		db.SetConnMaxIdleTime(30 * time.Second)

		return db, nil
	}
}
