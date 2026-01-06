// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

// RacScraper handles metrics collection for Oracle Real Application Clusters (RAC)
// and Automatic Storage Management (ASM) configurations.
type RacScraper struct {
	client               client.OracleClient
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	instanceName         string
	metricsBuilderConfig metadata.MetricsBuilderConfig
	isRacMode            *bool // Cached result of RAC detection to avoid repeated queries
	racModeMutex         sync.RWMutex
}

// NewRacScraper creates a new RacScraper instance for collecting RAC and ASM metrics.
func NewRacScraper(c client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, config metadata.MetricsBuilderConfig) *RacScraper {
	return &RacScraper{
		client:               c,
		mb:                   mb,
		logger:               logger,
		instanceName:         instanceName,
		metricsBuilderConfig: config,
	}
}

// isRacEnabled checks if Oracle RAC is enabled. The result is cached after the first check
// to avoid repeated queries to the database. Uses double-checked locking for thread safety.
func (s *RacScraper) isRacEnabled(ctx context.Context) (bool, error) {
	s.racModeMutex.RLock()
	if s.isRacMode != nil {
		result := *s.isRacMode
		s.racModeMutex.RUnlock()
		return result, nil
	}
	s.racModeMutex.RUnlock()

	s.racModeMutex.Lock()
	defer s.racModeMutex.Unlock()

	if s.isRacMode != nil {
		return *s.isRacMode, nil
	}

	detection, err := s.client.QueryRACDetection(ctx)
	if err != nil {
		return false, err
	}

	racEnabled := detection.ClusterDB.Valid && strings.ToUpper(detection.ClusterDB.String) == "TRUE"
	s.isRacMode = &racEnabled
	s.logger.Debug("RAC mode detection", zap.Bool("enabled", racEnabled))

	return racEnabled, nil
}

// isASMAvailable checks if Automatic Storage Management (ASM) is configured and available.
// Returns false with no error if ASM is simply not configured.
func (s *RacScraper) isASMAvailable(ctx context.Context) (bool, error) {
	detection, err := s.client.QueryASMDetection(ctx)
	if err != nil {
		s.logger.Debug("ASM not configured", zap.Error(err))
		return false, nil
	}

	asmAvailable := detection.ASMCount > 0
	s.logger.Debug("ASM detection")

	return asmAvailable, nil
}

// ScrapeRacMetrics collects all RAC and ASM related metrics concurrently.
// It first checks if RAC and ASM are enabled/available, then executes the appropriate
// scraping functions in parallel for efficiency.
func (s *RacScraper) ScrapeRacMetrics(ctx context.Context) []error {
	racEnabled, err := s.isRacEnabled(ctx)
	if err != nil {
		return []error{err}
	}

	asmAvailable, err := s.isASMAvailable(ctx)
	if err != nil {
		return []error{err}
	}

	if !racEnabled && !asmAvailable {
		return nil
	}

	if ctx.Err() != nil {
		return []error{ctx.Err()}
	}

	var scrapers []func(context.Context) []error
	var scraperCount int

	if asmAvailable {
		scrapers = append(scrapers, s.scrapeASMDiskGroups)
		scraperCount++
	}

	if racEnabled {
		scrapers = append(scrapers,
			s.scrapeClusterWaitEvents,
			s.scrapeInstanceStatus,
			s.scrapeActiveServices,
		)
		scraperCount += 3
	}

	if scraperCount == 0 {
		return nil
	}

	errorChan := make(chan []error, scraperCount)
	var wg sync.WaitGroup
	wg.Add(scraperCount)

	for _, scraper := range scrapers {
		go func(fn func(context.Context) []error) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				errorChan <- []error{ctx.Err()}
				return
			default:
			}
			if errs := fn(ctx); len(errs) > 0 {
				errorChan <- errs
			} else {
				errorChan <- nil
			}
		}(scraper)
	}

	go func() {
		wg.Wait()
		close(errorChan)
	}()

	var allErrors []error
	for errors := range errorChan {
		if errors != nil {
			allErrors = append(allErrors, errors...)
		}
	}

	if ctx.Err() != nil {
		allErrors = append(allErrors, ctx.Err())
	}

	return allErrors
}

// nullStringToString converts a sql.NullString to a regular string.
// Returns an empty string if the NullString is not valid.
func nullStringToString(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

// stringStatusToBinary converts a status string to a binary value (1 or 0).
// Returns 1 if the status matches the expected value (case-insensitive), otherwise 0.
func stringStatusToBinary(status, expectedValue string) int64 {
	if strings.ToUpper(status) == strings.ToUpper(expectedValue) {
		return 1
	}
	return 0
}

// formatTimeout converts a sql.NullFloat64 to a string representation for timeout values
func formatTimeout(timeout sql.NullFloat64) string {
	if !timeout.Valid {
		return "0"
	}
	return fmt.Sprintf("%.0f", timeout.Float64)
}

// getTimeoutValue extracts an int64 value from a sql.NullFloat64, defaulting to 0 if invalid
func getTimeoutValue(timeout sql.NullFloat64) int64 {
	if !timeout.Valid {
		return 0
	}
	return int64(timeout.Float64)
}

// scrapeASMDiskGroups collects metrics for ASM disk groups including total space,
// free space, and offline disk counts.
func (s *RacScraper) scrapeASMDiskGroups(ctx context.Context) []error {
	if !s.metricsBuilderConfig.Metrics.NewrelicoracledbAsmDiskgroupTotalMb.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbAsmDiskgroupFreeMb.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbAsmDiskgroupOfflineDisks.Enabled {
		return nil
	}

	var scrapeErrors []error

	diskGroups, err := s.client.QueryASMDiskGroups(ctx)
	if err != nil {
		return []error{err}
	}

	// Generate timestamp once for all metrics in this scrape cycle for consistency
	// and performance. All disk groups are collected at the same moment.
	now := pcommon.NewTimestampFromTime(time.Now())

	for _, diskGroup := range diskGroups {
		if !diskGroup.Name.Valid {
			continue
		}

		diskGroupName := diskGroup.Name.String

		if diskGroup.TotalMB.Valid && s.metricsBuilderConfig.Metrics.NewrelicoracledbAsmDiskgroupTotalMb.Enabled {
			s.mb.RecordNewrelicoracledbAsmDiskgroupTotalMbDataPoint(now, diskGroup.TotalMB.Float64, s.instanceName, diskGroupName)
		}
		if diskGroup.FreeMB.Valid && s.metricsBuilderConfig.Metrics.NewrelicoracledbAsmDiskgroupFreeMb.Enabled {
			s.mb.RecordNewrelicoracledbAsmDiskgroupFreeMbDataPoint(now, diskGroup.FreeMB.Float64, s.instanceName, diskGroupName)
		}
		if diskGroup.OfflineDisks.Valid && s.metricsBuilderConfig.Metrics.NewrelicoracledbAsmDiskgroupOfflineDisks.Enabled {
			s.mb.RecordNewrelicoracledbAsmDiskgroupOfflineDisksDataPoint(now, int64(diskGroup.OfflineDisks.Float64), s.instanceName, diskGroupName)
		}
	}

	return scrapeErrors
}

// scrapeClusterWaitEvents collects RAC cluster-wide wait event statistics
// including wait times and total wait counts.
func (s *RacScraper) scrapeClusterWaitEvents(ctx context.Context) []error {
	if !s.metricsBuilderConfig.Metrics.NewrelicoracledbRacWaitTime.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbRacTotalWaits.Enabled {
		return nil
	}

	var scrapeErrors []error

	events, err := s.client.QueryClusterWaitEvents(ctx)
	if err != nil {
		return []error{err}
	}

	// Generate timestamp once for all metrics in this scrape cycle for consistency
	// and performance. All wait events are collected at the same moment.
	now := pcommon.NewTimestampFromTime(time.Now())

	for _, event := range events {
		if !event.InstID.Valid || !event.Event.Valid {
			continue
		}

		instanceIDStr := event.InstID.String
		eventName := event.Event.String

		if event.TimeWaitedMicro.Valid && s.metricsBuilderConfig.Metrics.NewrelicoracledbRacWaitTime.Enabled {
			s.mb.RecordNewrelicoracledbRacWaitTimeDataPoint(now, event.TimeWaitedMicro.Float64, s.instanceName, instanceIDStr, eventName)
		}
		if event.TotalWaits.Valid && s.metricsBuilderConfig.Metrics.NewrelicoracledbRacTotalWaits.Enabled {
			s.mb.RecordNewrelicoracledbRacTotalWaitsDataPoint(now, int64(event.TotalWaits.Float64), s.instanceName, instanceIDStr, eventName)
		}
	}

	return scrapeErrors
}

// scrapeInstanceStatus collects status metrics for all RAC instances including
// instance status, uptime, database status, active state, logins allowed, archiver status,
// and version information.
func (s *RacScraper) scrapeInstanceStatus(ctx context.Context) []error {
	// Check if any instance status metrics are enabled
	if !s.isAnyInstanceMetricEnabled() {
		return nil
	}

	var scrapeErrors []error

	instances, err := s.client.QueryRACInstanceStatus(ctx)
	if err != nil {
		return []error{err}
	}

	// Generate timestamp once for all metrics in this scrape cycle for consistency
	// and performance. All instances are queried and collected at the same moment.
	now := pcommon.NewTimestampFromTime(time.Now())

	for _, instance := range instances {
		if !instance.InstID.Valid || !instance.Status.Valid {
			continue
		}

		instanceIDStr := instance.InstID.String
		statusStr := instance.Status.String
		instanceNameStr := nullStringToString(instance.InstanceName)
		hostNameStr := nullStringToString(instance.HostName)
		databaseStatusStr := nullStringToString(instance.DatabaseStatus)
		activeStateStr := nullStringToString(instance.ActiveState)
		loginsStr := nullStringToString(instance.Logins)
		archiverStr := nullStringToString(instance.Archiver)
		versionStr := nullStringToString(instance.Version)

		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceStatus.Enabled {
			statusValue := stringStatusToBinary(statusStr, "OPEN")
			s.mb.RecordNewrelicoracledbRacInstanceStatusDataPoint(now, statusValue, s.instanceName, instanceIDStr, instanceNameStr, hostNameStr, statusStr)
		}

		if instance.StartupTime.Valid && s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceUptimeSeconds.Enabled {
			uptime := time.Since(instance.StartupTime.Time).Seconds()
			s.mb.RecordNewrelicoracledbRacInstanceUptimeSecondsDataPoint(now, int64(uptime), s.instanceName, instanceIDStr, instanceNameStr, hostNameStr)
		}

		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceDatabaseStatus.Enabled {
			dbStatusValue := stringStatusToBinary(databaseStatusStr, "ACTIVE")
			s.mb.RecordNewrelicoracledbRacInstanceDatabaseStatusDataPoint(now, dbStatusValue, s.instanceName, instanceIDStr, instanceNameStr, hostNameStr, databaseStatusStr)
		}

		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceActiveState.Enabled {
			activeStateValue := stringStatusToBinary(activeStateStr, "NORMAL")
			s.mb.RecordNewrelicoracledbRacInstanceActiveStateDataPoint(now, activeStateValue, s.instanceName, instanceIDStr, instanceNameStr, hostNameStr, activeStateStr)
		}

		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceLoginsAllowed.Enabled {
			loginsValue := stringStatusToBinary(loginsStr, "ALLOWED")
			s.mb.RecordNewrelicoracledbRacInstanceLoginsAllowedDataPoint(now, loginsValue, s.instanceName, instanceIDStr, instanceNameStr, hostNameStr, loginsStr)
		}

		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceArchiverStarted.Enabled {
			archiverValue := stringStatusToBinary(archiverStr, "STARTED")
			s.mb.RecordNewrelicoracledbRacInstanceArchiverStartedDataPoint(now, archiverValue, s.instanceName, instanceIDStr, instanceNameStr, hostNameStr, archiverStr)
		}

		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceVersionInfo.Enabled {
			s.mb.RecordNewrelicoracledbRacInstanceVersionInfoDataPoint(now, 1, s.instanceName, instanceNameStr, hostNameStr, versionStr)
		}
	}

	return scrapeErrors
}

// isAnyInstanceMetricEnabled checks if any RAC instance status metric is enabled
func (s *RacScraper) isAnyInstanceMetricEnabled() bool {
	return s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceStatus.Enabled ||
		s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceUptimeSeconds.Enabled ||
		s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceDatabaseStatus.Enabled ||
		s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceActiveState.Enabled ||
		s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceLoginsAllowed.Enabled ||
		s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceArchiverStarted.Enabled ||
		s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceVersionInfo.Enabled
}

// isAnyServiceMetricEnabled checks if any RAC service metric is enabled
func (s *RacScraper) isAnyServiceMetricEnabled() bool {
	return s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceInstanceID.Enabled ||
		s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceNetworkConfig.Enabled ||
		s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceClbConfig.Enabled
}

// scrapeActiveServices collects metrics for active RAC services including
// instance assignments, network configuration, connection load balancing, service goals,
// blocked status, FAN enablement, Transaction Guard, and timeout settings.
func (s *RacScraper) scrapeActiveServices(ctx context.Context) []error {
	if !s.isAnyServiceMetricEnabled() {
		return nil
	}

	var scrapeErrors []error

	services, err := s.client.QueryRACActiveServices(ctx)
	if err != nil {
		return []error{err}
	}

	// Generate timestamp once for all metrics in this scrape cycle for consistency
	// and performance. All services are queried and collected at the same moment.
	now := pcommon.NewTimestampFromTime(time.Now())

	for _, service := range services {
		if !service.ServiceName.Valid || !service.InstID.Valid {
			continue
		}

		serviceNameStr := service.ServiceName.String
		instanceIDStr := service.InstID.String

		// Record metrics only if enabled
		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceInstanceID.Enabled {
			instanceIDInt, _ := strconv.ParseFloat(instanceIDStr, 64)
			s.mb.RecordNewrelicoracledbRacServiceInstanceIDDataPoint(now, instanceIDInt, s.instanceName, serviceNameStr, instanceIDStr)
		}

		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceNetworkConfig.Enabled {
			s.mb.RecordNewrelicoracledbRacServiceNetworkConfigDataPoint(now, 1, s.instanceName, serviceNameStr, nullStringToString(service.NetworkName))
		}

		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceClbConfig.Enabled {
			s.mb.RecordNewrelicoracledbRacServiceClbConfigDataPoint(now, 1, s.instanceName, serviceNameStr, nullStringToString(service.ClbGoal))
		}

		// Service goal configuration
		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceGoalConfig.Enabled {
			s.mb.RecordNewrelicoracledbRacServiceGoalConfigDataPoint(now, 1, s.instanceName, serviceNameStr, nullStringToString(service.Goal))
		}

		// Service blocked status (1 if blocked, 0 if not)
		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceBlockedStatus.Enabled {
			blockedValue := stringStatusToBinary(nullStringToString(service.Blocked), "YES")
			s.mb.RecordNewrelicoracledbRacServiceBlockedStatusDataPoint(now, blockedValue, s.instanceName, serviceNameStr, nullStringToString(service.Blocked))
		}

		// Fast Application Notification (FAN) enabled status (1 if enabled, 0 if not)
		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceFanEnabled.Enabled {
			fanValue := stringStatusToBinary(nullStringToString(service.AqHaNotification), "YES")
			s.mb.RecordNewrelicoracledbRacServiceFanEnabledDataPoint(now, fanValue, s.instanceName, serviceNameStr, nullStringToString(service.AqHaNotification))
		}

		// Transaction Guard enabled status (1 if enabled, 0 if not)
		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceTransactionGuardEnabled.Enabled {
			tgValue := stringStatusToBinary(nullStringToString(service.CommitOutcome), "TRUE")
			s.mb.RecordNewrelicoracledbRacServiceTransactionGuardEnabledDataPoint(now, tgValue, s.instanceName, serviceNameStr, nullStringToString(service.CommitOutcome))
		}

		// Session drain timeout in seconds
		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceDrainTimeoutSeconds.Enabled {
			drainTimeout := getTimeoutValue(service.DrainTimeout)
			drainTimeoutStr := formatTimeout(service.DrainTimeout)
			s.mb.RecordNewrelicoracledbRacServiceDrainTimeoutSecondsDataPoint(now, drainTimeout, s.instanceName, serviceNameStr, drainTimeoutStr)
		}

		// Application Continuity replay initiation timeout in seconds
		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceReplayTimeoutSeconds.Enabled {
			replayTimeout := getTimeoutValue(service.ReplayInitiationTimeout)
			replayTimeoutStr := formatTimeout(service.ReplayInitiationTimeout)
			s.mb.RecordNewrelicoracledbRacServiceReplayTimeoutSecondsDataPoint(now, replayTimeout, s.instanceName, serviceNameStr, replayTimeoutStr)
		}
	}

	return scrapeErrors
}
