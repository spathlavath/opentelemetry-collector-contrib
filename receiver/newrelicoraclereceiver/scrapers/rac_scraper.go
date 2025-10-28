// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

type RacScraper struct {
	db                   *sql.DB
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	instanceName         string
	metricsBuilderConfig metadata.MetricsBuilderConfig
	isRacMode            *bool
	racModeMutex         sync.RWMutex
}

func NewRacScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, config metadata.MetricsBuilderConfig) *RacScraper {
	return &RacScraper{
		db:                   db,
		mb:                   mb,
		logger:               logger,
		instanceName:         instanceName,
		metricsBuilderConfig: config,
	}
}

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

	rows, err := s.db.QueryContext(ctx, queries.RACDetectionSQL)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var clusterDB sql.NullString
	if rows.Next() {
		if err := rows.Scan(&clusterDB); err != nil {
			return false, err
		}
	}

	racEnabled := clusterDB.Valid && strings.ToUpper(clusterDB.String) == "TRUE"
	s.isRacMode = &racEnabled
	s.logger.Debug("RAC mode detection", zap.Bool("enabled", racEnabled))

	return racEnabled, nil
}

func (s *RacScraper) isASMAvailable(ctx context.Context) (bool, error) {
	rows, err := s.db.QueryContext(ctx, queries.ASMDetectionSQL)
	if err != nil {
		s.logger.Debug("ASM not configured", zap.Error(err))
		return false, nil
	}
	defer rows.Close()

	var asmCount int
	if rows.Next() {
		if err := rows.Scan(&asmCount); err != nil {
			return false, err
		}
	}

	asmAvailable := asmCount > 0
	s.logger.Debug("ASM detection", zap.Bool("available", asmAvailable), zap.Int("diskgroups", asmCount))

	return asmAvailable, nil
}

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

func nullStringToString(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

func stringStatusToBinary(status, expectedValue string) int64 {
	if strings.ToUpper(status) == strings.ToUpper(expectedValue) {
		return 1
	}
	return 0
}

func (s *RacScraper) scrapeASMDiskGroups(ctx context.Context) []error {
	if !s.metricsBuilderConfig.Metrics.NewrelicoracledbAsmDiskgroupTotalMb.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbAsmDiskgroupFreeMb.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbAsmDiskgroupOfflineDisks.Enabled {
		return nil
	}

	var scrapeErrors []error

	rows, err := s.db.QueryContext(ctx, queries.ASMDiskGroupSQL)
	if err != nil {
		return []error{err}
	}
	defer rows.Close()

	for rows.Next() {
		var name sql.NullString
		var totalMB sql.NullFloat64
		var freeMB sql.NullFloat64
		var offlineDisks sql.NullFloat64

		if err := rows.Scan(&name, &totalMB, &freeMB, &offlineDisks); err != nil {
			scrapeErrors = append(scrapeErrors, err)
			continue
		}

		if !name.Valid {
			continue
		}

		now := pcommon.NewTimestampFromTime(time.Now())

		if totalMB.Valid && s.metricsBuilderConfig.Metrics.NewrelicoracledbAsmDiskgroupTotalMb.Enabled {
			s.mb.RecordNewrelicoracledbAsmDiskgroupTotalMbDataPoint(now, totalMB.Float64, s.instanceName, name.String)
		}
		if freeMB.Valid && s.metricsBuilderConfig.Metrics.NewrelicoracledbAsmDiskgroupFreeMb.Enabled {
			s.mb.RecordNewrelicoracledbAsmDiskgroupFreeMbDataPoint(now, freeMB.Float64, s.instanceName, name.String)
		}
		if offlineDisks.Valid && s.metricsBuilderConfig.Metrics.NewrelicoracledbAsmDiskgroupOfflineDisks.Enabled {
			s.mb.RecordNewrelicoracledbAsmDiskgroupOfflineDisksDataPoint(now, int64(offlineDisks.Float64), s.instanceName, name.String)
		}
	}

	return scrapeErrors
}

func (s *RacScraper) scrapeClusterWaitEvents(ctx context.Context) []error {
	if !s.metricsBuilderConfig.Metrics.NewrelicoracledbRacWaitTime.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbRacTotalWaits.Enabled {
		return nil
	}

	var scrapeErrors []error

	rows, err := s.db.QueryContext(ctx, queries.ClusterWaitEventsSQL)
	if err != nil {
		return []error{err}
	}
	defer rows.Close()

	for rows.Next() {
		var instID sql.NullString
		var event sql.NullString
		var totalWaits sql.NullFloat64
		var timeWaitedMicro sql.NullFloat64

		if err := rows.Scan(&instID, &event, &totalWaits, &timeWaitedMicro); err != nil {
			scrapeErrors = append(scrapeErrors, err)
			continue
		}

		if !instID.Valid || !event.Valid {
			continue
		}

		now := pcommon.NewTimestampFromTime(time.Now())
		instanceIDStr := instID.String
		eventName := event.String

		if timeWaitedMicro.Valid && s.metricsBuilderConfig.Metrics.NewrelicoracledbRacWaitTime.Enabled {
			s.mb.RecordNewrelicoracledbRacWaitTimeDataPoint(now, timeWaitedMicro.Float64, s.instanceName, instanceIDStr, eventName)
		}
		if totalWaits.Valid && s.metricsBuilderConfig.Metrics.NewrelicoracledbRacTotalWaits.Enabled {
			s.mb.RecordNewrelicoracledbRacTotalWaitsDataPoint(now, int64(totalWaits.Float64), s.instanceName, instanceIDStr, eventName)
		}
	}

	return scrapeErrors
}

func (s *RacScraper) scrapeInstanceStatus(ctx context.Context) []error {
	// Check if any instance status metrics are enabled
	if !s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceStatus.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceUptimeSeconds.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceDatabaseStatus.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceActiveState.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceLoginsAllowed.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceArchiverStarted.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceVersionInfo.Enabled {
		return nil
	}

	var scrapeErrors []error

	rows, err := s.db.QueryContext(ctx, queries.RACInstanceStatusSQL)
	if err != nil {
		return []error{err}
	}
	defer rows.Close()

	for rows.Next() {
		var instID, instanceName, hostName, status sql.NullString
		var startupTime sql.NullTime
		var databaseStatus, activeState, logins, archiver, version sql.NullString

		if err := rows.Scan(&instID, &instanceName, &hostName, &status, &startupTime, &databaseStatus, &activeState, &logins, &archiver, &version); err != nil {
			scrapeErrors = append(scrapeErrors, err)
			continue
		}

		if !instID.Valid || !status.Valid {
			continue
		}

		now := pcommon.NewTimestampFromTime(time.Now())
		instanceIDStr := instID.String
		statusStr := status.String
		instanceNameStr := nullStringToString(instanceName)
		hostNameStr := nullStringToString(hostName)
		databaseStatusStr := nullStringToString(databaseStatus)
		activeStateStr := nullStringToString(activeState)
		loginsStr := nullStringToString(logins)
		archiverStr := nullStringToString(archiver)
		versionStr := nullStringToString(version)

		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceStatus.Enabled {
			statusValue := stringStatusToBinary(statusStr, "OPEN")
			s.mb.RecordNewrelicoracledbRacInstanceStatusDataPoint(now, statusValue, s.instanceName, instanceIDStr, instanceNameStr, hostNameStr, statusStr)
		}

		if startupTime.Valid && s.metricsBuilderConfig.Metrics.NewrelicoracledbRacInstanceUptimeSeconds.Enabled {
			uptime := time.Since(startupTime.Time).Seconds()
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
			s.mb.RecordNewrelicoracledbRacInstanceVersionInfoDataPoint(now, 1, s.instanceName, instanceIDStr, instanceNameStr, hostNameStr, versionStr)
		}
	}

	return scrapeErrors
}

func (s *RacScraper) scrapeActiveServices(ctx context.Context) []error {
	if !s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceInstanceID.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceFailoverConfig.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceNetworkConfig.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceCreationAgeDays.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceFailoverRetries.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceFailoverDelaySeconds.Enabled &&
		!s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceClbConfig.Enabled {
		return nil
	}

	var scrapeErrors []error

	rows, err := s.db.QueryContext(ctx, queries.RACActiveServicesSQL)
	if err != nil {
		return []error{err}
	}
	defer rows.Close()

	for rows.Next() {
		var serviceName, instID, failoverMethod, failoverType, goal sql.NullString
		var networkName, creationDate, failoverRetries, failoverDelay, clbGoal sql.NullString

		if err := rows.Scan(&serviceName, &instID, &failoverMethod, &failoverType, &goal, &networkName, &creationDate, &failoverRetries, &failoverDelay, &clbGoal); err != nil {
			scrapeErrors = append(scrapeErrors, err)
			continue
		}

		if !serviceName.Valid || !instID.Valid {
			continue
		}

		now := pcommon.NewTimestampFromTime(time.Now())
		serviceNameStr := serviceName.String
		instanceIDStr := instID.String

		// Record metrics only if enabled
		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceInstanceID.Enabled {
			instanceIDInt, _ := strconv.ParseFloat(instanceIDStr, 64)
			s.mb.RecordNewrelicoracledbRacServiceInstanceIDDataPoint(now, instanceIDInt, s.instanceName, serviceNameStr, instanceIDStr)
		}

		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceFailoverConfig.Enabled {
			s.mb.RecordNewrelicoracledbRacServiceFailoverConfigDataPoint(now, int64(1), s.instanceName, serviceNameStr, instanceIDStr,
				nullStringToString(failoverMethod), nullStringToString(failoverType), nullStringToString(goal))
		}

		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceNetworkConfig.Enabled {
			s.mb.RecordNewrelicoracledbRacServiceNetworkConfigDataPoint(now, 1, s.instanceName, serviceNameStr, instanceIDStr, nullStringToString(networkName))
		}

		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceCreationAgeDays.Enabled {
			s.mb.RecordNewrelicoracledbRacServiceCreationAgeDaysDataPoint(now, 1, s.instanceName, serviceNameStr, instanceIDStr)
		}

		// Parse and record failover metrics only if enabled
		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceFailoverRetries.Enabled {
			var failoverRetriesValue int64 = 0
			if failoverRetriesStr := nullStringToString(failoverRetries); failoverRetriesStr != "" {
				if retriesInt, err := strconv.ParseInt(failoverRetriesStr, 10, 64); err == nil {
					failoverRetriesValue = retriesInt
				}
			}
			s.mb.RecordNewrelicoracledbRacServiceFailoverRetriesDataPoint(now, failoverRetriesValue, s.instanceName, serviceNameStr, instanceIDStr)
		}

		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceFailoverDelaySeconds.Enabled {
			var failoverDelayValue int64 = 0
			if failoverDelayStr := nullStringToString(failoverDelay); failoverDelayStr != "" {
				if delayInt, err := strconv.ParseInt(failoverDelayStr, 10, 64); err == nil {
					failoverDelayValue = delayInt
				}
			}
			s.mb.RecordNewrelicoracledbRacServiceFailoverDelaySecondsDataPoint(now, failoverDelayValue, s.instanceName, serviceNameStr, instanceIDStr)
		}

		if s.metricsBuilderConfig.Metrics.NewrelicoracledbRacServiceClbConfig.Enabled {
			s.mb.RecordNewrelicoracledbRacServiceClbConfigDataPoint(now, 1, s.instanceName, serviceNameStr, instanceIDStr, nullStringToString(clbGoal))
		}
	}

	return scrapeErrors
}
