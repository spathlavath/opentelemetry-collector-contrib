// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

func (s *CoreScraper) scrapeLockedAccountsMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	if !s.config.Metrics.NewrelicoracledbLockedAccounts.Enabled {
		return nil
	}

	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.LockedAccountsSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing locked accounts query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var instID interface{}
		var lockedAccounts int64

		if err := rows.Scan(&instID, &lockedAccounts); err != nil {
			errors = append(errors, fmt.Errorf("error scanning locked accounts row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)
		s.mb.RecordNewrelicoracledbLockedAccountsDataPoint(now, lockedAccounts, s.instanceName, instanceID)
		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating locked accounts rows: %w", err))
	}

	s.logger.Debug("Locked accounts metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}

func (s *CoreScraper) scrapeGlobalNameInstanceMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.GlobalNameInstanceSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing global name instance metrics query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var instID interface{}
		var globalName string

		if err := rows.Scan(&instID, &globalName); err != nil {
			errors = append(errors, fmt.Errorf("error scanning global name instance metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)
		s.mb.RecordNewrelicoracledbGlobalNameDataPoint(now, 1, s.instanceName, instanceID, globalName)
		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating global name instance metrics rows: %w", err))
	}

	s.logger.Debug("Global name metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}

func (s *CoreScraper) scrapeDBIDInstanceMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.DBIDInstanceSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing database ID instance metrics query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var instID interface{}
		var dbID string

		if err := rows.Scan(&instID, &dbID); err != nil {
			errors = append(errors, fmt.Errorf("error scanning database ID instance metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)
		s.mb.RecordNewrelicoracledbDbIDDataPoint(now, 1, s.instanceName, instanceID, dbID)
		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating database ID instance metrics rows: %w", err))
	}

	s.logger.Debug("Database ID metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}

func (s *CoreScraper) scrapeLongRunningQueriesMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.LongRunningQueriesSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing long running queries metrics query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var instID interface{}
		var total int64

		if err := rows.Scan(&instID, &total); err != nil {
			errors = append(errors, fmt.Errorf("error scanning long running queries metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)
		s.mb.RecordNewrelicoracledbLongRunningQueriesDataPoint(now, total, s.instanceName, instanceID)
		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating long running queries metrics rows: %w", err))
	}

	s.logger.Debug("Long running queries metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}
