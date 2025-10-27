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

// scrapeLockedAccountsMetrics handles the locked accounts metrics
func (s *CoreScraper) scrapeLockedAccountsMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	// Check if locked accounts metric is enabled
	if !s.config.Metrics.NewrelicoracledbLockedAccounts.Enabled {
		return nil
	}

	var errors []error

	// Execute locked accounts query
	s.logger.Debug("Executing locked accounts query", zap.String("sql", queries.LockedAccountsSQL))

	rows, err := s.db.QueryContext(ctx, queries.LockedAccountsSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing locked accounts query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var instID interface{}
		var lockedAccounts int64

		err := rows.Scan(&instID, &lockedAccounts)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning locked accounts row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Record locked accounts metrics
		s.logger.Info("Locked accounts metrics collected",
			zap.String("instance_id", instanceID),
			zap.Int64("locked_accounts", lockedAccounts),
			zap.String("instance", s.instanceName),
		)

		// Record the locked accounts metric
		s.mb.RecordNewrelicoracledbLockedAccountsDataPoint(now, lockedAccounts, s.instanceName, instanceID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating locked accounts rows: %w", err))
	}

	s.logger.Debug("Collected Oracle locked accounts metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeGlobalNameInstanceMetrics handles the global name instance metrics
func (s *CoreScraper) scrapeGlobalNameInstanceMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	// Execute global name instance metrics query
	s.logger.Debug("Executing global name instance metrics query", zap.String("sql", queries.GlobalNameInstanceSQL))

	rows, err := s.db.QueryContext(ctx, queries.GlobalNameInstanceSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing global name instance metrics query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var instID interface{}
		var globalName string

		err := rows.Scan(&instID, &globalName)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning global name instance metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Record global name instance metrics
		s.logger.Info("Global name instance metrics collected",
			zap.String("instance_id", instanceID),
			zap.String("global_name", globalName),
			zap.String("instance", s.instanceName),
		)

		// Record the global name metric (using 1 as value since it's an attribute metric)
		s.mb.RecordNewrelicoracledbGlobalNameDataPoint(now, 1, s.instanceName, instanceID, globalName)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating global name instance metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle global name instance metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeDBIDInstanceMetrics handles the database ID instance metrics
func (s *CoreScraper) scrapeDBIDInstanceMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	// Execute database ID instance metrics query
	s.logger.Debug("Executing database ID instance metrics query", zap.String("sql", queries.DBIDInstanceSQL))

	rows, err := s.db.QueryContext(ctx, queries.DBIDInstanceSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing database ID instance metrics query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var instID interface{}
		var dbID string

		err := rows.Scan(&instID, &dbID)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning database ID instance metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Record database ID instance metrics
		s.logger.Info("Database ID instance metrics collected",
			zap.String("instance_id", instanceID),
			zap.String("db_id", dbID),
			zap.String("instance", s.instanceName),
		)

		// Record the database ID metric (using 1 as value since it's an attribute metric)
		s.mb.RecordNewrelicoracledbDbIDDataPoint(now, 1, s.instanceName, instanceID, dbID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating database ID instance metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle database ID instance metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeLongRunningQueriesMetrics handles the long running queries metrics
func (s *CoreScraper) scrapeLongRunningQueriesMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	// Execute long running queries metrics query
	s.logger.Debug("Executing long running queries metrics query", zap.String("sql", queries.LongRunningQueriesSQL))

	rows, err := s.db.QueryContext(ctx, queries.LongRunningQueriesSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing long running queries metrics query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var instID interface{}
		var total int64

		err := rows.Scan(&instID, &total)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning long running queries metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Record long running queries metrics
		s.logger.Info("Long running queries metrics collected",
			zap.String("instance_id", instanceID),
			zap.Int64("long_running_queries", total),
			zap.String("instance", s.instanceName),
		)

		// Record the long running queries metric
		s.mb.RecordNewrelicoracledbLongRunningQueriesDataPoint(now, total, s.instanceName, instanceID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating long running queries metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle long running queries metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}
