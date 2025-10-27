// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// scrapeSGASharedPoolLibraryCacheReloadRatioMetrics handles the SGA shared pool library cache reload ratio metrics
func (s *CoreScraper) scrapeSGASharedPoolLibraryCacheReloadRatioMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	// Execute SGA shared pool library cache reload ratio metrics query
	s.logger.Debug("Executing SGA shared pool library cache reload ratio metrics query", zap.String("sql", queries.SGASharedPoolLibraryCacheReloadRatioSQL))

	rows, err := s.db.QueryContext(ctx, queries.SGASharedPoolLibraryCacheReloadRatioSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing SGA shared pool library cache reload ratio metrics query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var ratio float64
		var instID interface{}

		err := rows.Scan(&ratio, &instID)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA shared pool library cache reload ratio metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Record SGA shared pool library cache reload ratio metrics
		s.logger.Info("SGA shared pool library cache reload ratio metrics collected",
			zap.String("instance_id", instanceID),
			zap.Float64("sga_shared_pool_library_cache_reload_ratio", ratio),
			zap.String("instance", s.instanceName),
		)

		// Record the SGA shared pool library cache reload ratio metric
		s.mb.RecordNewrelicoracledbSgaSharedPoolLibraryCacheReloadRatioDataPoint(now, ratio, s.instanceName, instanceID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA shared pool library cache reload ratio metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle SGA shared pool library cache reload ratio metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeSGASharedPoolLibraryCacheHitRatioMetrics handles the SGA shared pool library cache hit ratio metrics
func (s *CoreScraper) scrapeSGASharedPoolLibraryCacheHitRatioMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	// Execute SGA shared pool library cache hit ratio metrics query
	s.logger.Debug("Executing SGA shared pool library cache hit ratio metrics query", zap.String("sql", queries.SGASharedPoolLibraryCacheHitRatioSQL))

	rows, err := s.db.QueryContext(ctx, queries.SGASharedPoolLibraryCacheHitRatioSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing SGA shared pool library cache hit ratio metrics query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var ratio float64
		var instID interface{}

		err := rows.Scan(&ratio, &instID)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA shared pool library cache hit ratio metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Record SGA shared pool library cache hit ratio metrics
		s.logger.Info("SGA shared pool library cache hit ratio metrics collected",
			zap.String("instance_id", instanceID),
			zap.Float64("sga_shared_pool_library_cache_hit_ratio", ratio),
			zap.String("instance", s.instanceName),
		)

		// Record the SGA shared pool library cache hit ratio metric
		s.mb.RecordNewrelicoracledbSgaSharedPoolLibraryCacheHitRatioDataPoint(now, ratio, s.instanceName, instanceID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA shared pool library cache hit ratio metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle SGA shared pool library cache hit ratio metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeSGASharedPoolDictCacheMissRatioMetrics handles the SGA shared pool dictionary cache miss ratio metrics
func (s *CoreScraper) scrapeSGASharedPoolDictCacheMissRatioMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	// Execute SGA shared pool dictionary cache miss ratio metrics query
	s.logger.Debug("Executing SGA shared pool dictionary cache miss ratio metrics query", zap.String("sql", queries.SGASharedPoolDictCacheMissRatioSQL))

	rows, err := s.db.QueryContext(ctx, queries.SGASharedPoolDictCacheMissRatioSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing SGA shared pool dictionary cache miss ratio metrics query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var ratio float64
		var instID interface{}

		err := rows.Scan(&ratio, &instID)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA shared pool dictionary cache miss ratio metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Record SGA shared pool dictionary cache miss ratio metrics
		s.logger.Info("SGA shared pool dictionary cache miss ratio metrics collected",
			zap.String("instance_id", instanceID),
			zap.Float64("sga_shared_pool_dict_cache_miss_ratio", ratio),
			zap.String("instance", s.instanceName),
		)

		// Record the SGA shared pool dictionary cache miss ratio metric
		s.mb.RecordNewrelicoracledbSgaSharedPoolDictCacheMissRatioDataPoint(now, ratio, s.instanceName, instanceID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA shared pool dictionary cache miss ratio metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle SGA shared pool dictionary cache miss ratio metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeSGALogBufferSpaceWaitsMetrics handles the SGA log buffer space waits metrics
func (s *CoreScraper) scrapeSGALogBufferSpaceWaitsMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	// Execute SGA log buffer space waits metrics query
	s.logger.Debug("Executing SGA log buffer space waits metrics query", zap.String("sql", queries.SGALogBufferSpaceWaitsSQL))

	rows, err := s.db.QueryContext(ctx, queries.SGALogBufferSpaceWaitsSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing SGA log buffer space waits metrics query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var count int64
		var instID interface{}

		err := rows.Scan(&count, &instID)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA log buffer space waits metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Record SGA log buffer space waits metrics
		s.logger.Info("SGA log buffer space waits metrics collected",
			zap.String("instance_id", instanceID),
			zap.Int64("sga_log_buffer_space_waits", count),
			zap.String("instance", s.instanceName),
		)

		// Record the SGA log buffer space waits metric
		s.mb.RecordNewrelicoracledbSgaLogBufferSpaceWaitsDataPoint(now, count, s.instanceName, instanceID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA log buffer space waits metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle SGA log buffer space waits metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeSGALogAllocRetriesMetrics handles the SGA log allocation retries metrics
func (s *CoreScraper) scrapeSGALogAllocRetriesMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error
	metricCount := 0

	// Execute SGA log allocation retries query
	s.logger.Debug("Executing SGA log allocation retries query", zap.String("sql", queries.SGALogAllocRetriesSQL))
	rows, err := s.db.QueryContext(ctx, queries.SGALogAllocRetriesSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing SGA log allocation retries query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process query results
	for rows.Next() {
		var ratio sql.NullFloat64
		var instID interface{}

		err := rows.Scan(&ratio, &instID)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA log allocation retries metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Handle NULL ratio values
		ratioValue := 0.0
		if ratio.Valid {
			ratioValue = ratio.Float64
		}

		// Record SGA log allocation retries metrics
		s.logger.Info("SGA log allocation retries metrics collected",
			zap.String("instance_id", instanceID),
			zap.Float64("sga_log_allocation_retries_ratio", ratioValue),
			zap.String("instance", s.instanceName),
		)

		// Record the SGA log allocation retries metric
		s.mb.RecordNewrelicoracledbSgaLogAllocationRetriesRatioDataPoint(now, ratioValue, s.instanceName, instanceID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA log allocation retries metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle SGA log allocation retries metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeSGAHitRatioMetrics handles the SGA hit ratio metrics
func (s *CoreScraper) scrapeSGAHitRatioMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error
	metricCount := 0

	// Execute SGA hit ratio query
	s.logger.Debug("Executing SGA hit ratio query", zap.String("sql", queries.SGAHitRatioSQL))
	rows, err := s.db.QueryContext(ctx, queries.SGAHitRatioSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing SGA hit ratio query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process query results
	for rows.Next() {
		var instID interface{}
		var ratio sql.NullFloat64

		err := rows.Scan(&instID, &ratio)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA hit ratio metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Handle NULL ratio values
		ratioValue := 0.0
		if ratio.Valid {
			ratioValue = ratio.Float64
		}

		// Record SGA hit ratio metrics
		s.logger.Info("SGA hit ratio metrics collected",
			zap.String("instance_id", instanceID),
			zap.Float64("sga_hit_ratio", ratioValue),
			zap.String("instance", s.instanceName),
		)

		// Record the SGA hit ratio metric
		s.mb.RecordNewrelicoracledbSgaHitRatioDataPoint(now, ratioValue, s.instanceName, instanceID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA hit ratio metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle SGA hit ratio metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}
