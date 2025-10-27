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

// scrapePGAMetrics handles the PGA memory metrics
func (s *CoreScraper) scrapePGAMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	// Execute PGA metrics query
	s.logger.Debug("Executing PGA metrics query", zap.String("sql", queries.PGAMetricsSQL))

	rows, err := s.db.QueryContext(ctx, queries.PGAMetricsSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing PGA metrics query: %w", err))
		return errors
	}
	defer rows.Close()

	// Track metrics by instance ID to collect all PGA metrics for each instance
	instanceMetrics := make(map[string]map[string]int64)

	// Process each row and collect metrics
	for rows.Next() {
		var instID interface{}
		var name string
		var value float64

		err := rows.Scan(&instID, &name, &value)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning PGA metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Initialize the instance metrics map if needed
		if instanceMetrics[instanceID] == nil {
			instanceMetrics[instanceID] = make(map[string]int64)
		}

		// Store the metric value
		instanceMetrics[instanceID][name] = int64(value)
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating PGA metrics rows: %w", err))
	}

	// Record metrics for each instance
	metricCount := 0
	for instanceID, metrics := range instanceMetrics {
		// Record PGA memory metrics
		s.logger.Info("PGA memory metrics collected",
			zap.String("instance_id", instanceID),
			zap.Int64("pga_in_use", metrics["total PGA inuse"]),
			zap.Int64("pga_allocated", metrics["total PGA allocated"]),
			zap.Int64("pga_freeable", metrics["total freeable PGA memory"]),
			zap.Int64("pga_max_size", metrics["global memory bound"]),
			zap.String("instance", s.instanceName),
		)

		// Record each PGA metric if it exists
		if val, exists := metrics["total PGA inuse"]; exists {
			s.mb.RecordNewrelicoracledbMemoryPgaInUseBytesDataPoint(now, val, s.instanceName, instanceID)
		}
		if val, exists := metrics["total PGA allocated"]; exists {
			s.mb.RecordNewrelicoracledbMemoryPgaAllocatedBytesDataPoint(now, val, s.instanceName, instanceID)
		}
		if val, exists := metrics["total freeable PGA memory"]; exists {
			s.mb.RecordNewrelicoracledbMemoryPgaFreeableBytesDataPoint(now, val, s.instanceName, instanceID)
		}
		if val, exists := metrics["global memory bound"]; exists {
			s.mb.RecordNewrelicoracledbMemoryPgaMaxSizeBytesDataPoint(now, val, s.instanceName, instanceID)
		}

		metricCount++
	}

	s.logger.Debug("Collected Oracle PGA memory metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeSGAUGATotalMemoryMetrics handles the SGA UGA total memory metrics
func (s *CoreScraper) scrapeSGAUGATotalMemoryMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	// Execute SGA UGA total memory metrics query
	s.logger.Debug("Executing SGA UGA total memory metrics query", zap.String("sql", queries.SGAUGATotalMemorySQL))

	rows, err := s.db.QueryContext(ctx, queries.SGAUGATotalMemorySQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing SGA UGA total memory metrics query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var sum int64
		var instID interface{}

		err := rows.Scan(&sum, &instID)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA UGA total memory metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Record SGA UGA total memory metrics
		s.logger.Info("SGA UGA total memory metrics collected",
			zap.String("instance_id", instanceID),
			zap.Int64("sga_uga_total_bytes", sum),
			zap.String("instance", s.instanceName),
		)

		// Record the SGA UGA total memory metric
		s.mb.RecordNewrelicoracledbMemorySgaUgaTotalBytesDataPoint(now, sum, s.instanceName, instanceID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA UGA total memory metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle SGA UGA total memory metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeSGASharedPoolLibraryCacheMetrics handles the SGA shared pool library cache sharable memory metrics
func (s *CoreScraper) scrapeSGASharedPoolLibraryCacheMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	// Execute SGA shared pool library cache metrics query
	s.logger.Debug("Executing SGA shared pool library cache metrics query", zap.String("sql", queries.SGASharedPoolLibraryCacheShareableStatementSQL))

	rows, err := s.db.QueryContext(ctx, queries.SGASharedPoolLibraryCacheShareableStatementSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing SGA shared pool library cache metrics query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var sum int64
		var instID interface{}

		err := rows.Scan(&sum, &instID)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA shared pool library cache metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Record SGA shared pool library cache metrics
		s.logger.Info("SGA shared pool library cache metrics collected",
			zap.String("instance_id", instanceID),
			zap.Int64("sga_shared_pool_library_cache_sharable_bytes", sum),
			zap.String("instance", s.instanceName),
		)

		// Record the SGA shared pool library cache metric
		s.mb.RecordNewrelicoracledbMemorySgaSharedPoolLibraryCacheSharableBytesDataPoint(now, sum, s.instanceName, instanceID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA shared pool library cache metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle SGA shared pool library cache metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeSGASharedPoolLibraryCacheUserMetrics handles the SGA shared pool library cache shareable user memory metrics
func (s *CoreScraper) scrapeSGASharedPoolLibraryCacheUserMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	// Execute SGA shared pool library cache user metrics query
	s.logger.Debug("Executing SGA shared pool library cache user metrics query", zap.String("sql", queries.SGASharedPoolLibraryCacheShareableUserSQL))

	rows, err := s.db.QueryContext(ctx, queries.SGASharedPoolLibraryCacheShareableUserSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing SGA shared pool library cache user metrics query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var sum int64
		var instID interface{}

		err := rows.Scan(&sum, &instID)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA shared pool library cache user metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Record SGA shared pool library cache user metrics
		s.logger.Info("SGA shared pool library cache user metrics collected",
			zap.String("instance_id", instanceID),
			zap.Int64("sga_shared_pool_library_cache_user_bytes", sum),
			zap.String("instance", s.instanceName),
		)

		// Record the SGA shared pool library cache user metric
		s.mb.RecordNewrelicoracledbMemorySgaSharedPoolLibraryCacheUserBytesDataPoint(now, sum, s.instanceName, instanceID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA shared pool library cache user metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle SGA shared pool library cache user metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeSGAMetrics handles the SGA metrics
func (s *CoreScraper) scrapeSGAMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error
	metricCount := 0

	// Execute SGA query
	s.logger.Debug("Executing SGA query", zap.String("sql", queries.SGASQL))
	rows, err := s.db.QueryContext(ctx, queries.SGASQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing SGA query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process query results
	for rows.Next() {
		var instID interface{}
		var name string
		var value sql.NullInt64

		err := rows.Scan(&instID, &name, &value)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Handle NULL values
		valueInt := int64(0)
		if value.Valid {
			valueInt = value.Int64
		}

		// Record appropriate metric based on the name
		switch name {
		case "Fixed Size":
			s.logger.Info("SGA fixed size metrics collected",
				zap.String("instance_id", instanceID),
				zap.Int64("sga_fixed_size_bytes", valueInt),
				zap.String("instance", s.instanceName),
			)
			s.mb.RecordNewrelicoracledbSgaFixedSizeBytesDataPoint(now, valueInt, s.instanceName, instanceID)

		case "Redo Buffers":
			s.logger.Info("SGA redo buffers metrics collected",
				zap.String("instance_id", instanceID),
				zap.Int64("sga_redo_buffers_bytes", valueInt),
				zap.String("instance", s.instanceName),
			)
			s.mb.RecordNewrelicoracledbSgaRedoBuffersBytesDataPoint(now, valueInt, s.instanceName, instanceID)

		default:
			s.logger.Warn("Unknown SGA metric name", zap.String("name", name), zap.String("instance_id", instanceID))
		}

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle SGA metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}
