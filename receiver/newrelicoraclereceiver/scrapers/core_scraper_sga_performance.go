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

func (s *CoreScraper) scrapeSGASharedPoolLibraryCacheReloadRatioMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.SGASharedPoolLibraryCacheReloadRatioSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing SGA shared pool library cache reload ratio metrics query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var ratio float64
		var instID interface{}

		if err := rows.Scan(&ratio, &instID); err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA shared pool library cache reload ratio metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)
		s.mb.RecordNewrelicoracledbSgaSharedPoolLibraryCacheReloadRatioDataPoint(now, ratio, s.instanceName, instanceID)
		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA shared pool library cache reload ratio metrics rows: %w", err))
	}

	s.logger.Debug("SGA shared pool library cache reload ratio metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}

func (s *CoreScraper) scrapeSGASharedPoolLibraryCacheHitRatioMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.SGASharedPoolLibraryCacheHitRatioSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing SGA shared pool library cache hit ratio metrics query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var ratio float64
		var instID interface{}

		if err := rows.Scan(&ratio, &instID); err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA shared pool library cache hit ratio metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)
		s.mb.RecordNewrelicoracledbSgaSharedPoolLibraryCacheHitRatioDataPoint(now, ratio, s.instanceName, instanceID)
		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA shared pool library cache hit ratio metrics rows: %w", err))
	}

	s.logger.Debug("SGA shared pool library cache hit ratio metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}

func (s *CoreScraper) scrapeSGASharedPoolDictCacheMissRatioMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.SGASharedPoolDictCacheMissRatioSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing SGA shared pool dictionary cache miss ratio metrics query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var ratio float64
		var instID interface{}

		if err := rows.Scan(&ratio, &instID); err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA shared pool dictionary cache miss ratio metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)
		s.mb.RecordNewrelicoracledbSgaSharedPoolDictCacheMissRatioDataPoint(now, ratio, s.instanceName, instanceID)
		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA shared pool dictionary cache miss ratio metrics rows: %w", err))
	}

	s.logger.Debug("SGA shared pool dictionary cache miss ratio metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}

func (s *CoreScraper) scrapeSGALogBufferSpaceWaitsMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.SGALogBufferSpaceWaitsSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing SGA log buffer space waits metrics query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var count int64
		var instID interface{}

		if err := rows.Scan(&count, &instID); err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA log buffer space waits metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)
		s.mb.RecordNewrelicoracledbSgaLogBufferSpaceWaitsDataPoint(now, count, s.instanceName, instanceID)
		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA log buffer space waits metrics rows: %w", err))
	}

	s.logger.Debug("SGA log buffer space waits metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}

func (s *CoreScraper) scrapeSGALogAllocRetriesMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.SGALogAllocRetriesSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing SGA log allocation retries query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var ratio sql.NullFloat64
		var instID interface{}

		if err := rows.Scan(&ratio, &instID); err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA log allocation retries metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)

		ratioValue := 0.0
		if ratio.Valid {
			ratioValue = ratio.Float64
		}

		s.mb.RecordNewrelicoracledbSgaLogAllocationRetriesRatioDataPoint(now, ratioValue, s.instanceName, instanceID)
		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA log allocation retries metrics rows: %w", err))
	}

	s.logger.Debug("SGA log allocation retries metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}

func (s *CoreScraper) scrapeSGAHitRatioMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.SGAHitRatioSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing SGA hit ratio query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var instID interface{}
		var ratio sql.NullFloat64

		if err := rows.Scan(&instID, &ratio); err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA hit ratio metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)

		ratioValue := 0.0
		if ratio.Valid {
			ratioValue = ratio.Float64
		}

		s.mb.RecordNewrelicoracledbSgaHitRatioDataPoint(now, ratioValue, s.instanceName, instanceID)
		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA hit ratio metrics rows: %w", err))
	}

	s.logger.Debug("SGA hit ratio metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}
