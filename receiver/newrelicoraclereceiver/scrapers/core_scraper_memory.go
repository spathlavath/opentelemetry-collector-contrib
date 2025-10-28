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

func (s *CoreScraper) scrapePGAMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.PGAMetricsSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing PGA metrics query: %w", err)}
	}
	defer rows.Close()

	instanceMetrics := make(map[string]map[string]int64)

	for rows.Next() {
		var instID interface{}
		var name string
		var value float64

		if err := rows.Scan(&instID, &name, &value); err != nil {
			errors = append(errors, fmt.Errorf("error scanning PGA metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)

		if instanceMetrics[instanceID] == nil {
			instanceMetrics[instanceID] = make(map[string]int64)
		}

		instanceMetrics[instanceID][name] = int64(value)
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating PGA metrics rows: %w", err))
	}

	metricCount := 0
	for instanceID, metrics := range instanceMetrics {
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

	s.logger.Debug("PGA memory metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}

func (s *CoreScraper) scrapeSGAUGATotalMemoryMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.SGAUGATotalMemorySQL)
	if err != nil {
		return []error{fmt.Errorf("error executing SGA UGA total memory metrics query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var sum int64
		var instID interface{}

		if err := rows.Scan(&sum, &instID); err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA UGA total memory metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)
		s.mb.RecordNewrelicoracledbMemorySgaUgaTotalBytesDataPoint(now, sum, s.instanceName, instanceID)
		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA UGA total memory metrics rows: %w", err))
	}

	s.logger.Debug("SGA UGA total memory metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}

func (s *CoreScraper) scrapeSGASharedPoolLibraryCacheMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.SGASharedPoolLibraryCacheShareableStatementSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing SGA shared pool library cache metrics query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var sum int64
		var instID interface{}

		if err := rows.Scan(&sum, &instID); err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA shared pool library cache metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)
		s.mb.RecordNewrelicoracledbMemorySgaSharedPoolLibraryCacheSharableBytesDataPoint(now, sum, s.instanceName, instanceID)
		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA shared pool library cache metrics rows: %w", err))
	}

	s.logger.Debug("SGA shared pool library cache metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}

func (s *CoreScraper) scrapeSGASharedPoolLibraryCacheUserMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.SGASharedPoolLibraryCacheShareableUserSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing SGA shared pool library cache user metrics query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var sum int64
		var instID interface{}

		if err := rows.Scan(&sum, &instID); err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA shared pool library cache user metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)
		s.mb.RecordNewrelicoracledbMemorySgaSharedPoolLibraryCacheUserBytesDataPoint(now, sum, s.instanceName, instanceID)
		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA shared pool library cache user metrics rows: %w", err))
	}

	s.logger.Debug("SGA shared pool library cache user metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}

func (s *CoreScraper) scrapeSGAMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.SGASQL)
	if err != nil {
		return []error{fmt.Errorf("error executing SGA query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var instID interface{}
		var name string
		var value sql.NullInt64

		if err := rows.Scan(&instID, &name, &value); err != nil {
			errors = append(errors, fmt.Errorf("error scanning SGA metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)

		valueInt := int64(0)
		if value.Valid {
			valueInt = value.Int64
		}

		switch name {
		case "Fixed Size":
			s.mb.RecordNewrelicoracledbSgaFixedSizeBytesDataPoint(now, valueInt, s.instanceName, instanceID)
		case "Redo Buffers":
			s.mb.RecordNewrelicoracledbSgaRedoBuffersBytesDataPoint(now, valueInt, s.instanceName, instanceID)
		default:
			s.logger.Debug("Unknown SGA metric", zap.String("name", name))
		}

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating SGA metrics rows: %w", err))
	}

	s.logger.Debug("SGA metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}
