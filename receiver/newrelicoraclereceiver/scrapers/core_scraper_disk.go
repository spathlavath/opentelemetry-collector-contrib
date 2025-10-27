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

// scrapeReadWriteMetrics handles the disk read/write I/O metrics
func (s *CoreScraper) scrapeReadWriteMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	// Check if any disk I/O metrics are enabled
	if !s.config.Metrics.NewrelicoracledbDiskReads.Enabled &&
		!s.config.Metrics.NewrelicoracledbDiskWrites.Enabled &&
		!s.config.Metrics.NewrelicoracledbDiskBlocksRead.Enabled &&
		!s.config.Metrics.NewrelicoracledbDiskBlocksWritten.Enabled &&
		!s.config.Metrics.NewrelicoracledbDiskReadTimeMilliseconds.Enabled &&
		!s.config.Metrics.NewrelicoracledbDiskWriteTimeMilliseconds.Enabled {
		return nil
	}

	var errors []error

	// Execute read/write metrics query
	s.logger.Debug("Executing read/write metrics query", zap.String("sql", queries.ReadWriteMetricsSQL))

	rows, err := s.db.QueryContext(ctx, queries.ReadWriteMetricsSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing read/write metrics query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var instID interface{}
		var physicalReads, physicalWrites, physicalBlockReads, physicalBlockWrites, readTime, writeTime int64

		err := rows.Scan(&instID, &physicalReads, &physicalWrites, &physicalBlockReads, &physicalBlockWrites, &readTime, &writeTime)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning read/write metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Record disk I/O metrics
		s.logger.Info("Disk I/O metrics collected",
			zap.String("instance_id", instanceID),
			zap.Int64("physical_reads", physicalReads),
			zap.Int64("physical_writes", physicalWrites),
			zap.Int64("physical_block_reads", physicalBlockReads),
			zap.Int64("physical_block_writes", physicalBlockWrites),
			zap.Int64("read_time_ms", readTime),
			zap.Int64("write_time_ms", writeTime),
			zap.String("instance", s.instanceName),
		)

		// Record disk I/O metrics only if enabled
		if s.config.Metrics.NewrelicoracledbDiskReads.Enabled {
			s.mb.RecordNewrelicoracledbDiskReadsDataPoint(now, physicalReads, s.instanceName, instanceID)
		}
		if s.config.Metrics.NewrelicoracledbDiskWrites.Enabled {
			s.mb.RecordNewrelicoracledbDiskWritesDataPoint(now, physicalWrites, s.instanceName, instanceID)
		}
		if s.config.Metrics.NewrelicoracledbDiskBlocksRead.Enabled {
			s.mb.RecordNewrelicoracledbDiskBlocksReadDataPoint(now, physicalBlockReads, s.instanceName, instanceID)
		}
		if s.config.Metrics.NewrelicoracledbDiskBlocksWritten.Enabled {
			s.mb.RecordNewrelicoracledbDiskBlocksWrittenDataPoint(now, physicalBlockWrites, s.instanceName, instanceID)
		}
		if s.config.Metrics.NewrelicoracledbDiskReadTimeMilliseconds.Enabled {
			s.mb.RecordNewrelicoracledbDiskReadTimeMillisecondsDataPoint(now, readTime, s.instanceName, instanceID)
		}
		if s.config.Metrics.NewrelicoracledbDiskWriteTimeMilliseconds.Enabled {
			s.mb.RecordNewrelicoracledbDiskWriteTimeMillisecondsDataPoint(now, writeTime, s.instanceName, instanceID)
		}

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating read/write metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle disk I/O metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}
