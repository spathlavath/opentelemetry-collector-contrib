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

func (s *CoreScraper) scrapeReadWriteMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	if !s.config.Metrics.NewrelicoracledbDiskReads.Enabled &&
		!s.config.Metrics.NewrelicoracledbDiskWrites.Enabled &&
		!s.config.Metrics.NewrelicoracledbDiskBlocksRead.Enabled &&
		!s.config.Metrics.NewrelicoracledbDiskBlocksWritten.Enabled &&
		!s.config.Metrics.NewrelicoracledbDiskReadTimeMilliseconds.Enabled &&
		!s.config.Metrics.NewrelicoracledbDiskWriteTimeMilliseconds.Enabled {
		return nil
	}

	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.ReadWriteMetricsSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing read/write metrics query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var instID interface{}
		var physicalReads, physicalWrites, physicalBlockReads, physicalBlockWrites, readTime, writeTime int64

		if err := rows.Scan(&instID, &physicalReads, &physicalWrites, &physicalBlockReads, &physicalBlockWrites, &readTime, &writeTime); err != nil {
			errors = append(errors, fmt.Errorf("error scanning read/write metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)

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

	s.logger.Debug("Disk I/O metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}
