// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// SlowQueryScraper handles Oracle slow query metrics collection
// Based on the original collectSlowQueryMetrics implementation
type SlowQueryScraper struct {
	logger *zap.Logger
}

// NewSlowQueryScraper creates a new slow query scraper
func NewSlowQueryScraper(logger *zap.Logger) *SlowQueryScraper {
	return &SlowQueryScraper{
		logger: logger,
	}
}

// InstanceInfo represents Oracle instance information for resource attributes
type InstanceInfo struct {
	InstanceID   string
	InstanceName string
	GlobalName   string
	DbID         string
}

// SetResourceAttributes sets resource attributes based on instance information
func (i *InstanceInfo) SetResourceAttributes(res pcommon.Resource) {
	res.Attributes().PutStr("newrelic.oracle.instance.id", i.InstanceID)
	res.Attributes().PutStr("newrelic.oracle.instance.name", i.InstanceName)
	res.Attributes().PutStr("newrelic.oracle.global.name", i.GlobalName)
	res.Attributes().PutStr("newrelic.oracle.db.id", i.DbID)
}

// CollectSlowQueryMetrics collects metrics related to Oracle database slow queries.
// It returns a pmetric.Metrics object containing a single metric (OracleSlowQuerySample)
// with all slow query attributes as one data point per query.
// This implementation matches your original collectSlowQueryMetrics function.
func (s *SlowQueryScraper) CollectSlowQueryMetrics(db *sql.DB, skipGroups []string, instanceInfo *InstanceInfo) (pmetric.Metrics, error) {
	// Skip collection if slow query metrics are in the skip list
	for _, skipGroup := range skipGroups {
		if skipGroup == "slow_query_metrics" {
			s.logger.Info("Skipping slow query metrics collection as configured in skip_metrics_groups")
			return pmetric.NewMetrics(), nil
		}
	}

	s.logger.Debug("Executing slow query metrics SQL", zap.String("query", queries.SlowQuerySQL))

	rows, err := db.Query(queries.SlowQuerySQL)
	if err != nil {
		s.logger.Error("Failed to execute slow query metrics SQL", zap.Error(err))
		return pmetric.NewMetrics(), fmt.Errorf("error collecting slow query metrics: %w", err)
	}
	s.logger.Debug("Successfully executed slow query metrics SQL")
	defer rows.Close()

	now := pcommon.NewTimestampFromTime(time.Now())

	// Create a map to track unique queries by queryID
	uniqueQueries := make(map[string]struct {
		dbName           string
		schemaName       string
		stmtType         string
		execCount        int64
		avgCpuTimeMS     float64
		avgDiskReads     int64
		avgElapsedTimeMS float64
		hasFullTableScan string
		queryText        string
	})

	// First pass - collect unique queries by query_id
	for rows.Next() {
		var dbName, queryID, schemaName, stmtType, collectionTimestamp, lastExecTimestamp, queryText, hasFullTableScan string
		var execCount, avgDiskReads int64
		var avgCpuTimeMS, avgElapsedTimeMS float64

		if err := rows.Scan(&dbName, &queryID, &schemaName, &stmtType, &execCount, &collectionTimestamp,
			&lastExecTimestamp, &queryText, &avgCpuTimeMS, &avgDiskReads, &avgElapsedTimeMS, &hasFullTableScan); err != nil {
			if strings.Contains(err.Error(), "STATEMENT_TYPE") && strings.Contains(err.Error(), "NULL") {
				rows.Scan(&dbName, &queryID, &schemaName, nil, &execCount, &collectionTimestamp,
					&lastExecTimestamp, &queryText, &avgCpuTimeMS, &avgDiskReads, &avgElapsedTimeMS, &hasFullTableScan)
				stmtType = "UNKNOWN"
			} else {
				s.logger.Warn("Error scanning slow query metrics row", zap.Error(err))
				continue
			}
		}

		if entry, exists := uniqueQueries[queryID]; !exists || avgElapsedTimeMS > entry.avgElapsedTimeMS {
			uniqueQueries[queryID] = struct {
				dbName           string
				schemaName       string
				stmtType         string
				execCount        int64
				avgCpuTimeMS     float64
				avgDiskReads     int64
				avgElapsedTimeMS float64
				hasFullTableScan string
				queryText        string
			}{
				dbName:           dbName,
				schemaName:       schemaName,
				stmtType:         stmtType,
				execCount:        execCount,
				avgCpuTimeMS:     avgCpuTimeMS,
				avgDiskReads:     avgDiskReads,
				avgElapsedTimeMS: avgElapsedTimeMS,
				hasFullTableScan: hasFullTableScan,
				queryText:        queryText,
			}
		}
	}

	// Create a single metric for all slow query samples
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()

	// Set resource attributes from instance info
	if instanceInfo != nil {
		instanceInfo.SetResourceAttributes(rm.Resource())
	}

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver")
	sm.Scope().SetVersion("0.1.0")

	slowQueryCount := 0

	sampleMetric := sm.Metrics().AppendEmpty()
	sampleMetric.SetName("OracleSlowQuerySample")
	sampleMetric.SetDescription("Sample of Oracle slow query with all metrics as attributes")
	sampleMetric.SetUnit("1")
	sampleGauge := sampleMetric.SetEmptyGauge()

	for queryID, query := range uniqueQueries {
		if query.avgElapsedTimeMS < 0 || query.avgCpuTimeMS < 0 || query.avgDiskReads < 0 || query.execCount <= 0 {
			s.logger.Warn("Skipping slow query with invalid metrics",
				zap.String("query_id", queryID),
				zap.String("schema_name", query.schemaName),
				zap.Float64("avg_elapsed_time_ms", query.avgElapsedTimeMS),
				zap.Float64("avg_cpu_time_ms", query.avgCpuTimeMS),
				zap.Int64("avg_disk_reads", query.avgDiskReads),
				zap.Int64("execution_count", query.execCount))
			continue
		}

		dp := sampleGauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(now)
		dp.SetDoubleValue(query.avgElapsedTimeMS)

		dp.Attributes().PutStr("query_id", queryID)
		dp.Attributes().PutStr("schema_name", query.schemaName)
		dp.Attributes().PutStr("database_name", query.dbName)
		dp.Attributes().PutStr("statement_type", query.stmtType)
		dp.Attributes().PutStr("query_text", query.queryText)
		dp.Attributes().PutDouble("avg_cpu_time_ms", query.avgCpuTimeMS)
		dp.Attributes().PutInt("avg_disk_reads", query.avgDiskReads)
		dp.Attributes().PutDouble("avg_elapsed_time_ms", query.avgElapsedTimeMS)
		dp.Attributes().PutInt("execution_count", query.execCount)
		dp.Attributes().PutStr("has_full_table_scan", query.hasFullTableScan)

		slowQueryCount++
	}

	s.logger.Info("OracleSlowQuerySample metrics collection completed", zap.Int("total_slow_queries", slowQueryCount))

	return metrics, nil
}
