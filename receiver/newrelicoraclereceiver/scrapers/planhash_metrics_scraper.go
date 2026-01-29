// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	commonutils "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/common-utils"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// PlanHashMetricsScraper collects Oracle plan hash metrics
type PlanHashMetricsScraper struct {
	client               client.OracleClient
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

// NewPlanHashMetricsScraper creates a new Plan Hash Metrics Scraper instance
func NewPlanHashMetricsScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, metricsBuilderConfig metadata.MetricsBuilderConfig) (*PlanHashMetricsScraper, error) {
	if oracleClient == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}
	if mb == nil {
		return nil, fmt.Errorf("metrics builder cannot be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	return &PlanHashMetricsScraper{
		client:               oracleClient,
		mb:                   mb,
		logger:               logger,
		metricsBuilderConfig: metricsBuilderConfig,
	}, nil
}

// ScrapePlanHashMetrics collects plan hash metrics for the given SQL identifiers
func (s *PlanHashMetricsScraper) ScrapePlanHashMetrics(ctx context.Context, slowQueryIdentifiers []models.SQLIdentifier) []error {
	if len(slowQueryIdentifiers) == 0 {
		s.logger.Debug("No slow query identifiers provided, skipping plan hash metrics scraping")
		return nil
	}

	// Create lookup map for metadata
	sqlIDMap := make(map[string]models.SQLIdentifier, len(slowQueryIdentifiers))
	for _, identifier := range slowQueryIdentifiers {
		sqlIDMap[identifier.SQLID] = identifier
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	var errors []error

	// Query plan hash metrics for each SQL_ID
	for _, identifier := range slowQueryIdentifiers {
		planHashMetrics, err := s.client.QueryPlanHashMetrics(ctx, identifier.SQLID)
		if err != nil {
			s.logger.Error("Failed to query plan hash metrics",
				zap.String("sql_id", identifier.SQLID),
				zap.Error(err))
			errors = append(errors, err)
			continue
		}

		// Emit metrics for each plan hash
		for _, metrics := range planHashMetrics {
			s.recordPlanHashMetrics(now, &metrics, sqlIDMap)
		}
	}

	s.logger.Debug("Plan hash metrics scrape completed",
		zap.Int("sql_ids_processed", len(slowQueryIdentifiers)),
		zap.Int("errors", len(errors)))

	return errors
}

// recordPlanHashMetrics records plan hash metrics
func (s *PlanHashMetricsScraper) recordPlanHashMetrics(now pcommon.Timestamp, metrics *models.PlanHashMetrics, sqlIDMap map[string]models.SQLIdentifier) {
	if !metrics.HasValidIdentifier() {
		return
	}

	collectionTimestamp := commonutils.FormatTimestamp(metrics.GetCollectionTimestamp())
	sqlID := metrics.GetSQLID()
	planHashValue := commonutils.FormatInt64(metrics.GetPlanHashValue())
	totalExecutions := metrics.GetTotalExecutions()
	avgElapsedTimeMs := metrics.GetAvgElapsedTimeMs()
	avgCPUTimeMs := metrics.GetAvgCPUTimeMs()
	avgDiskReads := metrics.GetAvgDiskReads()
	avgBufferGets := metrics.GetAvgBufferGets()
	avgRowsReturned := metrics.GetAvgRowsReturned()
	firstLoadTime := metrics.GetFirstLoadTime()                        // VARCHAR2 - already a string
	lastActiveTime := commonutils.FormatTimestamp(metrics.GetLastActiveTime()) // DATE - needs formatting

	// Get client_name, transaction_name, and normalised_sql_hash from sqlIDMap
	// These will be empty strings if not present in the map or if the metadata values were empty
	var clientName, transactionName, normalisedSQLHash string
	if metadata, exists := sqlIDMap[sqlID]; exists {
		clientName = metadata.ClientName
		transactionName = metadata.TransactionName
		normalisedSQLHash = metadata.NormalisedSQLHash
	}

	// Record all plan hash performance metrics (without timestamps to avoid high cardinality)
	s.mb.RecordNewrelicoracledbPlanHashMetricsExecutionsDataPoint(
		now, totalExecutions,
		collectionTimestamp, sqlID, planHashValue,
		clientName, transactionName, normalisedSQLHash,
	)

	s.mb.RecordNewrelicoracledbPlanHashMetricsAvgElapsedTimeMsDataPoint(
		now, avgElapsedTimeMs,
		collectionTimestamp, sqlID, planHashValue,
		clientName, transactionName, normalisedSQLHash,
	)

	s.mb.RecordNewrelicoracledbPlanHashMetricsAvgCPUTimeMsDataPoint(
		now, avgCPUTimeMs,
		collectionTimestamp, sqlID, planHashValue,
		clientName, transactionName, normalisedSQLHash,
	)

	s.mb.RecordNewrelicoracledbPlanHashMetricsAvgDiskReadsDataPoint(
		now, avgDiskReads,
		collectionTimestamp, sqlID, planHashValue,
		clientName, transactionName, normalisedSQLHash,
	)

	s.mb.RecordNewrelicoracledbPlanHashMetricsAvgBufferGetsDataPoint(
		now, avgBufferGets,
		collectionTimestamp, sqlID, planHashValue,
		clientName, transactionName, normalisedSQLHash,
	)

	s.mb.RecordNewrelicoracledbPlanHashMetricsAvgRowsReturnedDataPoint(
		now, avgRowsReturned,
		collectionTimestamp, sqlID, planHashValue,
		clientName, transactionName, normalisedSQLHash,
	)

	// Record plan hash details metric with timestamps (similar to slow_queries.query_details)
	// This avoids high cardinality by separating timestamp attributes into a dedicated metric
	s.mb.RecordNewrelicoracledbPlanHashMetricsDetailsDataPoint(
		now, 1, // constant value of 1
		collectionTimestamp, sqlID, planHashValue,
		clientName, transactionName, normalisedSQLHash,
		firstLoadTime, lastActiveTime,
	)
}
