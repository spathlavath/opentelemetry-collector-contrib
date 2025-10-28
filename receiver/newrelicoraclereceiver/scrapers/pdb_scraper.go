// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

type PdbScraper struct {
	client             client.OracleClient
	mb                 *metadata.MetricsBuilder
	logger             *zap.Logger
	instanceName       string
	config             metadata.MetricsBuilderConfig
	isCDBCapable       *bool
	environmentChecked bool
	detectionMutex     sync.RWMutex
}

// NewPdbScraper creates a new PDB scraper
func NewPdbScraper(c client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, config metadata.MetricsBuilderConfig) *PdbScraper {
	return &PdbScraper{
		client:       c,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		config:       config,
	}
}

func (s *PdbScraper) ScrapePdbMetrics(ctx context.Context) []error {
	if err := s.checkCDBCapability(ctx); err != nil {
		return []error{err}
	}

	if !s.isCDBSupported() {
		return nil
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	return s.scrapePDBSysMetrics(ctx, now)
}

func (s *PdbScraper) scrapePDBSysMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	metrics, err := s.client.QueryPDBSysMetrics(ctx)
	if err != nil {
		return []error{err}
	}

	metricCount := 0
	for _, metric := range metrics {
		instanceIDStr := strconv.Itoa(metric.InstID)
		s.recordMetric(now, metric.MetricName, metric.Value, instanceIDStr)
		metricCount++
	}

	s.logger.Debug("Collected PDB sys metrics", zap.Int("count", metricCount))

	return nil
}

func (s *PdbScraper) recordMetric(now pcommon.Timestamp, metricName string, value float64, instanceID string) {
	switch metricName {
	case "Active Parallel Sessions":
		s.mb.RecordNewrelicoracledbPdbActiveParallelSessionsDataPoint(now, value, s.instanceName, instanceID)
	case "Active Serial Sessions":
		s.mb.RecordNewrelicoracledbPdbActiveSerialSessionsDataPoint(now, value, s.instanceName, instanceID)
	case "Average Active Sessions":
		s.mb.RecordNewrelicoracledbPdbAverageActiveSessionsDataPoint(now, value, s.instanceName, instanceID)
	case "Background CPU Usage Per Sec":
		s.mb.RecordNewrelicoracledbPdbBackgroundCPUUsagePerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Background Time Per Sec":
		s.mb.RecordNewrelicoracledbPdbBackgroundTimePerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "CPU Usage Per Sec":
		s.mb.RecordNewrelicoracledbPdbCPUUsagePerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "CPU Usage Per Txn":
		s.mb.RecordNewrelicoracledbPdbCPUUsagePerTransactionDataPoint(now, value, s.instanceName, instanceID)
	case "Current Logons Count":
		s.mb.RecordNewrelicoracledbPdbCurrentLogonsDataPoint(now, value, s.instanceName, instanceID)
	case "Current Open Cursors Count":
		s.mb.RecordNewrelicoracledbPdbCurrentOpenCursorsDataPoint(now, value, s.instanceName, instanceID)
	case "Database CPU Time Ratio":
		s.mb.RecordNewrelicoracledbPdbCPUTimeRatioDataPoint(now, value, s.instanceName, instanceID)
	case "Database Wait Time Ratio":
		s.mb.RecordNewrelicoracledbPdbWaitTimeRatioDataPoint(now, value, s.instanceName, instanceID)
	case "DB Block Changes Per Sec":
		s.mb.RecordNewrelicoracledbPdbBlockChangesPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "DB Block Changes Per Txn":
		s.mb.RecordNewrelicoracledbPdbBlockChangesPerTransactionDataPoint(now, value, s.instanceName, instanceID)
	case "Executions Per Sec":
		s.mb.RecordNewrelicoracledbPdbExecutionsPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Executions Per Txn":
		s.mb.RecordNewrelicoracledbPdbExecutionsPerTransactionDataPoint(now, value, s.instanceName, instanceID)
	case "Hard Parse Count Per Sec":
		s.mb.RecordNewrelicoracledbPdbHardParseCountPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Hard Parse Count Per Txn":
		s.mb.RecordNewrelicoracledbPdbHardParseCountPerTransactionDataPoint(now, value, s.instanceName, instanceID)
	case "Logical Reads Per Sec":
		s.mb.RecordNewrelicoracledbPdbLogicalReadsPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Logical Reads Per Txn":
		s.mb.RecordNewrelicoracledbPdbLogicalReadsPerTransactionDataPoint(now, value, s.instanceName, instanceID)
	case "Logons Per Txn":
		s.mb.RecordNewrelicoracledbPdbLogonsPerTransactionDataPoint(now, value, s.instanceName, instanceID)
	case "Network Traffic Volume Per Sec":
		s.mb.RecordNewrelicoracledbPdbNetworkTrafficBytePerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Open Cursors Per Sec":
		s.mb.RecordNewrelicoracledbPdbOpenCursorsPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Open Cursors Per Txn":
		s.mb.RecordNewrelicoracledbPdbOpenCursorsPerTransactionDataPoint(now, value, s.instanceName, instanceID)
	case "Parse Failure Count Per Sec":
		s.mb.RecordNewrelicoracledbPdbParseFailureCountPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Physical Read Total Bytes Per Sec":
		s.mb.RecordNewrelicoracledbPdbPhysicalReadBytesPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Physical Reads Per Txn":
		s.mb.RecordNewrelicoracledbPdbPhysicalReadsPerTransactionDataPoint(now, value, s.instanceName, instanceID)
	case "Physical Write Total Bytes Per Sec":
		s.mb.RecordNewrelicoracledbPdbPhysicalWriteBytesPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Physical Writes Per Txn":
		s.mb.RecordNewrelicoracledbPdbPhysicalWritesPerTransactionDataPoint(now, value, s.instanceName, instanceID)
	case "Redo Generated Per Sec":
		s.mb.RecordNewrelicoracledbPdbRedoGeneratedBytesPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Redo Generated Per Txn":
		s.mb.RecordNewrelicoracledbPdbRedoGeneratedBytesPerTransactionDataPoint(now, value, s.instanceName, instanceID)
	case "Response Time Per Txn":
		s.mb.RecordNewrelicoracledbPdbResponseTimePerTransactionDataPoint(now, value, s.instanceName, instanceID)
	case "Session Count":
		s.mb.RecordNewrelicoracledbPdbSessionCountDataPoint(now, value, s.instanceName, instanceID)
	case "Soft Parse Ratio":
		s.mb.RecordNewrelicoracledbPdbSoftParseRatioDataPoint(now, value, s.instanceName, instanceID)
	case "SQL Service Response Time":
		s.mb.RecordNewrelicoracledbPdbSQLServiceResponseTimeDataPoint(now, value, s.instanceName, instanceID)
	case "Total Parse Count Per Sec":
		s.mb.RecordNewrelicoracledbPdbTotalParseCountPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Total Parse Count Per Txn":
		s.mb.RecordNewrelicoracledbPdbTotalParseCountPerTransactionDataPoint(now, value, s.instanceName, instanceID)
	case "User Calls Per Sec":
		s.mb.RecordNewrelicoracledbPdbUserCallsPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "User Calls Per Txn":
		s.mb.RecordNewrelicoracledbPdbUserCallsPerTransactionDataPoint(now, value, s.instanceName, instanceID)
	case "User Commits Per Sec":
		s.mb.RecordNewrelicoracledbPdbUserCommitsPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "User Commits Percentage":
		s.mb.RecordNewrelicoracledbPdbUserCommitsPercentageDataPoint(now, value, s.instanceName, instanceID)
	case "User Rollbacks Per Sec":
		s.mb.RecordNewrelicoracledbPdbUserRollbacksPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "User Rollbacks Percentage":
		s.mb.RecordNewrelicoracledbPdbUserRollbacksPercentageDataPoint(now, value, s.instanceName, instanceID)
	case "User Transaction Per Sec":
		s.mb.RecordNewrelicoracledbPdbTransactionsPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Execute Without Parse Ratio":
		s.mb.RecordNewrelicoracledbPdbExecuteWithoutParseRatioDataPoint(now, value, s.instanceName, instanceID)
	case "Logons Per Sec":
		s.mb.RecordNewrelicoracledbPdbLogonsPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Physical Read Bytes Per Sec":
		s.mb.RecordNewrelicoracledbPdbDbPhysicalReadBytesPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Physical Reads Per Sec":
		s.mb.RecordNewrelicoracledbPdbDbPhysicalReadsPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Physical Write Bytes Per Sec":
		s.mb.RecordNewrelicoracledbPdbDbPhysicalWriteBytesPerSecondDataPoint(now, value, s.instanceName, instanceID)
	case "Physical Writes Per Sec":
		s.mb.RecordNewrelicoracledbPdbDbPhysicalWritesPerSecondDataPoint(now, value, s.instanceName, instanceID)
	default:
		s.logger.Debug("Unknown PDB metric", zap.String("metric_name", metricName))
	}
}

// CDB capability detection methods

// checkCDBCapability checks if the Oracle database supports CDB features
func (s *PdbScraper) checkCDBCapability(ctx context.Context) error {
	s.detectionMutex.Lock()
	defer s.detectionMutex.Unlock()

	if s.environmentChecked {
		return nil
	}

	capability, err := s.client.QueryCDBCapability(ctx)
	if err != nil {
		return err
	}

	cdbCapable := capability.IsCDB == 1
	s.isCDBCapable = &cdbCapable
	s.environmentChecked = true

	return nil
}

func (s *PdbScraper) isCDBSupported() bool {
	s.detectionMutex.RLock()
	defer s.detectionMutex.RUnlock()
	return s.isCDBCapable != nil && *s.isCDBCapable
}

func containsORACode(err error, oraCodes ...string) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	for _, code := range oraCodes {
		if strings.Contains(errStr, code) {
			return true
		}
	}
	return false
}
