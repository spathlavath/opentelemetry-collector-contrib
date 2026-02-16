// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"errors"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/common"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

// ReplicationScraper handles MySQL replication metrics collection.
// Only collects data if the instance is configured as a replication slave.
type ReplicationScraper struct {
	client                  common.Client
	mb                      *metadata.MetricsBuilder
	logger                  *zap.Logger
	enableAdditionalMetrics bool
}

// NewReplicationScraper creates a new replication metrics scraper.
func NewReplicationScraper(c common.Client, mb *metadata.MetricsBuilder, logger *zap.Logger, enableAdditionalMetrics bool) (*ReplicationScraper, error) {
	if c == nil {
		return nil, errors.New("client cannot be nil")
	}
	if mb == nil {
		return nil, errors.New("metrics builder cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	return &ReplicationScraper{
		client:                  c,
		mb:                      mb,
		logger:                  logger,
		enableAdditionalMetrics: enableAdditionalMetrics,
	}, nil
}

// ScrapeMetrics collects MySQL replication metrics from SHOW SLAVE/REPLICA STATUS.
func (s *ReplicationScraper) ScrapeMetrics(_ context.Context, now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	s.logger.Debug("Scraping MySQL replication metrics")

	// Always scrape core replica/slave metrics (backward compatible)
	s.scrapeReplicaMetrics(now, errs)

	// Only scrape additional metrics if flag is enabled
	if s.enableAdditionalMetrics {
		s.logger.Debug("Additional replication metrics collection enabled")
		// Scrape master/source metrics
		s.scrapeMasterMetrics(now, errs)

		// Scrape group replication metrics
		s.scrapeGroupReplicationMetrics(now, errs)
	}
}

// scrapeReplicaMetrics collects replication metrics when this node is a replica.
func (s *ReplicationScraper) scrapeReplicaMetrics(now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	replicationStatus, err := s.client.GetReplicationStatus()
	if err != nil {
		s.logger.Error("Failed to fetch replication status", zap.Error(err))
		errs.AddPartial(1, err)
		return
	}

	// If replication status is empty, this is not a replica
	if len(replicationStatus) == 0 {
		s.logger.Debug("Node is not a replica (no replication status)")
		return
	}

	// This is a replica - log status info
	s.logger.Debug("Node is a replica")

	// Parse and record replication metrics
	for key, value := range replicationStatus {
		switch key {
		case "Seconds_Behind_Master":
			// MySQL 5.7 and earlier - only send seconds_behind_master (requires replication flag)
			if s.enableAdditionalMetrics && value != "" && value != "NULL" {
				if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
					s.mb.RecordNewrelicmysqlReplicationSecondsBehindMasterDataPoint(now, intVal)
				}
			}
		case "Seconds_Behind_Source":
			// MySQL 8.0.22+ - only send seconds_behind_source (requires replication flag)
			if s.enableAdditionalMetrics && value != "" && value != "NULL" {
				if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
					s.mb.RecordNewrelicmysqlReplicationSecondsBehindSourceDataPoint(now, intVal)
				}
			}
		case "Read_Master_Log_Pos", "Read_Source_Log_Pos":
			if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordNewrelicmysqlReplicationReadMasterLogPosDataPoint(now, intVal)
			}
		case "Exec_Master_Log_Pos", "Exec_Source_Log_Pos":
			if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordNewrelicmysqlReplicationExecMasterLogPosDataPoint(now, intVal)
			}
		case "Last_IO_Errno":
			if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordNewrelicmysqlReplicationLastIoErrnoDataPoint(now, intVal)
			}
		case "Last_SQL_Errno", "Last_Errno":
			if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordNewrelicmysqlReplicationLastSQLErrnoDataPoint(now, intVal)
			}
		case "Relay_Log_Space":
			if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordNewrelicmysqlReplicationRelayLogSpaceDataPoint(now, intVal)
			}
		}
	}

	// Get IO and SQL thread status (compatible with MySQL 5.7 and 8.0+)
	ioRunning := replicationStatus["Slave_IO_Running"]
	if ioRunning == "" {
		ioRunning = replicationStatus["Replica_IO_Running"]
	}
	sqlRunning := replicationStatus["Slave_SQL_Running"]
	if sqlRunning == "" {
		sqlRunning = replicationStatus["Replica_SQL_Running"]
	}

	// Convert IO thread status to numeric: 0=No, 1=Yes, 2=Connecting
	ioStatus := common.ConvertReplicationThreadStatus(ioRunning)
	s.mb.RecordNewrelicmysqlReplicationSlaveIoRunningDataPoint(now, ioStatus)

	// Convert SQL thread status to numeric: 0=No, 1=Yes
	sqlStatus := common.ConvertReplicationThreadStatus(sqlRunning)
	s.mb.RecordNewrelicmysqlReplicationSlaveSQLRunningDataPoint(now, sqlStatus)

	// Calculate composite slave_running (1 if both IO and SQL threads are running, 0 otherwise)
	slaveRunning := int64(0)
	if ioRunning == "Yes" && sqlRunning == "Yes" {
		slaveRunning = 1
	}
	s.mb.RecordNewrelicmysqlReplicationSlaveRunningDataPoint(now, slaveRunning)
}

// scrapeMasterMetrics collects replication metrics when this node is a master/source.
func (s *ReplicationScraper) scrapeMasterMetrics(now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	s.logger.Debug("Attempting to scrape master replication metrics")

	masterStatus, err := s.client.GetMasterStatus()
	if err != nil {
		s.logger.Error("Failed to fetch master status", zap.Error(err))
		errs.AddPartial(1, err)
		return
	}

	s.logger.Debug("Master status retrieved", zap.Any("masterStatus", masterStatus))

	// Record number of connected slaves/replicas
	if slavesConnected, ok := masterStatus["Slaves_Connected"]; ok && slavesConnected != "" {
		if intVal, err := strconv.ParseInt(slavesConnected, 10, 64); err == nil {
			s.mb.RecordNewrelicmysqlReplicationSlavesConnectedDataPoint(now, intVal)
		}
	}

	if replicasConnected, ok := masterStatus["Replicas_Connected"]; ok && replicasConnected != "" {
		if intVal, err := strconv.ParseInt(replicasConnected, 10, 64); err == nil {
			s.mb.RecordNewrelicmysqlReplicationReplicasConnectedDataPoint(now, intVal)
		}
	}
}

// scrapeGroupReplicationMetrics collects MySQL Group Replication metrics.
func (s *ReplicationScraper) scrapeGroupReplicationMetrics(now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	groupStats, err := s.client.GetGroupReplicationStats()
	if err != nil {
		s.logger.Error("Failed to fetch group replication stats", zap.Error(err))
		errs.AddPartial(1, err)
		return
	}

	// If no group replication stats, return early
	if len(groupStats) == 0 {
		s.logger.Debug("Group replication not enabled or no stats available")
		return
	}

	s.logger.Debug("Collecting group replication metrics")

	// Parse and record group replication metrics
	for key, value := range groupStats {
		if value == "" {
			continue
		}

		intVal, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			s.logger.Debug("Failed to parse group replication metric", zap.String("key", key), zap.String("value", value), zap.Error(err))
			continue
		}

		switch key {
		case "group_replication_transactions":
			s.mb.RecordNewrelicmysqlReplicationGroupTransactionsDataPoint(now, intVal)
		case "group_replication_transactions_check":
			s.mb.RecordNewrelicmysqlReplicationGroupTransactionsCheckDataPoint(now, intVal)
		case "group_replication_conflicts_detected":
			s.mb.RecordNewrelicmysqlReplicationGroupConflictsDetectedDataPoint(now, intVal)
		case "group_replication_transactions_validating":
			s.mb.RecordNewrelicmysqlReplicationGroupTransactionsValidatingDataPoint(now, intVal)
		case "group_replication_transactions_in_applier_queue":
			s.mb.RecordNewrelicmysqlReplicationGroupTransactionsInApplierQueueDataPoint(now, intVal)
		case "group_replication_transactions_applied":
			s.mb.RecordNewrelicmysqlReplicationGroupTransactionsAppliedDataPoint(now, intVal)
		case "group_replication_transactions_proposed":
			s.mb.RecordNewrelicmysqlReplicationGroupTransactionsProposedDataPoint(now, intVal)
		case "group_replication_transactions_rollback":
			s.mb.RecordNewrelicmysqlReplicationGroupTransactionsRollbackDataPoint(now, intVal)
		}
	}
}
