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

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

// ReplicationScraper handles MySQL replication metrics collection.
// Only collects data if the instance is configured as a replication slave.
type ReplicationScraper struct {
	client client.MySQLClient
	mb     *metadata.MetricsBuilder
	logger *zap.Logger
}

// NewReplicationScraper creates a new replication metrics scraper.
func NewReplicationScraper(c client.MySQLClient, mb *metadata.MetricsBuilder, logger *zap.Logger) (*ReplicationScraper, error) {
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
		client: c,
		mb:     mb,
		logger: logger,
	}, nil
}

// ScrapeMetrics collects MySQL replication metrics from SHOW SLAVE/REPLICA STATUS.
func (s *ReplicationScraper) ScrapeMetrics(_ context.Context, now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	s.logger.Debug("Scraping MySQL replication metrics")

	replicationStatus, err := s.client.GetReplicationStatus()
	if err != nil {
		s.logger.Error("Failed to fetch replication status", zap.Error(err))
		errs.AddPartial(1, err)
		return
	}

	// If replication status is empty, this is not a replica
	if len(replicationStatus) == 0 {
		s.logger.Info("Node is a master (no replication status)")
		return
	}

	// This is a replica - log status info
	s.logger.Info("Node is a replica")

	// Parse and record replication metrics
	for key, value := range replicationStatus {
		switch key {
		case "Seconds_Behind_Master", "Seconds_Behind_Source":
			if value != "" && value != "NULL" {
				if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
					s.mb.RecordNewrelicmysqlReplicationSecondsBehindMasterDataPoint(now, intVal)
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
	ioStatus := ConvertReplicationThreadStatus(ioRunning)
	s.mb.RecordNewrelicmysqlReplicationSlaveIoRunningDataPoint(now, ioStatus)

	// Convert SQL thread status to numeric: 0=No, 1=Yes
	sqlStatus := ConvertReplicationThreadStatus(sqlRunning)
	s.mb.RecordNewrelicmysqlReplicationSlaveSQLRunningDataPoint(now, sqlStatus)

	// Calculate composite slave_running (1 if both IO and SQL threads are running, 0 otherwise)
	slaveRunning := int64(0)
	if ioRunning == "Yes" && sqlRunning == "Yes" {
		slaveRunning = 1
	}
	s.mb.RecordNewrelicmysqlReplicationSlaveRunningDataPoint(now, slaveRunning)
}
