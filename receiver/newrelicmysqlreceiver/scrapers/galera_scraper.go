// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/common"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

type GaleraScraper struct {
	client common.Client
	mb     *metadata.MetricsBuilder
	logger *zap.Logger
}

func NewGaleraScraper(client common.Client, mb *metadata.MetricsBuilder, logger *zap.Logger) (*GaleraScraper, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}
	if mb == nil {
		return nil, errors.New("metrics builder cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	return &GaleraScraper{
		client: client,
		mb:     mb,
		logger: logger,
	}, nil
}

func (s *GaleraScraper) ScrapeMetrics(_ context.Context, now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	s.logger.Debug("Scraping MySQL Galera cluster metrics")

	globalStats, err := s.client.GetGlobalStats()
	if err != nil {
		s.logger.Error("Failed to fetch global stats for Galera metrics", zap.Error(err))
		errs.AddPartial(1, err)
		return
	}

	for key, val := range globalStats {
		switch key {
		case "wsrep_cert_deps_distance":
			s.mb.RecordNewrelicmysqlGaleraWsrepCertDepsDistanceDataPoint(now, parseFloat64(val, s.logger, key))
		case "wsrep_cluster_size":
			s.mb.RecordNewrelicmysqlGaleraWsrepClusterSizeDataPoint(now, parseInt64(val, s.logger, key))
		case "wsrep_flow_control_paused":
			s.mb.RecordNewrelicmysqlGaleraWsrepFlowControlPausedDataPoint(now, parseFloat64(val, s.logger, key))
		case "wsrep_flow_control_paused_ns":
			s.mb.RecordNewrelicmysqlGaleraWsrepFlowControlPausedNsDataPoint(now, parseInt64(val, s.logger, key))
		case "wsrep_flow_control_recv":
			s.mb.RecordNewrelicmysqlGaleraWsrepFlowControlRecvDataPoint(now, parseInt64(val, s.logger, key))
		case "wsrep_flow_control_sent":
			s.mb.RecordNewrelicmysqlGaleraWsrepFlowControlSentDataPoint(now, parseInt64(val, s.logger, key))
		case "wsrep_local_cert_failures":
			s.mb.RecordNewrelicmysqlGaleraWsrepLocalCertFailuresDataPoint(now, parseInt64(val, s.logger, key))
		case "wsrep_local_recv_queue":
			s.mb.RecordNewrelicmysqlGaleraWsrepLocalRecvQueueDataPoint(now, parseInt64(val, s.logger, key))
		case "wsrep_local_recv_queue_avg":
			s.mb.RecordNewrelicmysqlGaleraWsrepLocalRecvQueueAvgDataPoint(now, parseFloat64(val, s.logger, key))
		case "wsrep_local_send_queue":
			s.mb.RecordNewrelicmysqlGaleraWsrepLocalSendQueueDataPoint(now, parseInt64(val, s.logger, key))
		case "wsrep_local_send_queue_avg":
			s.mb.RecordNewrelicmysqlGaleraWsrepLocalSendQueueAvgDataPoint(now, parseFloat64(val, s.logger, key))
		case "wsrep_local_state":
			s.mb.RecordNewrelicmysqlGaleraWsrepLocalStateDataPoint(now, parseInt64(val, s.logger, key))
		case "wsrep_received":
			s.mb.RecordNewrelicmysqlGaleraWsrepReceivedDataPoint(now, parseInt64(val, s.logger, key))
		case "wsrep_received_bytes":
			s.mb.RecordNewrelicmysqlGaleraWsrepReceivedBytesDataPoint(now, parseInt64(val, s.logger, key))
		case "wsrep_replicated_bytes":
			s.mb.RecordNewrelicmysqlGaleraWsrepReplicatedBytesDataPoint(now, parseInt64(val, s.logger, key))
		}
	}

	s.logger.Debug("Completed scraping MySQL Galera cluster metrics")
}
