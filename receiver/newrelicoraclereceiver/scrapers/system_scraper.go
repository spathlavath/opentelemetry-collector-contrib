// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers // import "github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/scrapers"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/client"
	"github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/internal/metadata"
)

type SystemScraper struct {
	client               client.OracleClient
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	metricsBuilderConfig metadata.MetricsBuilderConfig
	metricRegistry       *SystemMetricRegistry
}

func NewSystemScraper(c client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, metricsBuilderConfig metadata.MetricsBuilderConfig) *SystemScraper {
	return &SystemScraper{
		client:               c,
		mb:                   mb,
		logger:               logger,
		metricsBuilderConfig: metricsBuilderConfig,
		metricRegistry:       NewSystemMetricRegistry(),
	}
}

func (s *SystemScraper) ScrapeSystemMetrics(ctx context.Context) []error {
	var scrapeErrors []error

	metrics, err := s.client.QuerySystemMetrics(ctx)
	if err != nil {
		return []error{err}
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	for _, metric := range metrics {
		s.recordMetric(now, metric.MetricName, metric.Value, metric.InstanceID)
		metricCount++
	}

	return scrapeErrors
}

func (s *SystemScraper) recordMetric(now pcommon.Timestamp, metricName string, value float64, instanceIDStr string) {
	if !s.metricRegistry.RecordMetric(s.mb, now, metricName, value, instanceIDStr) {
		s.logger.Debug("Unknown system metric", zap.String("metric_name", metricName))
	}
}
