// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/scrapers"

import (
	"context"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
)

type SystemScraper struct {
	client                client.OracleClient
	mb                    *metadata.MetricsBuilder
	logger                *zap.Logger
	metricsBuilderConfig  metadata.MetricsBuilderConfig
	metricRegistry        *SystemMetricRegistry
	enableAdvancedMetrics bool
	uiCriticalMetrics     map[string]bool
}

func NewSystemScraper(c client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, metricsBuilderConfig metadata.MetricsBuilderConfig, enableAdvancedMetrics bool) *SystemScraper {
	return &SystemScraper{
		client:                c,
		mb:                    mb,
		logger:                logger,
		metricsBuilderConfig:  metricsBuilderConfig,
		metricRegistry:        NewSystemMetricRegistry(),
		enableAdvancedMetrics: enableAdvancedMetrics,
		uiCriticalMetrics: map[string]bool{
			"Buffer Cache Hit Ratio":    true,
			"Database CPU Time Ratio":   true,
			"Response Time Per Txn":     true,
			"I/O Megabytes per Second":  true,
			"Redo Generated Per Sec":    true,
			"Executions Per Sec":        true,
			"Average Active Sessions":   true,
			"Physical Reads Per Sec":    true,
			"Physical Writes Per Sec":   true,
			"Enqueue Waits Per Sec":     true,
			"Hard Parse Count Per Sec":  true,
			"User Transaction Per Sec":  true,
			"Host CPU Utilization (%)":  true,
		},
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
	isUICritical := s.uiCriticalMetrics[metricName]

	if isUICritical || s.enableAdvancedMetrics {
		if !s.metricRegistry.RecordMetric(s.mb, now, metricName, value, instanceIDStr) {
			s.logger.Debug("Unknown system metric", zap.String("metric_name", metricName))
		}
	}
}
