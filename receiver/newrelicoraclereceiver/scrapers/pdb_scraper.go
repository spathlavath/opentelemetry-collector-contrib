// Copyright New Relic, Inc. All rights reserved.
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
	config             metadata.MetricsBuilderConfig
	isCDBCapable       *bool
	environmentChecked bool
	detectionMutex     sync.RWMutex
	metricRegistry     *PdbMetricRegistry
}

// NewPdbScraper creates a new PDB scraper
func NewPdbScraper(c client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, config metadata.MetricsBuilderConfig) *PdbScraper {
	return &PdbScraper{
		client:         c,
		mb:             mb,
		logger:         logger,
		config:         config,
		metricRegistry: NewPdbMetricRegistry(),
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
		s.recordMetric(now, metric.MetricName, metric.Value, instanceIDStr, metric.PDBName)
		metricCount++
	}

	s.logger.Debug("Collected PDB sys metrics")

	return nil
}

func (s *PdbScraper) recordMetric(now pcommon.Timestamp, metricName string, value float64, instanceID string, pdbName string) {
	if !s.metricRegistry.RecordMetric(s.mb, now, metricName, value, instanceID, pdbName) {
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
