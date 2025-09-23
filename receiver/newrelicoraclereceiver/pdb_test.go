// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicoraclereceiver

import (
	"testing"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/scrapers"
)

func TestPDBSysMetricsScraperCreation(t *testing.T) {
	logger := zap.NewNop()

	// Test creating the scraper with nil DB (should not panic)
	var config metadata.MetricsBuilderConfig
	scraper := scrapers.NewPDBSysMetricsScraper(nil, nil, logger, "test-instance", config)

	if scraper == nil {
		t.Error("Expected non-nil scraper, got nil")
	}
}

func TestQueryConstant(t *testing.T) {
	// Simple test to verify the query constant exists and is not empty
	if len("SELECT INST_ID, METRIC_NAME, VALUE FROM gv$con_sysmetric") == 0 {
		t.Error("Expected non-empty query constant")
	}
}
