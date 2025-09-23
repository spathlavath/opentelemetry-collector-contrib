// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"

	"go.uber.org/zap"
	
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// PDBSysMetricsScraper handles Oracle PDB system metrics collection
// Following the same simple pattern as SessionScraper
type PDBSysMetricsScraper struct {
	db           *sql.DB
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	config       metadata.MetricsBuilderConfig
}

// NewPDBSysMetricsScraper creates a new PDB system metrics scraper
func NewPDBSysMetricsScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, config metadata.MetricsBuilderConfig) *PDBSysMetricsScraper {
	return &PDBSysMetricsScraper{
		db:           db,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		config:       config,
	}
}

// ScrapePDBSysMetrics collects Oracle PDB system metrics
// Simple approach like SessionScraper - no goroutines, no channels
func (s *PDBSysMetricsScraper) ScrapePDBSysMetrics(ctx context.Context) []error {
	var errors []error
	
	s.logger.Debug("Scraping Oracle PDB system metrics")
	
	// Execute PDB system metrics query directly using the shared DB connection
	s.logger.Debug("Executing PDB system metrics query", zap.String("sql", queries.PDBSysMetricsSQL))
	
	rows, err := s.db.QueryContext(ctx, queries.PDBSysMetricsSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing PDB system metrics query: %w", err))
		return errors
	}
	defer rows.Close()
	
	metricCount := 0
	for rows.Next() {
		var instID int64
		var metricName string
		var value float64
		
		err := rows.Scan(&instID, &metricName, &value)
		if err != nil {
			if err == sql.ErrNoRows {
				s.logger.Warn("No rows returned from PDB system metrics query")
				continue
			}
			errors = append(errors, fmt.Errorf("error scanning PDB metric row: %w", err))
			continue
		}
		
		// For now, just log the metrics - later we can add specific metric recording
		// based on the metric names we're interested in
		s.logger.Debug("Collected PDB system metric", 
			zap.String("metric_name", metricName),
			zap.Float64("value", value),
			zap.Int64("instance_id", instID),
			zap.String("instance", s.instanceName))
		
		metricCount++
	}
	
	if err := rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating PDB metric rows: %w", err))
		return errors
	}
	
	s.logger.Debug("Completed PDB system metrics collection", 
		zap.Int("metric_count", metricCount),
		zap.String("instance", s.instanceName))
	
	return errors
}