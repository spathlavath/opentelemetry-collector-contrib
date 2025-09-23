// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// SGAScraper is a scraper for SGA metrics
type SGAScraper struct {
	db           *sql.DB
	logger       *zap.Logger
	mb           *metadata.MetricsBuilder
	instanceName string
	config       Config
}

// NewSGAScraper creates a new SGAScraper
func NewSGAScraper(db *sql.DB, logger *zap.Logger, mb *metadata.MetricsBuilder, instanceName string, config Config) *SGAScraper {
	return &SGAScraper{
		db:           db,
		logger:       logger,
		mb:           mb,
		instanceName: instanceName,
		config:       config,
	}
}

// ScrapeSGA executes SGA metrics collection
// Follows the exact same pattern as nri-oracledb oracleSGA metricsGenerator
func (s *SGAScraper) ScrapeSGA(ctx context.Context) []error {
	var errors []error

	s.logger.Debug("Collecting SGA metrics")

	rows, err := s.db.QueryContext(ctx, queries.SGAMetricsSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("failed to query SGA metrics: %w", err))
		return errors
	}
	defer rows.Close()

	for rows.Next() {
		if err := s.processRow(rows); err != nil {
			errors = append(errors, fmt.Errorf("error processing SGA row: %w", err))
		}
	}

	// Check for any errors that occurred during row iteration
	if err := rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating over SGA rows: %w", err))
	}

	return errors
}

// processRow processes a single SGA row following nri-oracledb rowMetricsGenerator pattern
func (s *SGAScraper) processRow(rows *sql.Rows) error {
	var instID, name, valueStr string

	// Scan: inst_id, name, value (matching SGAMetricsSQL query structure)
	if err := rows.Scan(&instID, &name, &valueStr); err != nil {
		return fmt.Errorf("failed to scan SGA row: %w", err)
	}

	// Convert value to int64
	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse SGA value '%s': %w", valueStr, err)
	}

	// Record metrics based on SGA component name (following nri-oracledb pattern)
	switch name {
	case "Fixed Size":
		// Will be uncommented after mdatagen regenerates the functions
		// now := pcommon.NewTimestampFromTime(time.Now())
		// if s.config.GetMetrics().NewrelicoracledbSgaFixedSizeBytes.Enabled {
		//     s.mb.RecordNewrelicoracledbSgaFixedSizeBytesDataPoint(now, value, s.instanceName, instID)
		// }
		s.logger.Debug("SGA Fixed Size metric found but recording disabled until mdatagen regeneration")
	case "Redo Buffers":
		// Will be uncommented after mdatagen regenerates the functions
		// now := pcommon.NewTimestampFromTime(time.Now())
		// if s.config.GetMetrics().NewrelicoracledbSgaRedoBuffersBytes.Enabled {
		//     s.mb.RecordNewrelicoracledbSgaRedoBuffersBytesDataPoint(now, value, s.instanceName, instID)
		// }
		s.logger.Debug("SGA Redo Buffers metric found but recording disabled until mdatagen regeneration")
	default:
		s.logger.Debug("Unknown SGA metric", zap.String("name", name))
	}

	s.logger.Debug("Processed SGA metric",
		zap.String("name", name),
		zap.String("instance_id", instID),
		zap.Int64("value", value),
	)

	return nil
}