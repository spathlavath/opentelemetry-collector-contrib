// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package oraclesqlidentifierconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/oraclesqlidentifierconnector"

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// SQLIdentifier represents Oracle SQL identifier information
type SQLIdentifier struct {
	SQLID        string `json:"sql_id"`
	ChildAddress string `json:"child_address"`
	ChildNumber  string `json:"child_number"`
}

// oracleConnector extracts SQL identifiers from Oracle metrics and forwards them as logs
type oracleConnector struct {
	logsConsumer consumer.Logs
	config       *Config
	logger       *zap.Logger

	// Storage for SQL identifiers with TTL
	mu             sync.RWMutex
	sqlIdentifiers map[string]SQLIdentifier
	lastUpdated    time.Time
	ttl            time.Duration
}

const (
	defaultTTL = 5 * time.Minute

	// Default attribute names for Oracle SQL identifiers
	defaultSQLIdentifierAttr = "oracle.sql_identifier"
	defaultSQLIDAttr         = "SQL_ID"
	defaultChildAddressAttr  = "CHILD_ADDRESS"
	defaultChildNumberAttr   = "SQL_CHILD_NUMBER"

	// Log event names
	sqlIdentifierEventName = "oracle.sql_identifier_extracted"
)

// Capabilities returns the capabilities of the connector
func (c *oracleConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeMetrics processes metrics and extracts SQL identifiers
func (c *oracleConnector) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	extractedIdentifiers := c.extractSQLIdentifiersFromMetrics(md)

	if len(extractedIdentifiers) > 0 {
		c.logger.Info("Extracted SQL identifiers from Oracle metrics",
			zap.Int("count", len(extractedIdentifiers)))

		// Store identifiers
		c.storeSQLIdentifiers(extractedIdentifiers)

		// Forward as logs
		return c.forwardSQLIdentifiersAsLogs(ctx, extractedIdentifiers)
	}

	return nil
}

// Start initializes the connector
func (c *oracleConnector) Start(_ context.Context, _ component.Host) error {
	c.ttl = defaultTTL
	c.sqlIdentifiers = make(map[string]SQLIdentifier)

	c.logger.Info("Oracle SQL Identifier connector started",
		zap.Duration("ttl", c.ttl))

	return nil
}

// Shutdown stops the connector
func (c *oracleConnector) Shutdown(_ context.Context) error {
	c.logger.Info("Oracle SQL Identifier connector shutdown")
	return nil
}

// extractSQLIdentifiersFromMetrics extracts SQL identifiers from Oracle metrics
func (c *oracleConnector) extractSQLIdentifiersFromMetrics(md pmetric.Metrics) []SQLIdentifier {
	var identifiers []SQLIdentifier

	resourceMetrics := md.ResourceMetrics()
	for i := 0; i < resourceMetrics.Len(); i++ {
		rm := resourceMetrics.At(i)
		scopeMetrics := rm.ScopeMetrics()

		for j := 0; j < scopeMetrics.Len(); j++ {
			sm := scopeMetrics.At(j)
			metrics := sm.Metrics()

			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)

				// Look for Oracle wait events or slow query metrics
				if c.isOracleWaitEventMetric(metric) {
					extractedIds := c.extractFromWaitEventMetric(metric)
					identifiers = append(identifiers, extractedIds...)
				}
			}
		}
	}

	return c.deduplicateIdentifiers(identifiers)
}

// isOracleWaitEventMetric checks if a metric contains Oracle SQL identifier information
func (c *oracleConnector) isOracleWaitEventMetric(metric pmetric.Metric) bool {
	name := metric.Name()

	// Look for Oracle-specific metric names that would contain SQL identifiers
	return name == "newrelicoracledb.wait_events.current_wait_time_ms" ||
		name == "newrelicoracledb.wait_events.blocking_count" ||
		name == "newrelicoracledb.slow_query.count" ||
		name == "newrelicoracledb.child_cursor.count"
}

// extractFromWaitEventMetric extracts SQL identifiers from wait event metrics
func (c *oracleConnector) extractFromWaitEventMetric(metric pmetric.Metric) []SQLIdentifier {
	var identifiers []SQLIdentifier

	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dataPoints := metric.Gauge().DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			if sqlId := c.extractSQLIdentifierFromDataPoint(dp.Attributes()); sqlId != nil {
				identifiers = append(identifiers, *sqlId)
			}
		}
	case pmetric.MetricTypeSum:
		dataPoints := metric.Sum().DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			if sqlId := c.extractSQLIdentifierFromDataPoint(dp.Attributes()); sqlId != nil {
				identifiers = append(identifiers, *sqlId)
			}
		}
	}

	return identifiers
}

// extractSQLIdentifierFromDataPoint extracts SQL identifier from data point attributes
func (c *oracleConnector) extractSQLIdentifierFromDataPoint(attrs pcommon.Map) *SQLIdentifier {
	var sqlId SQLIdentifier
	found := false

	// Use configured attribute names or defaults
	sqlIDAttr := c.config.SQLIDAttribute
	if sqlIDAttr == "" {
		sqlIDAttr = defaultSQLIDAttr
	}

	childAddrAttr := c.config.ChildAddressAttribute
	if childAddrAttr == "" {
		childAddrAttr = defaultChildAddressAttr
	}

	childNumAttr := c.config.ChildNumberAttribute
	if childNumAttr == "" {
		childNumAttr = defaultChildNumberAttr
	}

	// Extract SQL ID
	if val, exists := attrs.Get(sqlIDAttr); exists {
		sqlId.SQLID = val.Str()
		found = true
	}

	// Extract child address
	if val, exists := attrs.Get(childAddrAttr); exists {
		sqlId.ChildAddress = val.Str()
		found = true
	}

	// Extract child number
	if val, exists := attrs.Get(childNumAttr); exists {
		sqlId.ChildNumber = val.Str()
		found = true
	}

	if found && sqlId.SQLID != "" {
		return &sqlId
	}

	return nil
}

// deduplicateIdentifiers removes duplicate SQL identifiers
func (c *oracleConnector) deduplicateIdentifiers(identifiers []SQLIdentifier) []SQLIdentifier {
	seen := make(map[string]bool)
	var result []SQLIdentifier

	for _, id := range identifiers {
		key := fmt.Sprintf("%s:%s:%s", id.SQLID, id.ChildAddress, id.ChildNumber)
		if !seen[key] {
			seen[key] = true
			result = append(result, id)
		}
	}

	return result
}

// storeSQLIdentifiers stores SQL identifiers with TTL
func (c *oracleConnector) storeSQLIdentifiers(identifiers []SQLIdentifier) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, id := range identifiers {
		key := fmt.Sprintf("%s:%s:%s", id.SQLID, id.ChildAddress, id.ChildNumber)
		c.sqlIdentifiers[key] = id
	}
	c.lastUpdated = time.Now()

	c.logger.Debug("Stored SQL identifiers",
		zap.Int("count", len(identifiers)),
		zap.Time("timestamp", c.lastUpdated))
}

// forwardSQLIdentifiersAsLogs forwards extracted SQL identifiers as log records
func (c *oracleConnector) forwardSQLIdentifiersAsLogs(ctx context.Context, identifiers []SQLIdentifier) error {
	if len(identifiers) == 0 {
		return nil
	}

	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()

	// Set scope info
	scope := scopeLogs.Scope()
	scope.SetName("oracle-sql-identifier-connector")
	scope.SetVersion("1.0.0")

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for _, sqlId := range identifiers {
		logRecord := scopeLogs.LogRecords().AppendEmpty()
		logRecord.SetTimestamp(timestamp)
		logRecord.SetEventName(sqlIdentifierEventName)
		logRecord.SetSeverityText("INFO")

		// Add SQL identifier information as attributes
		attrs := logRecord.Attributes()
		attrs.PutStr("sql_id", sqlId.SQLID)
		attrs.PutStr("child_address", sqlId.ChildAddress)
		attrs.PutStr("child_number", sqlId.ChildNumber)

		// Add as JSON in body
		identifierJSON, _ := json.Marshal(sqlId)
		logRecord.Body().SetStr(string(identifierJSON))

		c.logger.Debug("Created log record for SQL identifier",
			zap.String("sql_id", sqlId.SQLID),
			zap.String("child_address", sqlId.ChildAddress))
	}

	return c.logsConsumer.ConsumeLogs(ctx, logs)
}

// GetStoredSQLIdentifiers returns currently stored SQL identifiers (for external access)
func (c *oracleConnector) GetStoredSQLIdentifiers() []SQLIdentifier {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check if data is still valid
	if time.Since(c.lastUpdated) > c.ttl {
		return nil
	}

	var result []SQLIdentifier
	for _, id := range c.sqlIdentifiers {
		result = append(result, id)
	}

	return result
}
