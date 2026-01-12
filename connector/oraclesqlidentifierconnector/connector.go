// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package oraclesqlidentifierconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/oraclesqlidentifierconnector"

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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
	defaultSQLIDAttr         = "query_id"         // New Relic Oracle receiver uses query_id
	defaultChildAddressAttr  = "child_address"    // Not provided by New Relic receiver
	defaultChildNumberAttr   = "sql_child_number" // New Relic Oracle receiver uses sql_child_number

	// Log event names
	sqlIdentifierEventName = "oracle.sql_identifier_extracted"
)

// Capabilities returns the capabilities of the connector
func (c *oracleConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeMetrics processes metrics and extracts SQL identifiers
func (c *oracleConnector) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	c.logger.Info("ConsumeMetrics called",
		zap.Int("resourceMetrics", md.ResourceMetrics().Len()))

	extractedIdentifiers := c.extractSQLIdentifiersFromMetrics(md)

	c.logger.Info("SQL identifier extraction completed",
		zap.Int("count", len(extractedIdentifiers)))

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
	c.logger.Info("Processing resource metrics", zap.Int("count", resourceMetrics.Len()))

	for i := 0; i < resourceMetrics.Len(); i++ {
		rm := resourceMetrics.At(i)
		scopeMetrics := rm.ScopeMetrics()
		c.logger.Info("Processing scope metrics", zap.Int("count", scopeMetrics.Len()))

		for j := 0; j < scopeMetrics.Len(); j++ {
			sm := scopeMetrics.At(j)
			metrics := sm.Metrics()
			c.logger.Info("Processing metrics", zap.Int("count", metrics.Len()))

			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				metricName := metric.Name()
				c.logger.Debug("Processing metric",
					zap.String("name", metricName),
					zap.String("type", metric.Type().String()))

				// Look for Oracle wait events or any Oracle metrics that might contain SQL identifiers
				if c.isOracleWaitEventMetric(metric) {
					c.logger.Info("Found Oracle metric with potential SQL identifiers", zap.String("name", metricName))
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

	c.logger.Debug("Checking metric for Oracle wait event patterns", zap.String("name", name))

	// Look for New Relic Oracle receiver metric names that contain SQL identifiers
	isOracleMetric := name == "newrelicoracledb.wait_events.current_wait_time_ms" ||
		name == "newrelicoracledb.blocking_queries.wait_time_ms" ||
		name == "newrelicoracledb.child_cursors.cpu_time" ||
		name == "newrelicoracledb.child_cursors.elapsed_time" ||
		name == "newrelicoracledb.child_cursors.executions" ||
		name == "newrelicoracledb.child_cursors.buffer_gets" ||
		name == "newrelicoracledb.child_cursors.disk_reads" ||
		name == "newrelicoracledb.child_cursors.details" ||
		// Pattern matching for any New Relic Oracle metric
		strings.Contains(strings.ToLower(name), "newrelicoracledb") ||
		strings.Contains(strings.ToLower(name), "oracle") ||
		strings.Contains(strings.ToLower(name), "wait_event") ||
		strings.Contains(strings.ToLower(name), "child_cursor")

	if isOracleMetric {
		c.logger.Info("Metric matches Oracle pattern", zap.String("name", name))
	}

	return isOracleMetric
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

	// Log all available attributes for debugging
	attributesList := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		attributesList = append(attributesList, fmt.Sprintf("%s=%s", k, v.AsString()))
		return true
	})
	c.logger.Debug("Available attributes in data point",
		zap.Strings("attributes", attributesList))

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

	c.logger.Debug("Looking for SQL identifier attributes",
		zap.String("sql_id_attr", sqlIDAttr),
		zap.String("child_addr_attr", childAddrAttr),
		zap.String("child_num_attr", childNumAttr))

	// Extract SQL ID
	if val, exists := attrs.Get(sqlIDAttr); exists {
		sqlId.SQLID = val.Str()
		found = true
		c.logger.Debug("Found SQL ID", zap.String("value", sqlId.SQLID))
	} else {
		// Try alternative names for SQL ID
		alternativeNames := []string{"SQL_ID", "sql_id", "sqlid", "SQLID", "query_id", "QUERY_ID"}
		for _, altName := range alternativeNames {
			if val, exists := attrs.Get(altName); exists {
				sqlId.SQLID = val.Str()
				found = true
				c.logger.Debug("Found SQL ID with alternative name",
					zap.String("attr_name", altName),
					zap.String("value", sqlId.SQLID))
				break
			}
		}
	}

	// Extract child address
	if val, exists := attrs.Get(childAddrAttr); exists {
		sqlId.ChildAddress = val.Str()
		found = true
		c.logger.Debug("Found child address", zap.String("value", sqlId.ChildAddress))
	} else {
		// Try alternative names for child address
		alternativeNames := []string{"CHILD_ADDRESS", "child_address", "address", "ADDRESS", "CURSOR_ADDRESS", "cursor_address"}
		for _, altName := range alternativeNames {
			if val, exists := attrs.Get(altName); exists {
				sqlId.ChildAddress = val.Str()
				found = true
				c.logger.Debug("Found child address with alternative name",
					zap.String("attr_name", altName),
					zap.String("value", sqlId.ChildAddress))
				break
			}
		}
		if sqlId.ChildAddress == "" {
			c.logger.Debug("Child address not found, using empty string")
			sqlId.ChildAddress = "" // Allow empty child address
		}
	}

	// Extract child number
	if val, exists := attrs.Get(childNumAttr); exists {
		sqlId.ChildNumber = val.Str()
		found = true
		c.logger.Debug("Found child number", zap.String("value", sqlId.ChildNumber))
	} else {
		// Try alternative names for child number
		alternativeNames := []string{"SQL_CHILD_NUMBER", "sql_child_number", "child_number", "CHILD_NUMBER", "CURSOR_CHILD_NUMBER", "cursor_child_number"}
		for _, altName := range alternativeNames {
			if val, exists := attrs.Get(altName); exists {
				sqlId.ChildNumber = val.Str()
				found = true
				c.logger.Debug("Found child number with alternative name",
					zap.String("attr_name", altName),
					zap.String("value", sqlId.ChildNumber))
				break
			}
		}
		if sqlId.ChildNumber == "" {
			c.logger.Debug("Child number not found, using empty string")
			sqlId.ChildNumber = "" // Allow empty child number
		}
	}

	// We only require SQL_ID to be present, child_address and child_number are optional
	if found && sqlId.SQLID != "" {
		c.logger.Info("Successfully extracted SQL identifier",
			zap.String("sql_id", sqlId.SQLID),
			zap.String("child_address", sqlId.ChildAddress),
			zap.String("child_number", sqlId.ChildNumber))
		return &sqlId
	}

	c.logger.Debug("No SQL identifier found in data point")
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
