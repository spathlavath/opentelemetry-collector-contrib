// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func TestNewWaitEventBlockingScraper_Success(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	countThreshold := 10

	scraper, err := NewWaitEventBlockingScraper(mockClient, mb, logger, config, countThreshold)

	assert.NoError(t, err)
	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, config, scraper.metricsBuilderConfig)
	assert.Equal(t, countThreshold, scraper.queryMonitoringCountThreshold)
}

func TestNewWaitEventBlockingScraper_NilClient(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewWaitEventBlockingScraper(nil, mb, logger, config, 10)

	assert.Error(t, err)
	assert.Nil(t, scraper)
	assert.Contains(t, err.Error(), "client cannot be nil")
}

func TestNewWaitEventBlockingScraper_NilMetricsBuilder(t *testing.T) {
	mockClient := &client.MockClient{}
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewWaitEventBlockingScraper(mockClient, nil, logger, config, 10)

	assert.Error(t, err)
	assert.Nil(t, scraper)
	assert.Contains(t, err.Error(), "metrics builder cannot be nil")
}

func TestNewWaitEventBlockingScraper_NilLogger(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewWaitEventBlockingScraper(mockClient, mb, nil, config, 10)

	assert.Error(t, err)
	assert.Nil(t, scraper)
	assert.Contains(t, err.Error(), "logger cannot be nil")
}

func TestFetchWaitEvents_Success(t *testing.T) {
	mockClient := &client.MockClient{
		WaitEventsWithBlocking: []models.WaitEventWithBlocking{
			{
				CollectionTimestamp: sql.NullTime{Time: time.Now(), Valid: true},
				DatabaseName:        sql.NullString{String: "ORCL", Valid: true},
				SID:                 sql.NullInt64{Int64: 123, Valid: true},
				WaitTimeMs:          sql.NullFloat64{Float64: 5500, Valid: true},
				SQLID:               sql.NullString{String: "abc123", Valid: true},
				SQLChildNumber:      sql.NullInt64{Int64: 0, Valid: true},
			},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	ctx := t.Context()
	slowQuerySQLIDs := []string{"sql1", "sql2"}

	waitEvents, err := scraper.fetchWaitEvents(ctx, slowQuerySQLIDs)

	assert.NoError(t, err)
	assert.Len(t, waitEvents, 1)
	assert.Equal(t, "ORCL", waitEvents[0].DatabaseName.String)
}

func TestFetchWaitEvents_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("query failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	ctx := t.Context()
	slowQuerySQLIDs := []string{"sql1"}

	waitEvents, err := scraper.fetchWaitEvents(ctx, slowQuerySQLIDs)

	assert.Error(t, err)
	assert.Nil(t, waitEvents)
	assert.Contains(t, err.Error(), "query failed")
}

func TestScrapeWaitEventsAndBlocking_Success(t *testing.T) {
	mockClient := &client.MockClient{
		WaitEventsWithBlocking: []models.WaitEventWithBlocking{
			{
				CollectionTimestamp: sql.NullTime{Time: time.Now(), Valid: true},
				DatabaseName:        sql.NullString{String: "ORCL", Valid: true},
				SID:                 sql.NullInt64{Int64: 123, Valid: true},
				WaitTimeMs:          sql.NullFloat64{Float64: 5500, Valid: true},
				SQLID:               sql.NullString{String: "abc123", Valid: true},
				SQLChildNumber:      sql.NullInt64{Int64: 0, Valid: true},
				Event:               sql.NullString{String: "db file sequential read", Valid: true},
			},
			{
				CollectionTimestamp: sql.NullTime{Time: time.Now(), Valid: true},
				DatabaseName:        sql.NullString{String: "ORCL", Valid: true},
				SID:                 sql.NullInt64{Int64: 456, Valid: true},
				WaitTimeMs:          sql.NullFloat64{Float64: 3200, Valid: true},
				SQLID:               sql.NullString{String: "def456", Valid: true},
				SQLChildNumber:      sql.NullInt64{Int64: 1, Valid: true},
				Event:               sql.NullString{String: "db file scattered read", Valid: true},
			},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	ctx := t.Context()
	slowQueryIdentifiers := []models.SQLIdentifier{{SQLID: "sql1", ChildNumber: 0, Timestamp: time.Now()}}

	sqlIdentifiers, errs := scraper.ScrapeWaitEventsAndBlocking(ctx, slowQueryIdentifiers)

	assert.Empty(t, errs)
	assert.Len(t, sqlIdentifiers, 2)
}

func TestScrapeWaitEventsAndBlocking_FetchError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("connection failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	ctx := t.Context()
	slowQueryIdentifiers := []models.SQLIdentifier{{SQLID: "sql1", ChildNumber: 0, Timestamp: time.Now()}}

	sqlIdentifiers, errs := scraper.ScrapeWaitEventsAndBlocking(ctx, slowQueryIdentifiers)

	assert.Nil(t, sqlIdentifiers)
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "connection failed")
}

func TestScrapeWaitEventsAndBlocking_EmptyResults(t *testing.T) {
	mockClient := &client.MockClient{
		WaitEventsWithBlocking: []models.WaitEventWithBlocking{},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	ctx := t.Context()
	slowQueryIdentifiers := []models.SQLIdentifier{}

	sqlIdentifiers, errs := scraper.ScrapeWaitEventsAndBlocking(ctx, slowQueryIdentifiers)

	assert.Empty(t, errs)
	assert.Empty(t, sqlIdentifiers)
}

func TestRecordWaitEventMetrics_ValidEvent(_ *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	now := time.Now()
	event := &models.WaitEventWithBlocking{
		CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
		DatabaseName:        sql.NullString{String: "ORCL", Valid: true},
		Username:            sql.NullString{String: "testuser", Valid: true},
		SID:                 sql.NullInt64{Int64: 123, Valid: true},
		Serial:              sql.NullInt64{Int64: 456, Valid: true},
		Status:              sql.NullString{String: "ACTIVE", Valid: true},
		State:               sql.NullString{String: "WAITING", Valid: true},
		SQLID:               sql.NullString{String: "abc123", Valid: true},
		SQLChildNumber:      sql.NullInt64{Int64: 0, Valid: true},
		WaitTimeMs:          sql.NullFloat64{Float64: 5500, Valid: true},
		Event:               sql.NullString{String: "db file sequential read", Valid: true},
		WaitClass:           sql.NullString{String: "User I/O", Valid: true},
	}

	// This should not panic
	scraper.recordWaitEventMetrics(pcommon.NewTimestampFromTime(now), event, make(map[string]models.SQLIdentifier))
}

func TestRecordWaitEventMetrics_InvalidEvent(_ *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	now := time.Now()
	event := &models.WaitEventWithBlocking{
		// Missing WaitTimeMs - invalid
		CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
		DatabaseName:        sql.NullString{String: "ORCL", Valid: true},
		SID:                 sql.NullInt64{Int64: 123, Valid: true},
	}

	// This should return early and not panic
	scraper.recordWaitEventMetrics(pcommon.NewTimestampFromTime(now), event, make(map[string]models.SQLIdentifier))
}

func TestEmitWaitEventMetrics_ValidEvents(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	now := time.Now()
	waitEvents := []models.WaitEventWithBlocking{
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "ORCL", Valid: true},
			SID:                 sql.NullInt64{Int64: 123, Valid: true},
			WaitTimeMs:          sql.NullFloat64{Float64: 5500, Valid: true},
			SQLID:               sql.NullString{String: "abc123", Valid: true},
			SQLChildNumber:      sql.NullInt64{Int64: 0, Valid: true},
			Event:               sql.NullString{String: "db file sequential read", Valid: true},
		},
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "ORCL", Valid: true},
			SID:                 sql.NullInt64{Int64: 456, Valid: true},
			WaitTimeMs:          sql.NullFloat64{Float64: 3200, Valid: true},
			SQLID:               sql.NullString{String: "def456", Valid: true},
			SQLChildNumber:      sql.NullInt64{Int64: 1, Valid: true},
			Event:               sql.NullString{String: "db file scattered read", Valid: true},
		},
	}

	waitEventCount, blockingCount := scraper.emitWaitEventMetrics(pcommon.NewTimestampFromTime(now), waitEvents, make(map[string]models.SQLIdentifier))

	assert.Equal(t, 2, waitEventCount)
	assert.Equal(t, 0, blockingCount) // No blocking sessions in this test
}

func TestEmitWaitEventMetrics_WithBlockedEvents(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	now := time.Now()
	waitEvents := []models.WaitEventWithBlocking{
		{
			CollectionTimestamp:        sql.NullTime{Time: now, Valid: true},
			DatabaseName:               sql.NullString{String: "ORCL", Valid: true},
			SID:                        sql.NullInt64{Int64: 123, Valid: true},
			WaitTimeMs:                 sql.NullFloat64{Float64: 5500, Valid: true},
			SQLID:                      sql.NullString{String: "abc123", Valid: true},
			SQLChildNumber:             sql.NullInt64{Int64: 0, Valid: true},
			Event:                      sql.NullString{String: "enq: TX - row lock contention", Valid: true},
			BlockingSessionStatus:      sql.NullString{String: "ACTIVE", Valid: true},
			ImmediateBlockerSID:        sql.NullInt64{Int64: 456, Valid: true},
			FinalBlockingSessionStatus: sql.NullString{String: "ACTIVE", Valid: true},
			FinalBlockerSID:            sql.NullInt64{Int64: 456, Valid: true},
		},
	}

	waitEventCount, blockingCount := scraper.emitWaitEventMetrics(pcommon.NewTimestampFromTime(now), waitEvents, make(map[string]models.SQLIdentifier))

	assert.Equal(t, 1, waitEventCount)
	assert.Equal(t, 1, blockingCount)
}

func TestEmitWaitEventMetrics_EmptyList(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	now := time.Now()
	waitEvents := []models.WaitEventWithBlocking{}

	waitEventCount, blockingCount := scraper.emitWaitEventMetrics(pcommon.NewTimestampFromTime(now), waitEvents, make(map[string]models.SQLIdentifier))

	assert.Equal(t, 0, waitEventCount)
	assert.Equal(t, 0, blockingCount)
}

func TestShouldIncludeIdentifier_ValidIdentifier(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	event := &models.WaitEventWithBlocking{
		SQLID:          sql.NullString{String: "abc123", Valid: true},
		SQLChildNumber: sql.NullInt64{Int64: 0, Valid: true},
	}

	result := scraper.shouldIncludeIdentifier(event)

	assert.True(t, result)
}

func TestShouldIncludeIdentifier_MissingQueryID(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	event := &models.WaitEventWithBlocking{
		SQLID:          sql.NullString{Valid: false},
		SQLChildNumber: sql.NullInt64{Int64: 0, Valid: true},
	}

	result := scraper.shouldIncludeIdentifier(event)

	assert.False(t, result)
}

func TestShouldIncludeIdentifier_MissingChildNumber(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	event := &models.WaitEventWithBlocking{
		SQLID:          sql.NullString{String: "abc123", Valid: true},
		SQLChildNumber: sql.NullInt64{Valid: false},
	}

	result := scraper.shouldIncludeIdentifier(event)

	assert.False(t, result)
}

func TestExtractSQLIdentifiers_UniqueIdentifiers(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	now := time.Now()
	waitEvents := []models.WaitEventWithBlocking{
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "ORCL", Valid: true},
			SID:                 sql.NullInt64{Int64: 123, Valid: true},
			WaitTimeMs:          sql.NullFloat64{Float64: 5500, Valid: true},
			SQLID:               sql.NullString{String: "abc123", Valid: true},
			SQLChildNumber:      sql.NullInt64{Int64: 0, Valid: true},
			Event:               sql.NullString{String: "db file sequential read", Valid: true},
		},
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "ORCL", Valid: true},
			SID:                 sql.NullInt64{Int64: 456, Valid: true},
			WaitTimeMs:          sql.NullFloat64{Float64: 3200, Valid: true},
			SQLID:               sql.NullString{String: "def456", Valid: true},
			SQLChildNumber:      sql.NullInt64{Int64: 1, Valid: true},
			Event:               sql.NullString{String: "db file scattered read", Valid: true},
		},
	}

	identifiers := scraper.extractSQLIdentifiers(waitEvents, make(map[string]models.SQLIdentifier))

	assert.Len(t, identifiers, 2)
	sqlIDs := make(map[string]bool)
	for _, id := range identifiers {
		sqlIDs[id.SQLID] = true
	}
	assert.True(t, sqlIDs["abc123"])
	assert.True(t, sqlIDs["def456"])
}

func TestExtractSQLIdentifiers_DuplicateIdentifiers(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	now := time.Now()
	waitEvents := []models.WaitEventWithBlocking{
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "ORCL", Valid: true},
			SID:                 sql.NullInt64{Int64: 123, Valid: true},
			WaitTimeMs:          sql.NullFloat64{Float64: 5500, Valid: true},
			SQLID:               sql.NullString{String: "abc123", Valid: true},
			SQLChildNumber:      sql.NullInt64{Int64: 0, Valid: true},
			Event:               sql.NullString{String: "db file sequential read", Valid: true},
		},
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "ORCL", Valid: true},
			SID:                 sql.NullInt64{Int64: 456, Valid: true},
			WaitTimeMs:          sql.NullFloat64{Float64: 3200, Valid: true},
			SQLID:               sql.NullString{String: "abc123", Valid: true},
			SQLChildNumber:      sql.NullInt64{Int64: 0, Valid: true},
			Event:               sql.NullString{String: "db file sequential read", Valid: true},
		},
	}

	identifiers := scraper.extractSQLIdentifiers(waitEvents, make(map[string]models.SQLIdentifier))

	// Should deduplicate
	assert.Len(t, identifiers, 1)
	assert.Equal(t, "abc123", identifiers[0].SQLID)
	assert.Equal(t, int64(0), identifiers[0].ChildNumber)
}

func TestExtractSQLIdentifiers_InvalidEvents(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	waitEvents := []models.WaitEventWithBlocking{
		{
			// Missing WaitTimeMs - invalid
			CollectionTimestamp: sql.NullTime{Time: time.Now(), Valid: true},
			DatabaseName:        sql.NullString{String: "ORCL", Valid: true},
			SID:                 sql.NullInt64{Int64: 123, Valid: true},
			SQLID:               sql.NullString{String: "abc123", Valid: true},
			SQLChildNumber:      sql.NullInt64{Int64: 0, Valid: true},
		},
	}

	identifiers := scraper.extractSQLIdentifiers(waitEvents, make(map[string]models.SQLIdentifier))

	// Invalid event should not be included
	assert.Empty(t, identifiers)
}

func TestExtractSQLIdentifiers_ZeroTimestamp(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	waitEvents := []models.WaitEventWithBlocking{
		{
			CollectionTimestamp: sql.NullTime{}, // Zero timestamp
			DatabaseName:        sql.NullString{String: "ORCL", Valid: true},
			SID:                 sql.NullInt64{Int64: 123, Valid: true},
			WaitTimeMs:          sql.NullFloat64{Float64: 5500, Valid: true},
			SQLID:               sql.NullString{String: "abc123", Valid: true},
			SQLChildNumber:      sql.NullInt64{Int64: 0, Valid: true},
			Event:               sql.NullString{String: "db file sequential read", Valid: true},
		},
	}

	identifiers := scraper.extractSQLIdentifiers(waitEvents, make(map[string]models.SQLIdentifier))

	// Should use current time when timestamp is zero
	assert.Len(t, identifiers, 1)
	assert.Equal(t, "abc123", identifiers[0].SQLID)
	assert.False(t, identifiers[0].Timestamp.IsZero())
}

func TestRecordBlockingMetrics_ValidBlockedEvent(_ *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	now := time.Now()
	event := &models.WaitEventWithBlocking{
		CollectionTimestamp:        sql.NullTime{Time: now, Valid: true},
		DatabaseName:               sql.NullString{String: "ORCL", Valid: true},
		Username:                   sql.NullString{String: "testuser", Valid: true},
		SID:                        sql.NullInt64{Int64: 123, Valid: true},
		Serial:                     sql.NullInt64{Int64: 456, Valid: true},
		State:                      sql.NullString{String: "WAITING", Valid: true},
		SQLID:                      sql.NullString{String: "abc123", Valid: true},
		SQLChildNumber:             sql.NullInt64{Int64: 0, Valid: true},
		WaitTimeMs:                 sql.NullFloat64{Float64: 5500, Valid: true},
		Event:                      sql.NullString{String: "enq: TX - row lock contention", Valid: true},
		WaitClass:                  sql.NullString{String: "Application", Valid: true},
		BlockingSessionStatus:      sql.NullString{String: "ACTIVE", Valid: true},
		ImmediateBlockerSID:        sql.NullInt64{Int64: 456, Valid: true},
		FinalBlockingSessionStatus: sql.NullString{String: "ACTIVE", Valid: true},
		FinalBlockerSID:            sql.NullInt64{Int64: 456, Valid: true},
		FinalBlockerUser:           sql.NullString{String: "blockinguser", Valid: true},
	}

	// This should not panic
	scraper.recordBlockingMetrics(pcommon.NewTimestampFromTime(now), event, make(map[string]models.SQLIdentifier))
}

func TestRecordBlockingMetrics_ZeroWaitTime(_ *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	now := time.Now()
	event := &models.WaitEventWithBlocking{
		CollectionTimestamp:   sql.NullTime{Time: now, Valid: true},
		DatabaseName:          sql.NullString{String: "ORCL", Valid: true},
		SID:                   sql.NullInt64{Int64: 123, Valid: true},
		WaitTimeMs:            sql.NullFloat64{Float64: 0.0, Valid: true}, // Zero wait time
		BlockingSessionStatus: sql.NullString{String: "ACTIVE", Valid: true},
	}

	// Should return early and not panic
	scraper.recordBlockingMetrics(pcommon.NewTimestampFromTime(now), event, make(map[string]models.SQLIdentifier))
}

func TestRecordBlockingMetrics_NegativeWaitTime(_ *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, _ := NewWaitEventBlockingScraper(mockClient, mb, logger, config, 10)

	now := time.Now()
	event := &models.WaitEventWithBlocking{
		CollectionTimestamp:   sql.NullTime{Time: now, Valid: true},
		DatabaseName:          sql.NullString{String: "ORCL", Valid: true},
		SID:                   sql.NullInt64{Int64: 123, Valid: true},
		WaitTimeMs:            sql.NullFloat64{Float64: -1.0, Valid: true}, // Negative wait time
		BlockingSessionStatus: sql.NullString{String: "ACTIVE", Valid: true},
	}

	// Should return early and not panic
	scraper.recordBlockingMetrics(pcommon.NewTimestampFromTime(now), event, make(map[string]models.SQLIdentifier))
}
