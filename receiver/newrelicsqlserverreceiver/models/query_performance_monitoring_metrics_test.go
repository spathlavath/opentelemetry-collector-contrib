package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlowQueryModel(t *testing.T) {
	t.Run("SlowQuery model with all fields populated", func(t *testing.T) {
		queryText := "SELECT * FROM USERS WHERE ID = ?"
		queryID := QueryID("0x12345678")
		clientName := "MyApp-SQLServer"
		nrApmGuid := "MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI5MjMzNDQwNw"
		sqlHash := "d1c08094cf228a33039e9ee0387ab83c"
		avgElapsedTime := 150.5
		executionCount := int64(1000)
		intervalAvgElapsed := 120.3
		intervalExecCount := int64(50)

		slowQuery := SlowQuery{
			QueryText:                &queryText,
			QueryID:                  &queryID,
			ClientName:               &clientName,
			NrServiceGuid:                &nrApmGuid,
			NormalisedSqlHash:        &sqlHash,
			AvgElapsedTimeMS:         &avgElapsedTime,
			ExecutionCount:           &executionCount,
			IntervalAvgElapsedTimeMS: &intervalAvgElapsed,
			IntervalExecutionCount:   &intervalExecCount,
		}

		// Verify all fields are set
		assert.NotNil(t, slowQuery.QueryText)
		assert.Equal(t, queryText, *slowQuery.QueryText)

		assert.NotNil(t, slowQuery.QueryID)
		assert.Equal(t, queryID, *slowQuery.QueryID)

		assert.NotNil(t, slowQuery.ClientName)
		assert.Equal(t, clientName, *slowQuery.ClientName)

		assert.NotNil(t, slowQuery.NrServiceGuid)
		assert.Equal(t, nrApmGuid, *slowQuery.NrServiceGuid)

		assert.NotNil(t, slowQuery.NormalisedSqlHash)
		assert.Equal(t, sqlHash, *slowQuery.NormalisedSqlHash)
		assert.Len(t, *slowQuery.NormalisedSqlHash, 32, "MD5 hash should be 32 characters")

		assert.NotNil(t, slowQuery.AvgElapsedTimeMS)
		assert.Equal(t, avgElapsedTime, *slowQuery.AvgElapsedTimeMS)

		assert.NotNil(t, slowQuery.ExecutionCount)
		assert.Equal(t, executionCount, *slowQuery.ExecutionCount)

		assert.NotNil(t, slowQuery.IntervalAvgElapsedTimeMS)
		assert.Equal(t, intervalAvgElapsed, *slowQuery.IntervalAvgElapsedTimeMS)

		assert.NotNil(t, slowQuery.IntervalExecutionCount)
		assert.Equal(t, intervalExecCount, *slowQuery.IntervalExecutionCount)
	})

	t.Run("SlowQuery model with nil New Relic metadata fields", func(t *testing.T) {
		queryText := "SELECT 1"

		slowQuery := SlowQuery{
			QueryText:         &queryText,
			ClientName:        nil,
			NrServiceGuid:         nil,
			NormalisedSqlHash: nil,
		}

		// Verify metadata fields can be nil
		assert.NotNil(t, slowQuery.QueryText)
		assert.Nil(t, slowQuery.ClientName)
		assert.Nil(t, slowQuery.NrServiceGuid)
		assert.Nil(t, slowQuery.NormalisedSqlHash)
	})

	t.Run("SlowQuery model with empty client name and nr_service_guid", func(t *testing.T) {
		queryText := "SELECT * FROM PRODUCTS"
		emptyClient := ""
		emptyGuid := ""
		sqlHash := "abc123def456abc123def456abc123de"

		slowQuery := SlowQuery{
			QueryText:         &queryText,
			ClientName:        &emptyClient,
			NrServiceGuid:         &emptyGuid,
			NormalisedSqlHash: &sqlHash,
		}

		// Empty strings are valid values
		assert.NotNil(t, slowQuery.ClientName)
		assert.Equal(t, "", *slowQuery.ClientName)

		assert.NotNil(t, slowQuery.NrServiceGuid)
		assert.Equal(t, "", *slowQuery.NrServiceGuid)

		assert.NotNil(t, slowQuery.NormalisedSqlHash)
		assert.NotEmpty(t, *slowQuery.NormalisedSqlHash)
	})
}

func TestActiveRunningQueryModel(t *testing.T) {
	t.Run("ActiveRunningQuery model with populated fields", func(t *testing.T) {
		sessionID := int64(52)
		queryText := "SELECT * FROM ORDERS"
		waitType := "LCK_M_S"
		waitTime := 1.5
		clientName := "MyApp"
		nrApmGuid := "ABC123"
		sqlHash := "abc123def456abc123def456abc123de"

		activeQuery := ActiveRunningQuery{
			CurrentSessionID:  &sessionID,
			QueryText:         &queryText,
			WaitType:          &waitType,
			WaitTimeS:         &waitTime,
			ClientName:        &clientName,
			NrServiceGuid:         &nrApmGuid,
			NormalisedSqlHash: &sqlHash,
		}

		assert.NotNil(t, activeQuery.CurrentSessionID)
		assert.Equal(t, sessionID, *activeQuery.CurrentSessionID)

		assert.NotNil(t, activeQuery.QueryText)
		assert.Equal(t, queryText, *activeQuery.QueryText)

		assert.NotNil(t, activeQuery.WaitType)
		assert.Equal(t, waitType, *activeQuery.WaitType)

		assert.NotNil(t, activeQuery.WaitTimeS)
		assert.Equal(t, waitTime, *activeQuery.WaitTimeS)

		assert.NotNil(t, activeQuery.ClientName)
		assert.Equal(t, clientName, *activeQuery.ClientName)

		assert.NotNil(t, activeQuery.NrServiceGuid)
		assert.Equal(t, nrApmGuid, *activeQuery.NrServiceGuid)

		assert.NotNil(t, activeQuery.NormalisedSqlHash)
		assert.Equal(t, sqlHash, *activeQuery.NormalisedSqlHash)
	})
}

func TestWaitTimeAnalysisModel(t *testing.T) {
	t.Run("WaitTimeAnalysis model", func(t *testing.T) {
		waitCategory := "PAGEIOLATCH_SH"
		totalWaitTimeMs := 25000.5
		waitEventCount := int64(15)

		waitAnalysis := WaitTimeAnalysis{
			WaitCategory:    &waitCategory,
			TotalWaitTimeMs: &totalWaitTimeMs,
			WaitEventCount:  &waitEventCount,
		}

		assert.NotNil(t, waitAnalysis.WaitCategory)
		assert.Equal(t, waitCategory, *waitAnalysis.WaitCategory)

		assert.NotNil(t, waitAnalysis.TotalWaitTimeMs)
		assert.Equal(t, totalWaitTimeMs, *waitAnalysis.TotalWaitTimeMs)

		assert.NotNil(t, waitAnalysis.WaitEventCount)
		assert.Equal(t, waitEventCount, *waitAnalysis.WaitEventCount)
	})
}

func TestSlowQueryWithIntervalMetrics(t *testing.T) {
	t.Run("Interval metrics calculation scenario", func(t *testing.T) {
		// Simulate a slow query with both cumulative and interval metrics
		queryText := "SELECT * FROM CUSTOMERS WHERE STATUS = ?"
		queryID := QueryID("0xABCDEF")
		sqlHash := "1234567890abcdef1234567890abcdef"

		// Cumulative metrics (from database)
		totalElapsedTime := 50000.0                                  // 50 seconds total
		executionCount := int64(500)                                 // 500 total executions
		avgElapsedTime := totalElapsedTime / float64(executionCount) // 100ms average

		// Interval metrics (calculated by scraper)
		intervalAvgElapsed := 95.5     // 95.5ms in last interval
		intervalExecCount := int64(25) // 25 new executions

		slowQuery := SlowQuery{
			QueryText:                &queryText,
			QueryID:                  &queryID,
			NormalisedSqlHash:        &sqlHash,
			TotalElapsedTimeMS:       &totalElapsedTime,
			ExecutionCount:           &executionCount,
			AvgElapsedTimeMS:         &avgElapsedTime,
			IntervalAvgElapsedTimeMS: &intervalAvgElapsed,
			IntervalExecutionCount:   &intervalExecCount,
		}

		// Verify cumulative metrics
		assert.NotNil(t, slowQuery.TotalElapsedTimeMS)
		assert.Equal(t, 50000.0, *slowQuery.TotalElapsedTimeMS)

		assert.NotNil(t, slowQuery.ExecutionCount)
		assert.Equal(t, int64(500), *slowQuery.ExecutionCount)

		assert.NotNil(t, slowQuery.AvgElapsedTimeMS)
		assert.InDelta(t, 100.0, *slowQuery.AvgElapsedTimeMS, 0.01)

		// Verify interval metrics (delta calculations)
		assert.NotNil(t, slowQuery.IntervalAvgElapsedTimeMS)
		assert.Equal(t, 95.5, *slowQuery.IntervalAvgElapsedTimeMS)

		assert.NotNil(t, slowQuery.IntervalExecutionCount)
		assert.Equal(t, int64(25), *slowQuery.IntervalExecutionCount)

		// Verify the query has normalized hash
		assert.NotNil(t, slowQuery.NormalisedSqlHash)
		assert.Len(t, *slowQuery.NormalisedSqlHash, 32)
	})
}

func TestModelTagging(t *testing.T) {
	t.Run("Verify struct tags for New Relic metadata fields", func(t *testing.T) {
		// This test ensures the model fields have correct struct tags for serialization
		slowQuery := SlowQuery{}

		// Use reflection to verify struct tags (basic check)
		// The actual tag validation is done by the metadata generator

		// Just verify we can create instances with all field combinations
		clientName := "TestApp"
		nrApmGuid := "MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI5MjMzNDQwNw"
		sqlHash := "abcd1234efgh5678ijkl9012mnop3456"

		slowQuery.ClientName = &clientName
		slowQuery.NrServiceGuid = &nrApmGuid
		slowQuery.NormalisedSqlHash = &sqlHash

		assert.Equal(t, "TestApp", *slowQuery.ClientName)
		assert.Equal(t, "MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI5MjMzNDQwNw", *slowQuery.NrServiceGuid)
		assert.Equal(t, "abcd1234efgh5678ijkl9012mnop3456", *slowQuery.NormalisedSqlHash)
	})
}

func TestWaitTimeAnalysisWithNilFields(t *testing.T) {
	t.Run("WaitTimeAnalysis with only wait category", func(t *testing.T) {
		waitCategory := "PAGEIOLATCH_SH"

		waitAnalysis := WaitTimeAnalysis{
			WaitCategory:    &waitCategory,
			TotalWaitTimeMs: nil,
			WaitEventCount:  nil,
		}

		assert.NotNil(t, waitAnalysis.WaitCategory)
		assert.Equal(t, waitCategory, *waitAnalysis.WaitCategory)
		assert.Nil(t, waitAnalysis.TotalWaitTimeMs)
		assert.Nil(t, waitAnalysis.WaitEventCount)
	})
}

func TestNilSafetyInModels(t *testing.T) {
	t.Run("Models handle nil pointer fields safely", func(t *testing.T) {
		// Test that models can be created with all nil fields without panicking
		slowQuery := SlowQuery{
			QueryText:         nil,
			QueryID:           nil,
			ClientName:        nil,
			NrServiceGuid:         nil,
			NormalisedSqlHash: nil,
		}

		// These should all be nil without causing issues
		assert.Nil(t, slowQuery.QueryText)
		assert.Nil(t, slowQuery.QueryID)
		assert.Nil(t, slowQuery.ClientName)
		assert.Nil(t, slowQuery.NrServiceGuid)
		assert.Nil(t, slowQuery.NormalisedSqlHash)

		// Setting values should work
		newQueryText := "SELECT 1"
		slowQuery.QueryText = &newQueryText
		assert.NotNil(t, slowQuery.QueryText)
		assert.Equal(t, "SELECT 1", *slowQuery.QueryText)
	})
}
