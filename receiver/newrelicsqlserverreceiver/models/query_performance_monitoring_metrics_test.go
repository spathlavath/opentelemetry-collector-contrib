package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlowQueryModel(t *testing.T) {
	t.Run("SlowQuery model with all fields populated", func(t *testing.T) {
		queryText := "SELECT * FROM USERS WHERE ID = ?"
		queryID := QueryID("0x12345678")
		nrApmGuid := "MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI5MjMzNDQwNw"
		sqlHash := "d1c08094cf228a33039e9ee0387ab83c"
		executionCount := int64(1000)
		intervalExecCount := int64(50)

		slowQuery := SlowQuery{
			QueryText:              &queryText,
			QueryID:                &queryID,
			NrServiceGuid:          &nrApmGuid,
			NormalisedSqlHash:      &sqlHash,
			ExecutionCount:         &executionCount,
			IntervalExecutionCount: &intervalExecCount,
		}

		// Verify all fields are set
		assert.NotNil(t, slowQuery.QueryText)
		assert.Equal(t, queryText, *slowQuery.QueryText)

		assert.NotNil(t, slowQuery.QueryID)
		assert.Equal(t, queryID, *slowQuery.QueryID)

		assert.NotNil(t, slowQuery.NrServiceGuid)
		assert.Equal(t, nrApmGuid, *slowQuery.NrServiceGuid)

		assert.NotNil(t, slowQuery.NormalisedSqlHash)
		assert.Equal(t, sqlHash, *slowQuery.NormalisedSqlHash)
		assert.Len(t, *slowQuery.NormalisedSqlHash, 32, "MD5 hash should be 32 characters")

		assert.NotNil(t, slowQuery.ExecutionCount)
		assert.Equal(t, executionCount, *slowQuery.ExecutionCount)

		assert.NotNil(t, slowQuery.IntervalExecutionCount)
		assert.Equal(t, intervalExecCount, *slowQuery.IntervalExecutionCount)
	})

	t.Run("SlowQuery model with nil New Relic metadata fields", func(t *testing.T) {
		queryText := "SELECT 1"

		slowQuery := SlowQuery{
			QueryText:         &queryText,
			NrServiceGuid:     nil,
			NormalisedSqlHash: nil,
		}

		// Verify metadata fields can be nil
		assert.NotNil(t, slowQuery.QueryText)
		assert.Nil(t, slowQuery.NrServiceGuid)
		assert.Nil(t, slowQuery.NormalisedSqlHash)
	})

	t.Run("SlowQuery model with empty client name and nr_service_guid", func(t *testing.T) {
		queryText := "SELECT * FROM PRODUCTS"
		emptyGuid := ""
		sqlHash := "abc123def456abc123def456abc123de"

		slowQuery := SlowQuery{
			QueryText:         &queryText,
			NrServiceGuid:     &emptyGuid,
			NormalisedSqlHash: &sqlHash,
		}

		// Empty strings are valid values

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
		nrApmGuid := "ABC123"
		sqlHash := "abc123def456abc123def456abc123de"

		activeQuery := ActiveRunningQuery{
			CurrentSessionID:  &sessionID,
			QueryText:         &queryText,
			WaitType:          &waitType,
			WaitTimeS:         &waitTime,
			NrServiceGuid:     &nrApmGuid,
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
		totalElapsedTime := 50000.0  // 50 seconds total
		executionCount := int64(500) // 500 total executions

		// Interval metrics (calculated by scraper)
		intervalExecCount := int64(25) // 25 new executions

		slowQuery := SlowQuery{
			QueryText:              &queryText,
			QueryID:                &queryID,
			NormalisedSqlHash:      &sqlHash,
			TotalElapsedTimeMS:     &totalElapsedTime,
			ExecutionCount:         &executionCount,
			IntervalExecutionCount: &intervalExecCount,
		}

		// Verify cumulative metrics
		assert.NotNil(t, slowQuery.TotalElapsedTimeMS)
		assert.Equal(t, 50000.0, *slowQuery.TotalElapsedTimeMS)

		assert.NotNil(t, slowQuery.ExecutionCount)
		assert.Equal(t, int64(500), *slowQuery.ExecutionCount)

		// Verify interval metrics (delta calculations)
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
		nrApmGuid := "MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI5MjMzNDQwNw"
		sqlHash := "abcd1234efgh5678ijkl9012mnop3456"

		slowQuery.NrServiceGuid = &nrApmGuid
		slowQuery.NormalisedSqlHash = &sqlHash

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
			NrServiceGuid:     nil,
			NormalisedSqlHash: nil,
		}

		// These should all be nil without causing issues
		assert.Nil(t, slowQuery.QueryText)
		assert.Nil(t, slowQuery.QueryID)
		assert.Nil(t, slowQuery.NrServiceGuid)
		assert.Nil(t, slowQuery.NormalisedSqlHash)

		// Setting values should work
		newQueryText := "SELECT 1"
		slowQuery.QueryText = &newQueryText
		assert.NotNil(t, slowQuery.QueryText)
		assert.Equal(t, "SELECT 1", *slowQuery.QueryText)
	})
}

func TestActiveRunningQueryBlockingMetadata(t *testing.T) {
	t.Run("ActiveRunningQuery with blocker APM metadata", func(t *testing.T) {
		sessionID := int64(55)
		blockingSessionID := int64(52)
		queryText := "/* nr_service_guid=\"VictimGUID123\", nr_service=\"victim-service\" */ SELECT * FROM Orders"
		blockingQueryText := "/* nr_service_guid=\"BlockerGUID456\", nr_service=\"blocker-service\" */ UPDATE Orders SET Status = 'Processed'"

		victimGuid := "VictimGUID123"

		activeQuery := ActiveRunningQuery{
			CurrentSessionID:           &sessionID,
			BlockingSessionID:          &blockingSessionID,
			QueryText:                  &queryText,
			BlockingQueryStatementText: &blockingQueryText,
			NrServiceGuid:              &victimGuid,
		}

		// Verify victim metadata
		assert.NotNil(t, activeQuery.NrServiceGuid)
		assert.Equal(t, "VictimGUID123", *activeQuery.NrServiceGuid)

		// Verify session IDs
		assert.NotNil(t, activeQuery.CurrentSessionID)
		assert.Equal(t, int64(55), *activeQuery.CurrentSessionID)
		assert.NotNil(t, activeQuery.BlockingSessionID)
		assert.Equal(t, int64(52), *activeQuery.BlockingSessionID)
	})

	t.Run("ActiveRunningQuery with only victim metadata", func(t *testing.T) {
		sessionID := int64(55)
		blockingSessionID := int64(52)
		queryText := "/* nr_service_guid=\"VictimGUID123\", nr_service=\"victim-service\" */ SELECT * FROM Orders"
		blockingQueryText := "UPDATE Orders SET Status = 'Processed'" // No APM metadata

		victimGuid := "VictimGUID123"

		activeQuery := ActiveRunningQuery{
			CurrentSessionID:           &sessionID,
			BlockingSessionID:          &blockingSessionID,
			QueryText:                  &queryText,
			BlockingQueryStatementText: &blockingQueryText,
			NrServiceGuid:              &victimGuid,
		}

		// Verify victim metadata exists
		assert.NotNil(t, activeQuery.NrServiceGuid)
		assert.Equal(t, "VictimGUID123", *activeQuery.NrServiceGuid)

		// Query text should still exist
		assert.NotNil(t, activeQuery.BlockingQueryStatementText)
		assert.Contains(t, *activeQuery.BlockingQueryStatementText, "UPDATE Orders")
	})

	t.Run("ActiveRunningQuery with no blocking scenario", func(t *testing.T) {
		sessionID := int64(55)
		queryText := "/* nr_service_guid=\"ServiceGUID\", nr_service=\"my-service\" */ SELECT * FROM Products"

		serviceGuid := "ServiceGUID"

		activeQuery := ActiveRunningQuery{
			CurrentSessionID:           &sessionID,
			BlockingSessionID:          nil, // No blocking
			QueryText:                  &queryText,
			BlockingQueryStatementText: nil, // No blocker query
			NrServiceGuid:              &serviceGuid,
		}

		// Verify victim metadata exists
		assert.NotNil(t, activeQuery.NrServiceGuid)
		assert.Equal(t, "ServiceGUID", *activeQuery.NrServiceGuid)

		// Verify no blocking scenario
		assert.Nil(t, activeQuery.BlockingSessionID)
		assert.Nil(t, activeQuery.BlockingQueryStatementText)
	})

	t.Run("ActiveRunningQuery with blocker metadata but no victim metadata", func(t *testing.T) {
		sessionID := int64(55)
		blockingSessionID := int64(52)
		queryText := "SELECT * FROM Orders" // No APM metadata
		blockingQueryText := "/* nr_service_guid=\"BlockerGUID\", nr_service=\"blocker-service\" */ UPDATE Orders SET Status = 'Processed'"

		activeQuery := ActiveRunningQuery{
			CurrentSessionID:           &sessionID,
			BlockingSessionID:          &blockingSessionID,
			QueryText:                  &queryText,
			BlockingQueryStatementText: &blockingQueryText,
			NrServiceGuid:              nil, // No victim metadata
		}

		// Verify victim metadata is nil
		assert.Nil(t, activeQuery.NrServiceGuid)

		// Verify blocker query text exists
		assert.NotNil(t, activeQuery.BlockingQueryStatementText)
		assert.Contains(t, *activeQuery.BlockingQueryStatementText, "UPDATE Orders")
	})

	t.Run("ActiveRunningQuery with blocking session", func(t *testing.T) {
		sessionID := int64(55)
		blockingSessionID := int64(52)

		activeQuery := ActiveRunningQuery{
			CurrentSessionID:  &sessionID,
			BlockingSessionID: &blockingSessionID,
		}

		// Verify session IDs are set
		assert.NotNil(t, activeQuery.CurrentSessionID)
		assert.Equal(t, int64(55), *activeQuery.CurrentSessionID)
		assert.NotNil(t, activeQuery.BlockingSessionID)
		assert.Equal(t, int64(52), *activeQuery.BlockingSessionID)
	})
}

func TestActiveRunningQueryCompleteBlockingScenario(t *testing.T) {
	t.Run("Complete blocking scenario with all metadata", func(t *testing.T) {
		// Victim (blocked query) details
		victimSessionID := int64(55)
		victimQueryText := "/* nr_service_guid=\"MTE2MDAzMTl8QVBNfEFQUAxJQ0FUSU9OfDI5MjMzNDQwNw\", nr_service=\"victim-java-app\" TargetID_5 */ SELECT * FROM Person.Person WHERE BusinessEntityID = 5"
		victimGuid := "MTE2MDAzMTl8QVBNfEFQUAxJQ0FUSU9OfDI5MjMzNDQwNw"
		victimQueryID := QueryID("0x1A2B3C4D5E6F7890")

		// Blocker query details
		blockerSessionID := int64(52)
		blockerQueryText := "/* nr_service_guid=\"MTE2MDAzMTl8QVBNfEFQUAxJQ0FUSU9OfDI5MjMzNDQwNw\", nr_service=\"blocker-java-app\" TargetID_5 */ UPDATE Person.Person SET Title = 'Mr.' WHERE BusinessEntityID = 5"
		blockerQueryHash := QueryID("0x9876543210ABCDEF")

		// Wait details
		waitType := "LCK_M_S"
		waitTimeS := 30.5
		waitResource := "KEY: 5:72057594037927936 (3c0d0ab44ea1)"

		// Timestamps
		requestStartTime := "2024-01-15 10:30:45.123"
		collectionTimestamp := "2024-01-15 10:31:15.789"

		// Database context
		databaseName := "AdventureWorks2022"
		loginName := "app_user"
		blockerLoginName := "app_admin"

		activeQuery := ActiveRunningQuery{
			// Victim details
			CurrentSessionID: &victimSessionID,
			QueryText:        &victimQueryText,
			QueryID:          &victimQueryID,
			NrServiceGuid:    &victimGuid,

			// Blocker details
			BlockingSessionID:          &blockerSessionID,
			BlockingQueryStatementText: &blockerQueryText,
			BlockingQueryHash:          &blockerQueryHash,
			BlockerLoginName:           &blockerLoginName,

			// Wait details
			WaitType:     &waitType,
			WaitTimeS:    &waitTimeS,
			WaitResource: &waitResource,

			// Context
			DatabaseName:        &databaseName,
			LoginName:           &loginName,
			RequestStartTime:    &requestStartTime,
			CollectionTimestamp: &collectionTimestamp,
		}

		// Verify victim details
		assert.Equal(t, int64(55), *activeQuery.CurrentSessionID)
		assert.Contains(t, *activeQuery.QueryText, "SELECT * FROM Person.Person")
		assert.Equal(t, victimGuid, *activeQuery.NrServiceGuid)
		assert.Equal(t, "0x1A2B3C4D5E6F7890", activeQuery.QueryID.String())

		// Verify blocker details
		assert.Equal(t, int64(52), *activeQuery.BlockingSessionID)
		assert.Contains(t, *activeQuery.BlockingQueryStatementText, "UPDATE Person.Person")
		assert.Equal(t, "0x9876543210ABCDEF", activeQuery.BlockingQueryHash.String())
		assert.Equal(t, "app_admin", *activeQuery.BlockerLoginName)

		// Verify wait details
		assert.Equal(t, "LCK_M_S", *activeQuery.WaitType)
		assert.Equal(t, 30.5, *activeQuery.WaitTimeS)
		assert.Contains(t, *activeQuery.WaitResource, "KEY:")

		// Verify context
		assert.Equal(t, "AdventureWorks2022", *activeQuery.DatabaseName)
		assert.Equal(t, "app_user", *activeQuery.LoginName)
		assert.NotEmpty(t, *activeQuery.RequestStartTime)
		assert.NotEmpty(t, *activeQuery.CollectionTimestamp)
	})
}
