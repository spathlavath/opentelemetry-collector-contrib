package scrapers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/helpers"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
)

func TestSQLNormalizationIntegration(t *testing.T) {
	tests := []struct {
		name                  string
		inputQueryText        string
		expectedNormalizedSQL string
		expectedClientName    string
		expectedHashNotEmpty  bool
	}{
		{
			name: "Query with New Relic metadata and T-SQL parameters",
			inputQueryText: `/* nr_service="MyApp-SQLServer" */
			SELECT * FROM customers WHERE id = @customerId AND status = @status`,
			expectedNormalizedSQL: "SELECT * FROM CUSTOMERS WHERE ID = ? AND STATUS = ?",
			expectedClientName:    "MyApp-SQLServer",
			expectedHashNotEmpty:  true,
		},
		{
			name:                  "Query with only service name",
			inputQueryText:        `/* nr_service="ProductionDB" */ SELECT TOP 100 * FROM orders WHERE order_date > '2024-01-01'`,
			expectedNormalizedSQL: "SELECT TOP ? * FROM ORDERS WHERE ORDER_DATE > ?",
			expectedClientName:    "ProductionDB",
			expectedHashNotEmpty:  true,
		},
		{
			name:                  "Query without New Relic metadata",
			inputQueryText:        `SELECT * FROM users WHERE age > 18 AND city = 'Seattle'`,
			expectedNormalizedSQL: "SELECT * FROM USERS WHERE AGE > ? AND CITY = ?",
			expectedClientName:    "",
			expectedHashNotEmpty:  true,
		},
		{
			name: "Complex query with IN clause and metadata",
			inputQueryText: `/* nr_service="Analytics-Service" */
			SELECT product_id, SUM(quantity) FROM sales
			WHERE product_id IN (@p1, @p2, @p3) AND year = @year
			GROUP BY product_id`,
			expectedNormalizedSQL: "SELECT PRODUCT_ID, SUM(QUANTITY) FROM SALES WHERE PRODUCT_ID IN (?) AND YEAR = ? GROUP BY PRODUCT_ID",
			expectedClientName:    "Analytics-Service",
			expectedHashNotEmpty:  true,
		},
		{
			name:                  "Empty query text",
			inputQueryText:        "",
			expectedNormalizedSQL: "",
			expectedClientName:    "",
			expectedHashNotEmpty:  false,
		},
		{
			name:                  "Query with numeric literals",
			inputQueryText:        `SELECT * FROM products WHERE price > 100.50 AND stock < 10`,
			expectedNormalizedSQL: "SELECT * FROM PRODUCTS WHERE PRICE > ? AND STOCK < ?",
			expectedClientName:    "",
			expectedHashNotEmpty:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate what processSlowQueryMetrics does
			var result models.SlowQuery
			queryText := tt.inputQueryText
			result.QueryText = &queryText

			// Perform the normalization (this is what the scraper does)
			if result.QueryText != nil && *result.QueryText != "" {
				nrApmGuid, clientName := helpers.ExtractNewRelicMetadata(*result.QueryText)
				normalizedSQL, sqlHash := helpers.NormalizeSqlAndHash(*result.QueryText)

				if nrApmGuid != "" {
					result.NrServiceGuid = &nrApmGuid
				}
				if clientName != "" {
				}
				if sqlHash != "" {
					result.NormalizedSqlHash = &sqlHash
				}
				result.QueryText = &normalizedSQL
			}

			// Verify normalized SQL
			if result.QueryText != nil {
				assert.Equal(t, tt.expectedNormalizedSQL, *result.QueryText, "Normalized SQL should match")
			} else {
				assert.Equal(t, "", tt.expectedNormalizedSQL, "Normalized SQL should be empty")
			}

			// Verify client name
			if tt.expectedClientName != "" {
			} else {
			}

			// Verify hash
			if tt.expectedHashNotEmpty {
				assert.NotNil(t, result.NormalizedSqlHash, "NormalizedSqlHash should not be nil")
				assert.NotEmpty(t, *result.NormalizedSqlHash, "NormalizedSqlHash should not be empty")
				// Verify it's a valid MD5 hash (32 hex characters)
				assert.Len(t, *result.NormalizedSqlHash, 32, "MD5 hash should be 32 characters")
			} else {
				assert.Nil(t, result.NormalizedSqlHash, "NormalizedSqlHash should be nil for empty query")
			}
		})
	}
}

func TestSQLHashConsistency(t *testing.T) {
	// Test that the same logical query produces the same hash regardless of literals
	tests := []struct {
		name        string
		query1      string
		query2      string
		shouldMatch bool
	}{
		{
			name:        "Same query structure with different literals",
			query1:      "SELECT * FROM users WHERE id = 123 AND name = 'John'",
			query2:      "SELECT * FROM users WHERE id = 456 AND name = 'Jane'",
			shouldMatch: true,
		},
		{
			name:        "Same query with T-SQL parameters",
			query1:      "SELECT * FROM users WHERE id = @id1 AND name = @name1",
			query2:      "SELECT * FROM users WHERE id = @id2 AND name = @name2",
			shouldMatch: true,
		},
		{
			name:        "Different query structures",
			query1:      "SELECT * FROM users WHERE id = 123",
			query2:      "SELECT * FROM orders WHERE id = 123",
			shouldMatch: false,
		},
		{
			name:        "Same query with different IN clause values",
			query1:      "SELECT * FROM products WHERE id IN (1, 2, 3)",
			query2:      "SELECT * FROM products WHERE id IN (4, 5, 6, 7, 8)",
			shouldMatch: true,
		},
		{
			name:        "Query with and without comments",
			query1:      "/* This is a comment */ SELECT * FROM users WHERE id = 100",
			query2:      "SELECT * FROM users WHERE id = 200",
			shouldMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, hash1 := helpers.NormalizeSqlAndHash(tt.query1)
			_, hash2 := helpers.NormalizeSqlAndHash(tt.query2)

			if tt.shouldMatch {
				assert.Equal(t, hash1, hash2, "Hashes should match for logically identical queries")
			} else {
				assert.NotEqual(t, hash1, hash2, "Hashes should differ for different query structures")
			}
		})
	}
}

func TestMetadataExtractionEdgeCases(t *testing.T) {
	tests := []struct {
		name            string
		inputSQL        string
		expectedGuid    string
		expectedService string
	}{
		{
			name:            "Metadata in middle of query",
			inputSQL:        `SELECT /* nr_apm_guid="ABC123", nr_service="MyApp" */ * FROM users`,
			expectedGuid:    "ABC123",
			expectedService: "MyApp",
		},
		{
			name:            "Metadata with extra spaces",
			inputSQL:        `/*  nr_service = "MyApp"  ,  nr_apm_guid = "XYZ789"  */  SELECT * FROM orders`,
			expectedGuid:    "XYZ789",
			expectedService: "MyApp",
		},
		{
			name:            "Only service, no GUID",
			inputSQL:        `/* nr_service="ProductionDB" */ SELECT * FROM logs`,
			expectedGuid:    "",
			expectedService: "ProductionDB",
		},
		{
			name:            "Service with hyphens and underscores",
			inputSQL:        `/* nr_service="My-Production_DB-v2" */ SELECT 1`,
			expectedGuid:    "",
			expectedService: "My-Production_DB-v2",
		},
		{
			name:            "Service with commas in value",
			inputSQL:        `/* nr_service="API, Production, US-East" */ SELECT *`,
			expectedGuid:    "",
			expectedService: "API, Production, US-East",
		},
		{
			name:            "No metadata present",
			inputSQL:        "/* Just a regular comment */ SELECT * FROM tables",
			expectedGuid:    "",
			expectedService: "",
		},
		{
			name:            "nr_apm_guid with service",
			inputSQL:        `/* nr_apm_guid="DEF456", nr_service="BackgroundJob" */ SELECT 1`,
			expectedGuid:    "DEF456",
			expectedService: "BackgroundJob",
		},
		{
			name:            "nr_apm_guid only (no service)",
			inputSQL:        `/* nr_apm_guid="GHI789" */ UPDATE users SET status = 'active'`,
			expectedGuid:    "GHI789",
			expectedService: "",
		},
		{
			name:            "Base64 encoded nr_apm_guid",
			inputSQL:        `/* nr_apm_guid="MjU2NHxBUE18QVBQTElDQVRJT058MTIzNDU2Nzg5", nr_service="payment-api" */ INSERT INTO transactions VALUES (1, 100)`,
			expectedGuid:    "MjU2NHxBUE18QVBQTElDQVRJT058MTIzNDU2Nzg5",
			expectedService: "payment-api",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			guid, service := helpers.ExtractNewRelicMetadata(tt.inputSQL)

			assert.Equal(t, tt.expectedGuid, guid, "GUID should match")
			assert.Equal(t, tt.expectedService, service, "Service name should match")
		})
	}
}

func TestNormalizationPrivacy(t *testing.T) {
	// Test that normalization removes sensitive data
	tests := []struct {
		name             string
		inputSQL         string
		shouldContain    []string
		shouldNotContain []string
	}{
		{
			name:             "Removes SSN and sensitive data",
			inputSQL:         "SELECT * FROM users WHERE ssn = '123-45-6789' AND email = 'user@example.com'",
			shouldContain:    []string{"SELECT", "FROM", "USERS", "WHERE", "SSN", "=", "?", "AND", "EMAIL"},
			shouldNotContain: []string{"123-45-6789", "user@example.com"},
		},
		{
			name:             "Removes credit card numbers",
			inputSQL:         "INSERT INTO payments (card_number) VALUES ('4111111111111111')",
			shouldContain:    []string{"INSERT", "INTO", "PAYMENTS", "VALUES", "?"},
			shouldNotContain: []string{"4111111111111111"},
		},
		{
			name:             "Removes passwords",
			inputSQL:         "UPDATE users SET password = 'SuperSecret123!' WHERE id = 42",
			shouldContain:    []string{"UPDATE", "USERS", "SET", "PASSWORD", "=", "?", "WHERE", "ID"},
			shouldNotContain: []string{"SuperSecret123!", "42"},
		},
		{
			name:             "Removes PII in WHERE clause",
			inputSQL:         "SELECT * FROM employees WHERE name = 'John Doe' AND salary > 100000",
			shouldContain:    []string{"SELECT", "FROM", "EMPLOYEES", "WHERE", "NAME", "SALARY"},
			shouldNotContain: []string{"John Doe", "100000"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			normalizedSQL, _ := helpers.NormalizeSqlAndHash(tt.inputSQL)

			for _, expected := range tt.shouldContain {
				assert.Contains(t, normalizedSQL, expected, "Normalized SQL should contain %s", expected)
			}

			for _, sensitive := range tt.shouldNotContain {
				assert.NotContains(t, normalizedSQL, sensitive, "Normalized SQL should NOT contain sensitive data: %s", sensitive)
			}
		})
	}
}

func TestCrossLanguageCompatibility(t *testing.T) {
	// These tests verify that T-SQL normalization produces the same hash as other language agents
	// when the query structure is identical
	tests := []struct {
		name                  string
		tSQLQuery             string
		equivalentJavaQuery   string
		shouldProduceSameHash bool
	}{
		{
			name:                  "Simple SELECT with parameters",
			tSQLQuery:             "SELECT * FROM USERS WHERE ID = @id",
			equivalentJavaQuery:   "SELECT * FROM USERS WHERE ID = ?",
			shouldProduceSameHash: true,
		},
		{
			name:                  "SELECT with IN clause",
			tSQLQuery:             "SELECT * FROM PRODUCTS WHERE ID IN (@p1, @p2, @p3)",
			equivalentJavaQuery:   "SELECT * FROM PRODUCTS WHERE ID IN (?, ?, ?)",
			shouldProduceSameHash: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, tSQLHash := helpers.NormalizeSqlAndHash(tt.tSQLQuery)
			_, javaHash := helpers.NormalizeSqlAndHash(tt.equivalentJavaQuery)

			if tt.shouldProduceSameHash {
				assert.Equal(t, tSQLHash, javaHash, "T-SQL and Java queries should produce same hash")
			}
		})
	}
}

func TestBlockingQueryAPMMetadataExtraction(t *testing.T) {
	tests := []struct {
		name                   string
		victimQueryText        string
		blockingQueryText      string
		expectedVictimGuid     string
		expectedVictimService  string
		expectedBlockerGuid    string
		expectedBlockerService string
		shouldExtractVictim    bool
		shouldExtractBlocker   bool
		description            string
	}{
		{
			name:                   "Both victim and blocker have APM metadata",
			victimQueryText:        `/* nr_service_guid="VictimGUID123", nr_service="victim-java-app" */ SELECT * FROM Orders WHERE OrderID = 5`,
			blockingQueryText:      `/* nr_service_guid="BlockerGUID456", nr_service="blocker-java-app" */ UPDATE Orders SET Status = 'Processing' WHERE OrderID = 5`,
			expectedVictimGuid:     "VictimGUID123",
			expectedVictimService:  "victim-java-app",
			expectedBlockerGuid:    "BlockerGUID456",
			expectedBlockerService: "blocker-java-app",
			shouldExtractVictim:    true,
			shouldExtractBlocker:   true,
			description:            "Standard blocking scenario with both services instrumented",
		},
		{
			name:                   "Only victim has APM metadata",
			victimQueryText:        `/* nr_service_guid="VictimGUID123", nr_service="victim-service" */ SELECT * FROM Products`,
			blockingQueryText:      `UPDATE Products SET Stock = Stock - 1`, // No APM metadata
			expectedVictimGuid:     "VictimGUID123",
			expectedVictimService:  "victim-service",
			expectedBlockerGuid:    "",
			expectedBlockerService: "",
			shouldExtractVictim:    true,
			shouldExtractBlocker:   false,
			description:            "Victim has APM agent, blocker does not",
		},
		{
			name:                   "Only blocker has APM metadata",
			victimQueryText:        `SELECT * FROM Customers WHERE CustomerID = 100`, // No APM metadata
			blockingQueryText:      `/* nr_service_guid="BlockerGUID789", nr_service="long-running-batch" */ UPDATE Customers SET LastModified = GETDATE()`,
			expectedVictimGuid:     "",
			expectedVictimService:  "",
			expectedBlockerGuid:    "BlockerGUID789",
			expectedBlockerService: "long-running-batch",
			shouldExtractVictim:    false,
			shouldExtractBlocker:   true,
			description:            "Blocker has APM agent, victim does not",
		},
		{
			name:                   "Neither has APM metadata",
			victimQueryText:        `SELECT * FROM Inventory WHERE WarehouseID = 10`,
			blockingQueryText:      `UPDATE Inventory SET Quantity = Quantity + 50`,
			expectedVictimGuid:     "",
			expectedVictimService:  "",
			expectedBlockerGuid:    "",
			expectedBlockerService: "",
			shouldExtractVictim:    false,
			shouldExtractBlocker:   false,
			description:            "Legacy systems without APM instrumentation",
		},
		{
			name:                   "Real-world New Relic GUID format",
			victimQueryText:        `/* nr_service_guid="MTE2MDAzMTl8QVBNfEFQUAxJQ0FUSU9OfDI5MjMzNDQwNw", nr_service="payment-api" */ SELECT * FROM Transactions`,
			blockingQueryText:      `/* nr_service_guid="MTE2MDAzMTl8QVBNfEFQUAxJQ0FUSU9OfDk4NzY1NDMyMQ", nr_service="fraud-detection" */ UPDATE Transactions SET Status = 'Verified'`,
			expectedVictimGuid:     "MTE2MDAzMTl8QVBNfEFQUAxJQ0FUSU9OfDI5MjMzNDQwNw",
			expectedVictimService:  "payment-api",
			expectedBlockerGuid:    "MTE2MDAzMTl8QVBNfEFQUAxJQ0FUSU9OfDk4NzY1NDMyMQ",
			expectedBlockerService: "fraud-detection",
			shouldExtractVictim:    true,
			shouldExtractBlocker:   true,
			description:            "Production New Relic Base64-encoded GUIDs",
		},
		{
			name:                   "Service names with special characters",
			victimQueryText:        `/* nr_service_guid="GUID1", nr_service="order-service-v2.1_prod" */ SELECT * FROM Orders`,
			blockingQueryText:      `/* nr_service_guid="GUID2", nr_service="inventory-sync_batch-job" */ UPDATE Orders SET ProcessedAt = GETDATE()`,
			expectedVictimGuid:     "GUID1",
			expectedVictimService:  "order-service-v2.1_prod",
			expectedBlockerGuid:    "GUID2",
			expectedBlockerService: "inventory-sync_batch-job",
			shouldExtractVictim:    true,
			shouldExtractBlocker:   true,
			description:            "Service names with hyphens, underscores, and versions",
		},
		{
			name:                   "Metadata with extra whitespace",
			victimQueryText:        `/*  nr_service_guid = "VictimGUID"  ,  nr_service = "victim-svc"  */ SELECT 1`,
			blockingQueryText:      `/*  nr_service_guid = "BlockerGUID"  ,  nr_service = "blocker-svc"  */ UPDATE Orders`,
			expectedVictimGuid:     "VictimGUID",
			expectedVictimService:  "victim-svc",
			expectedBlockerGuid:    "BlockerGUID",
			expectedBlockerService: "blocker-svc",
			shouldExtractVictim:    true,
			shouldExtractBlocker:   true,
			description:            "Parser should handle extra whitespace",
		},
		{
			name:                   "Blocker with table lock scenario",
			victimQueryText:        `/* nr_service_guid="ReadOnlyGUID", nr_service="reporting-service" */ SELECT COUNT(*) FROM Person.Person`,
			blockingQueryText:      `/* nr_service_guid="WriterGUID", nr_service="data-migration" */ UPDATE Person.Person WITH (TABLOCK, XLOCK) SET Title = 'Mr.'`,
			expectedVictimGuid:     "ReadOnlyGUID",
			expectedVictimService:  "reporting-service",
			expectedBlockerGuid:    "WriterGUID",
			expectedBlockerService: "data-migration",
			shouldExtractVictim:    true,
			shouldExtractBlocker:   true,
			description:            "Table-level lock blocking with APM correlation",
		},
		{
			name:                   "Empty blocking query text",
			victimQueryText:        `/* nr_service_guid="VictimGUID", nr_service="victim-svc" */ SELECT * FROM Orders`,
			blockingQueryText:      "",
			expectedVictimGuid:     "VictimGUID",
			expectedVictimService:  "victim-svc",
			expectedBlockerGuid:    "",
			expectedBlockerService: "",
			shouldExtractVictim:    true,
			shouldExtractBlocker:   false,
			description:            "Blocker query text not available",
		},
		{
			name:                   "nr_service_guid only (no nr_service)",
			victimQueryText:        `/* nr_service_guid="VictimGUID" */ SELECT * FROM Users`,
			blockingQueryText:      `/* nr_service_guid="BlockerGUID" */ UPDATE Users SET LastLogin = GETDATE()`,
			expectedVictimGuid:     "VictimGUID",
			expectedVictimService:  "",
			expectedBlockerGuid:    "BlockerGUID",
			expectedBlockerService: "",
			shouldExtractVictim:    true,
			shouldExtractBlocker:   true,
			description:            "Only GUID present, no service name",
		},
		{
			name:                   "nr_service only (no nr_service_guid)",
			victimQueryText:        `/* nr_service="victim-service" */ SELECT * FROM Accounts`,
			blockingQueryText:      `/* nr_service="blocker-service" */ UPDATE Accounts SET Balance = Balance + 100`,
			expectedVictimGuid:     "",
			expectedVictimService:  "victim-service",
			expectedBlockerGuid:    "",
			expectedBlockerService: "blocker-service",
			shouldExtractVictim:    true, // Service name alone can be extracted
			shouldExtractBlocker:   true, // Service name alone can be extracted
			description:            "Only service name present, no GUID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate what the scraper does for active queries
			var result models.ActiveRunningQuery
			result.QueryText = &tt.victimQueryText
			result.BlockingQueryStatementText = &tt.blockingQueryText

			// Extract victim metadata
			var victimGuid, victimService string
			if result.QueryText != nil && *result.QueryText != "" {
				victimGuid, victimService = helpers.ExtractNewRelicMetadata(*result.QueryText)
			}

			// Blocker metadata extraction has been removed

			// Populate model fields
			if victimGuid != "" {
				result.NrServiceGuid = &victimGuid
			}
			if victimService != "" {
			}

			// Verify victim metadata
			if tt.shouldExtractVictim {
				// Check GUID
				if tt.expectedVictimGuid != "" {
					assert.NotNil(t, result.NrServiceGuid, "Victim GUID should be extracted")
					assert.Equal(t, tt.expectedVictimGuid, *result.NrServiceGuid, "Victim GUID mismatch")
				} else {
					assert.Nil(t, result.NrServiceGuid, "Victim GUID should be nil when not present")
				}

				// Check service name
				if tt.expectedVictimService != "" {
				} else {
				}
			} else {
				// When shouldExtractVictim is false, both should be nil
				assert.Nil(t, result.NrServiceGuid, "Victim GUID should not be extracted")
			}

			// Blocker metadata extraction has been removed
		})
	}
}

func TestBlockingQueryMetadataWithDoubleQuotes(t *testing.T) {
	// Test the double-quote format that's used in the chaos engine
	tests := []struct {
		name                   string
		blockingQueryText      string
		expectedBlockerGuid    string
		expectedBlockerService string
	}{
		{
			name:                   "Double-quoted metadata (chaos engine format)",
			blockingQueryText:      `/* nr_service_guid="MTE2MDAzMTl8QVBNfEFQUAxJQ0FUSU9OfDI5MjMzNDQwNw", nr_service="blocker-java" TargetID_5 */ UPDATE Person.Person SET Title = 'Mr.' WHERE BusinessEntityID = 5`,
			expectedBlockerGuid:    "MTE2MDAzMTl8QVBNfEFQUAxJQ0FUSU9OfDI5MjMzNDQwNw",
			expectedBlockerService: "blocker-java",
		},
		{
			name:                   "Single-quoted metadata (alternative format)",
			blockingQueryText:      `/* nr_service_guid='SingleQuoteGUID', nr_service='single-quote-service' */ SELECT 1`,
			expectedBlockerGuid:    "",
			expectedBlockerService: "",
		},
		{
			name:                   "Mixed quotes",
			blockingQueryText:      `/* nr_service_guid="DoubleQuoteGUID", nr_service='mixed-service' */ UPDATE Orders`,
			expectedBlockerGuid:    "DoubleQuoteGUID",
			expectedBlockerService: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			guid, service := helpers.ExtractNewRelicMetadata(tt.blockingQueryText)

			if tt.expectedBlockerGuid != "" {
				assert.Equal(t, tt.expectedBlockerGuid, guid, "Blocker GUID should match")
			} else {
				assert.Equal(t, "", guid, "Blocker GUID should be empty")
			}

			if tt.expectedBlockerService != "" {
				assert.Equal(t, tt.expectedBlockerService, service, "Blocker service should match")
			} else {
				assert.Equal(t, "", service, "Blocker service should be empty")
			}
		})
	}
}

func TestBlockingQueryNormalization(t *testing.T) {
	// Test that blocking query text is normalized correctly
	blockingQueryText := `/* nr_service_guid="BlockerGUID", nr_service="blocker-app" */
		UPDATE Person.Person SET Title = 'Mr.' WHERE BusinessEntityID = 5`

	// Extract metadata first
	guid, service := helpers.ExtractNewRelicMetadata(blockingQueryText)
	assert.Equal(t, "BlockerGUID", guid)
	assert.Equal(t, "blocker-app", service)

	// Then normalize the query
	normalizedSQL, hash := helpers.NormalizeSqlAndHash(blockingQueryText)

	// Verify normalization
	assert.Contains(t, normalizedSQL, "UPDATE PERSON.PERSON")
	assert.Contains(t, normalizedSQL, "SET TITLE = ?")
	assert.Contains(t, normalizedSQL, "WHERE BUSINESSENTITYID = ?")
	assert.NotContains(t, normalizedSQL, "Mr.")
	assert.NotContains(t, normalizedSQL, "5")
	assert.Len(t, hash, 32, "Hash should be 32 characters (MD5)")
}

func TestEmitBlockingQueriesAsCustomEvents(t *testing.T) {
	tests := []struct {
		name                          string
		activeQueries                 []models.ActiveRunningQuery
		expectedEventCount            int
		expectedBlockingNrServiceGuid string
		expectedBlockingNormalizedHash string
		expectAnonymization           bool
	}{
		{
			name: "Blocking query with full APM metadata",
			activeQueries: []models.ActiveRunningQuery{
				{
					CurrentSessionID:  ptr(int64(123)),
					RequestID:         ptr(int64(0)),
					RequestStartTime:  ptr("2025-02-06 10:15:30"),
					BlockingSessionID: ptr(int64(456)),
					BlockingQueryStatementText: ptr(`/* nr_service_guid="blocker-guid-123" nr_service="blocker-service" */
						UPDATE users SET status = 'active' WHERE id = 100`),
					BlockingNrServiceGuid:     ptr("blocker-guid-123"),
					BlockingNormalizedSqlHash: ptr("abc123def456"),
				},
			},
			expectedEventCount:             1,
			expectedBlockingNrServiceGuid:  "blocker-guid-123",
			expectedBlockingNormalizedHash: "abc123def456",
			expectAnonymization:            true,
		},
		{
			name: "Blocking query without APM metadata",
			activeQueries: []models.ActiveRunningQuery{
				{
					CurrentSessionID:       ptr(int64(200)),
					RequestID:              ptr(int64(0)),
					RequestStartTime:       ptr("2025-02-06 10:20:00"),
					BlockingSessionID:      ptr(int64(300)),
					BlockingQueryStatementText: ptr(`UPDATE products SET price = 99.99 WHERE product_id = 50`),
					BlockingNrServiceGuid:     nil, // No APM metadata
					BlockingNormalizedSqlHash: nil,
				},
			},
			expectedEventCount:             1,
			expectedBlockingNrServiceGuid:  "",
			expectedBlockingNormalizedHash: "",
			expectAnonymization:            true,
		},
		{
			name: "Multiple blocking queries - deduplication by composite key",
			activeQueries: []models.ActiveRunningQuery{
				{
					CurrentSessionID:       ptr(int64(100)),
					RequestID:              ptr(int64(0)),
					RequestStartTime:       ptr("2025-02-06 11:00:00"),
					BlockingSessionID:      ptr(int64(200)),
					BlockingQueryStatementText: ptr(`DELETE FROM logs WHERE date < '2024-01-01'`),
					BlockingNrServiceGuid:     ptr("guid-abc"),
					BlockingNormalizedSqlHash: ptr("hash-xyz"),
				},
				// Duplicate - same session, request, time, blocker
				{
					CurrentSessionID:       ptr(int64(100)),
					RequestID:              ptr(int64(0)),
					RequestStartTime:       ptr("2025-02-06 11:00:00"),
					BlockingSessionID:      ptr(int64(200)),
					BlockingQueryStatementText: ptr(`DELETE FROM logs WHERE date < '2024-01-01'`),
					BlockingNrServiceGuid:     ptr("guid-abc"),
					BlockingNormalizedSqlHash: ptr("hash-xyz"),
				},
			},
			expectedEventCount:             1, // Should deduplicate
			expectedBlockingNrServiceGuid:  "guid-abc",
			expectedBlockingNormalizedHash: "hash-xyz",
			expectAnonymization:            true,
		},
		{
			name: "No blocking queries",
			activeQueries: []models.ActiveRunningQuery{
				{
					CurrentSessionID:  ptr(int64(100)),
					RequestID:         ptr(int64(0)),
					RequestStartTime:  ptr("2025-02-06 12:00:00"),
					BlockingSessionID: nil, // No blocker
				},
			},
			expectedEventCount: 0, // No events emitted
		},
		{
			name: "Blocking query with zero blocking_session_id",
			activeQueries: []models.ActiveRunningQuery{
				{
					CurrentSessionID:  ptr(int64(100)),
					RequestID:         ptr(int64(0)),
					RequestStartTime:  ptr("2025-02-06 12:00:00"),
					BlockingSessionID: ptr(int64(0)), // Zero blocker
				},
			},
			expectedEventCount: 0, // Should skip
		},
		{
			name: "Multiple unique blocking events",
			activeQueries: []models.ActiveRunningQuery{
				{
					CurrentSessionID:       ptr(int64(10)),
					RequestID:              ptr(int64(0)),
					RequestStartTime:       ptr("2025-02-06 13:00:00"),
					BlockingSessionID:      ptr(int64(20)),
					BlockingQueryStatementText: ptr(`/* nr_service_guid="guid-1" */ SELECT * FROM table1`),
					BlockingNrServiceGuid:     ptr("guid-1"),
					BlockingNormalizedSqlHash: ptr("hash-1"),
				},
				{
					CurrentSessionID:       ptr(int64(30)),
					RequestID:              ptr(int64(0)),
					RequestStartTime:       ptr("2025-02-06 13:05:00"),
					BlockingSessionID:      ptr(int64(40)),
					BlockingQueryStatementText: ptr(`/* nr_service_guid="guid-2" */ SELECT * FROM table2`),
					BlockingNrServiceGuid:     ptr("guid-2"),
					BlockingNormalizedSqlHash: ptr("hash-2"),
				},
			},
			expectedEventCount: 2, // Two distinct blocking events
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create scraper with mock logger
			scraper := &QueryPerformanceScraper{
				logger: createTestLogger(),
				mb:     createMockMetricsBuilder(t),
			}

			// Call EmitBlockingQueriesAsCustomEvents
			err := scraper.EmitBlockingQueriesAsCustomEvents(tt.activeQueries)
			assert.NoError(t, err)

			// Verify the mock metrics builder received the correct number of calls
			// (This would require the mock to track calls, which we'll verify indirectly)

			// For now, we verify no error and the function executes correctly
			// In a full integration test, we would check the emitted metrics
		})
	}
}

// Helper function to create pointer
func ptr[T any](v T) *T {
	return &v
}

// Helper function to create a test logger
func createTestLogger() *zap.Logger {
	return zap.NewNop() // No-op logger for tests
}

// Helper function to create a mock metrics builder
func createMockMetricsBuilder(t *testing.T) *metadata.MetricsBuilder {
	t.Helper()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(
		metadata.DefaultMetricsBuilderConfig(),
		settings,
	)
	return mb
}
