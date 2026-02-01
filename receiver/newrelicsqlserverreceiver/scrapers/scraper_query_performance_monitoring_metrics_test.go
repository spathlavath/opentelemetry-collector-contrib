package scrapers

import (
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/helpers"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/stretchr/testify/assert"
)

func TestSQLNormalizationIntegration(t *testing.T) {
	tests := []struct {
		name                    string
		inputQueryText          string
		expectedNormalizedSQL   string
		expectedClientName      string
		expectedTransactionName string
		expectedHashNotEmpty    bool
	}{
		{
			name: "Query with New Relic metadata and T-SQL parameters",
			inputQueryText: `/* nr_service=MyApp-SQLServer,nr_txn=WebTransaction/API/customers (GET) */
			SELECT * FROM customers WHERE id = @customerId AND status = @status`,
			expectedNormalizedSQL:   "SELECT * FROM CUSTOMERS WHERE ID = ? AND STATUS = ?",
			expectedClientName:      "MyApp-SQLServer",
			expectedTransactionName: "WebTransaction/API/customers (GET)",
			expectedHashNotEmpty:    true,
		},
		{
			name:                    "Query with only service name",
			inputQueryText:          `/* nr_service=ProductionDB */ SELECT TOP 100 * FROM orders WHERE order_date > '2024-01-01'`,
			expectedNormalizedSQL:   "SELECT TOP ? * FROM ORDERS WHERE ORDER_DATE > ?",
			expectedClientName:      "ProductionDB",
			expectedTransactionName: "",
			expectedHashNotEmpty:    true,
		},
		{
			name:                    "Query without New Relic metadata",
			inputQueryText:          `SELECT * FROM users WHERE age > 18 AND city = 'Seattle'`,
			expectedNormalizedSQL:   "SELECT * FROM USERS WHERE AGE > ? AND CITY = ?",
			expectedClientName:      "",
			expectedTransactionName: "",
			expectedHashNotEmpty:    true,
		},
		{
			name: "Complex query with IN clause and metadata",
			inputQueryText: `/* nr_service=Analytics-Service,nr_txn=WebTransaction/Report/sales */
			SELECT product_id, SUM(quantity) FROM sales
			WHERE product_id IN (@p1, @p2, @p3) AND year = @year
			GROUP BY product_id`,
			expectedNormalizedSQL:   "SELECT PRODUCT_ID, SUM(QUANTITY) FROM SALES WHERE PRODUCT_ID IN (?) AND YEAR = ? GROUP BY PRODUCT_ID",
			expectedClientName:      "Analytics-Service",
			expectedTransactionName: "WebTransaction/Report/sales",
			expectedHashNotEmpty:    true,
		},
		{
			name:                    "Empty query text",
			inputQueryText:          "",
			expectedNormalizedSQL:   "",
			expectedClientName:      "",
			expectedTransactionName: "",
			expectedHashNotEmpty:    false,
		},
		{
			name: "Query with numeric literals",
			inputQueryText: `SELECT * FROM products WHERE price > 100.50 AND stock < 10`,
			expectedNormalizedSQL:   "SELECT * FROM PRODUCTS WHERE PRICE > ? AND STOCK < ?",
			expectedClientName:      "",
			expectedTransactionName: "",
			expectedHashNotEmpty:    true,
		},
		{
			name: "Transaction name with commas and special characters",
			inputQueryText: `/* nr_service=MyApp,nr_txn=WebTransaction/API/orders,createOrder,v2 */
			INSERT INTO orders (customer_id, amount) VALUES (@cid, @amt)`,
			expectedNormalizedSQL:   "INSERT INTO ORDERS (CUSTOMER_ID, AMOUNT) VALUES (?, ?)",
			expectedClientName:      "MyApp",
			expectedTransactionName: "WebTransaction/API/orders,createOrder,v2",
			expectedHashNotEmpty:    true,
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
				clientName, transactionName := helpers.ExtractNewRelicMetadata(*result.QueryText)
				normalizedSQL, sqlHash := helpers.NormalizeSqlAndHash(*result.QueryText)

				if clientName != "" {
					result.ClientName = &clientName
				}
				if transactionName != "" {
					result.TransactionName = &transactionName
				}
				if sqlHash != "" {
					result.NormalisedSqlHash = &sqlHash
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
				assert.NotNil(t, result.ClientName, "ClientName should not be nil")
				assert.Equal(t, tt.expectedClientName, *result.ClientName, "ClientName should match")
			} else {
				assert.Nil(t, result.ClientName, "ClientName should be nil")
			}

			// Verify transaction name
			if tt.expectedTransactionName != "" {
				assert.NotNil(t, result.TransactionName, "TransactionName should not be nil")
				assert.Equal(t, tt.expectedTransactionName, *result.TransactionName, "TransactionName should match")
			} else {
				assert.Nil(t, result.TransactionName, "TransactionName should be nil")
			}

			// Verify hash
			if tt.expectedHashNotEmpty {
				assert.NotNil(t, result.NormalisedSqlHash, "NormalisedSqlHash should not be nil")
				assert.NotEmpty(t, *result.NormalisedSqlHash, "NormalisedSqlHash should not be empty")
				// Verify it's a valid MD5 hash (32 hex characters)
				assert.Len(t, *result.NormalisedSqlHash, 32, "MD5 hash should be 32 characters")
			} else {
				assert.Nil(t, result.NormalisedSqlHash, "NormalisedSqlHash should be nil for empty query")
			}
		})
	}
}

func TestSQLHashConsistency(t *testing.T) {
	// Test that the same logical query produces the same hash regardless of literals
	tests := []struct {
		name     string
		query1   string
		query2   string
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
		name                    string
		inputSQL                string
		expectedService         string
		expectedTransaction     string
	}{
		{
			name:                "Metadata in middle of query",
			inputSQL:            "SELECT /* nr_service=MyApp,nr_txn=Web/Home */ * FROM users",
			expectedService:     "MyApp",
			expectedTransaction: "Web/Home",
		},
		{
			name:                "Metadata with extra spaces",
			inputSQL:            "/*  nr_service=MyApp  ,  nr_txn=Web/API  */  SELECT * FROM orders",
			expectedService:     "MyApp",
			expectedTransaction: "Web/API",
		},
		{
			name:                "Only service, no transaction",
			inputSQL:            "/* nr_service=ProductionDB */ SELECT * FROM logs",
			expectedService:     "ProductionDB",
			expectedTransaction: "",
		},
		{
			name:                "Service with hyphens and underscores",
			inputSQL:            "/* nr_service=My-Production_DB-v2 */ SELECT 1",
			expectedService:     "My-Production_DB-v2",
			expectedTransaction: "",
		},
		{
			name:                "Transaction with parentheses and HTTP methods",
			inputSQL:            "/* nr_service=API,nr_txn=WebTransaction/API/customers/{id} (GET) */ SELECT *",
			expectedService:     "API",
			expectedTransaction: "WebTransaction/API/customers/{id} (GET)",
		},
		{
			name:                "No metadata present",
			inputSQL:            "/* Just a regular comment */ SELECT * FROM tables",
			expectedService:     "",
			expectedTransaction: "",
		},
		{
			name:                "Metadata with trace ID (should stop at trace_id)",
			inputSQL:            "/* nr_service=MyApp,nr_txn=Web/Home,nr_trace_id=abc123 */ SELECT 1",
			expectedService:     "MyApp",
			expectedTransaction: "Web/Home",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, transaction := helpers.ExtractNewRelicMetadata(tt.inputSQL)

			assert.Equal(t, tt.expectedService, service, "Service name should match")
			assert.Equal(t, tt.expectedTransaction, transaction, "Transaction name should match")
		})
	}
}

func TestNormalizationPrivacy(t *testing.T) {
	// Test that normalization removes sensitive data
	tests := []struct {
		name          string
		inputSQL      string
		shouldContain []string
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
		name         string
		tSQLQuery    string
		equivalentJavaQuery string
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
