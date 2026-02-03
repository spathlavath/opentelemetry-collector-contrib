package helpers

import (
	"testing"
)

func TestNormalizeSql(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Basic SELECT with numeric literal",
			input:    "SELECT * FROM users WHERE id = 123",
			expected: "SELECT * FROM USERS WHERE ID = ?",
		},
		{
			name:     "SELECT with string literal",
			input:    "SELECT * FROM users WHERE name = 'John Doe'",
			expected: "SELECT * FROM USERS WHERE NAME = ?",
		},
		{
			name:     "SELECT with T-SQL named parameter",
			input:    "SELECT * FROM users WHERE id = @userId",
			expected: "SELECT * FROM USERS WHERE ID = ?",
		},
		{
			name:     "SELECT with multiple T-SQL parameters",
			input:    "SELECT * FROM users WHERE id = @id AND name = @name",
			expected: "SELECT * FROM USERS WHERE ID = ? AND NAME = ?",
		},
		{
			name:     "SELECT with JDBC placeholder",
			input:    "SELECT * FROM users WHERE id = ?",
			expected: "SELECT * FROM USERS WHERE ID = ?",
		},
		{
			name:     "SELECT with IN clause - multiple values",
			input:    "SELECT * FROM users WHERE id IN (1, 2, 3)",
			expected: "SELECT * FROM USERS WHERE ID IN (?)",
		},
		{
			name:     "SELECT with IN clause - T-SQL parameters",
			input:    "SELECT * FROM users WHERE id IN (@id1, @id2, @id3)",
			expected: "SELECT * FROM USERS WHERE ID IN (?)",
		},
		{
			name:     "SELECT with single-line comment",
			input:    "SELECT * FROM users -- this is a comment\nWHERE id = 1",
			expected: "SELECT * FROM USERS WHERE ID = ?",
		},
		{
			name:     "SELECT with multi-line comment",
			input:    "SELECT * FROM users /* this is a\nmulti-line comment */ WHERE id = 1",
			expected: "SELECT * FROM USERS WHERE ID = ?",
		},
		{
			name:     "Complex query with multiple literals and whitespace",
			input:    "SELECT   *  FROM   users WHERE   id = 123   AND   name = 'test'   ",
			expected: "SELECT * FROM USERS WHERE ID = ? AND NAME = ?",
		},
		{
			name:     "Query with scientific notation",
			input:    "SELECT * FROM users WHERE salary > 1.5E6",
			expected: "SELECT * FROM USERS WHERE SALARY > ?",
		},
		{
			name:     "Query with escaped single quote",
			input:    "SELECT * FROM users WHERE name = 'O''Brien'",
			expected: "SELECT * FROM USERS WHERE NAME = ?",
		},
		{
			name:     "Empty query",
			input:    "",
			expected: "",
		},
		{
			name:     "T-SQL stored procedure call with parameters",
			input:    "EXEC GetUserDetails @userId = 123, @includeHistory = 'true'",
			expected: "EXEC GETUSERDETAILS ? = ?, ? = ?",
		},
		{
			name:     "UPDATE with T-SQL parameters",
			input:    "UPDATE users SET name = @name, age = @age WHERE id = @id",
			expected: "UPDATE USERS SET NAME = ?, AGE = ? WHERE ID = ?",
		},
		{
			name:     "INSERT with multiple values",
			input:    "INSERT INTO users (name, age) VALUES ('John', 25), ('Jane', 30)",
			expected: "INSERT INTO USERS (NAME, AGE) VALUES (?, ?), (?, ?)",
		},
		{
			name:     "Query with negative numbers",
			input:    "SELECT * FROM transactions WHERE amount < -100",
			expected: "SELECT * FROM TRANSACTIONS WHERE AMOUNT < ?",
		},
		{
			name:     "Query with decimal numbers",
			input:    "SELECT * FROM products WHERE price = 99.99",
			expected: "SELECT * FROM PRODUCTS WHERE PRICE = ?",
		},
		{
			name:     "T-SQL query with numbered parameters",
			input:    "SELECT * FROM users WHERE id = @1 AND status = @2",
			expected: "SELECT * FROM USERS WHERE ID = ? AND STATUS = ?",
		},
		{
			name:     "Query with table names containing underscores",
			input:    "SELECT * FROM user_details WHERE user_id = 123",
			expected: "SELECT * FROM USER_DETAILS WHERE USER_ID = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeSql(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeSql(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestNormalizeSqlAndHash(t *testing.T) {
	// Test that the same normalized query produces the same hash
	query1 := "SELECT * FROM users WHERE id = 123"
	query2 := "SELECT * FROM users WHERE id = 456"
	query3 := "select * from users where id = 789" // lowercase version

	normalized1, hash1 := NormalizeSqlAndHash(query1)
	normalized2, hash2 := NormalizeSqlAndHash(query2)
	normalized3, hash3 := NormalizeSqlAndHash(query3)

	// All three queries should normalize to the same SQL
	expectedNormalized := "SELECT * FROM USERS WHERE ID = ?"
	if normalized1 != expectedNormalized {
		t.Errorf("normalized1 = %q; want %q", normalized1, expectedNormalized)
	}
	if normalized2 != expectedNormalized {
		t.Errorf("normalized2 = %q; want %q", normalized2, expectedNormalized)
	}
	if normalized3 != expectedNormalized {
		t.Errorf("normalized3 = %q; want %q", normalized3, expectedNormalized)
	}

	// All three queries should produce the same hash
	if hash1 != hash2 {
		t.Errorf("hash1 (%s) != hash2 (%s)", hash1, hash2)
	}
	if hash1 != hash3 {
		t.Errorf("hash1 (%s) != hash3 (%s)", hash1, hash3)
	}

	// Hash should be 32 characters (MD5 hex encoding)
	if len(hash1) != 32 {
		t.Errorf("hash length = %d; want 32", len(hash1))
	}
}

func TestGenerateMD5Hash(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Known MD5 hash",
			input:    "SELECT * FROM USERS WHERE ID = ?",
			expected: "d1c08094cf228a33039e9ee0387ab83c", // Known MD5 hash of this string
		},
		{
			name:     "Empty string",
			input:    "",
			expected: "d41d8cd98f00b204e9800998ecf8427e", // MD5 of empty string
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GenerateMD5Hash(tt.input)
			if result != tt.expected {
				t.Errorf("GenerateMD5Hash(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestExtractNewRelicMetadata(t *testing.T) {
	tests := []struct {
		name              string
		input             string
		expectedNrService string
	}{
		// Quoted value format tests (REQUIRED FORMAT)
		{
			name:              "Only nr_service",
			input:             `/* nr_service="MyApp-SQLServer" */ SELECT * FROM employees`,
			expectedNrService: "MyApp-SQLServer",
		},
		{
			name:              "nr_service with extra spaces",
			input:             `/*  nr_service="MyService" */ SELECT * FROM users`,
			expectedNrService: "MyService",
		},
		{
			name:              "Real APM log example - with service name",
			input:             `/* nr_service="MyApp-SQLServer" */ SELECT c.customer_id, c.name FROM customers c WHERE c.status = @status`,
			expectedNrService: "MyApp-SQLServer",
		},
		{
			name:              "No New Relic metadata",
			input:             "SELECT * FROM employees WHERE id = 1",
			expectedNrService: "",
		},
		{
			name:              "Comment without New Relic metadata",
			input:             "/* This is a regular comment */ SELECT * FROM employees",
			expectedNrService: "",
		},
		{
			name:              "Service with commas (requires quotes)",
			input:             `/* nr_service="MyApp, Production, US-East" */ SELECT * FROM users`,
			expectedNrService: "MyApp, Production, US-East",
		},
		{
			name:              "Service with special characters",
			input:             `/* nr_service="MyApp-Backend (Production)" */ SELECT * FROM customers`,
			expectedNrService: "MyApp-Backend (Production)",
		},
		{
			name:              "Old format with nr_txn should only extract nr_service",
			input:             `/* nr_service="MyApp",nr_txn="WebTransaction/API/users" */ SELECT * FROM users`,
			expectedNrService: "MyApp",
		},
		{
			name:              "Service name with hyphens and numbers",
			input:             `/* nr_service="MyApp-2024-prod",nr_txn="WebTransaction/API/test" */ SELECT * FROM test`,
			expectedNrService: "MyApp-2024-prod",
			expectedNrTxn:     "WebTransaction/API/test",
		},
		{
			name:              "Transaction with DELETE method",
			input:             `/* nr_service="MyApp",nr_txn="WebTransaction/API/customers/{id} (DELETE)" */ DELETE FROM customers WHERE id = @id`,
			expectedNrService: "MyApp",
			expectedNrTxn:     "WebTransaction/API/customers/{id} (DELETE)",
		},
		{
			name:              "Service with multiple hyphens and underscores",
			input:             `/* nr_service="My_App-Service-2024",nr_txn="WebTransaction/API/endpoint" */ SELECT * FROM data`,
			expectedNrService: "My_App-Service-2024",
			expectedNrTxn:     "WebTransaction/API/endpoint",
		},
		{
			name:              "Transaction name with special characters",
			input:             `/* nr_service="MyApp",nr_txn="WebTransaction/API/search?query=test&page=1" */ SELECT * FROM results`,
			expectedNrService: "MyApp",
			expectedNrTxn:     "WebTransaction/API/search?query=test&page=1",
		},
		{
			name:              "Complex real-world example",
			input:             `/* nr_service="ECommerce-API-Prod",nr_txn="WebTransaction/SpringMVC/OrderController/processCheckout (POST)" */ EXEC ProcessOrder @orderId = @p1, @userId = @p2`,
			expectedNrService: "ECommerce-API-Prod",
			expectedNrTxn:     "WebTransaction/SpringMVC/OrderController/processCheckout (POST)",
		},
		// Additional quoted format tests
		{
			name:              "Service with background job notation",
			input:             `/* nr_service="MyApp-SQLServer, Background Job" */ SELECT * FROM employees`,
			expectedNrService: "MyApp-SQLServer, Background Job",
		},
		{
			name:              "Service with company name format",
			input:             `/* nr_service="MyApp, Inc" */ SELECT * FROM orders`,
			expectedNrService: "MyApp, Inc",
		},
		{
			name:              "Spaces around equals signs",
			input:             `/* nr_service = "MyApp, Background" */ SELECT * FROM test`,
			expectedNrService: "MyApp, Background",
		},
		{
			name:              "Service with multiple location segments",
			input:             `/* nr_service="MyApp, Inc, USA, Production" */ SELECT * FROM data`,
			expectedNrService: "MyApp, Inc, USA, Production",
		},
		{
			name:              "Production service with regions",
			input:             `/* nr_service="ECommerce-API, Production, US-East" */ EXEC ProcessOrder @orderId = @p1`,
			expectedNrService: "ECommerce-API, Production, US-East",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nrService := ExtractNewRelicMetadata(tt.input)
			if nrService != tt.expectedNrService {
				t.Errorf("nrService = %q; want %q", nrService, tt.expectedNrService)
			}
		})
	}
}

func TestNormalizeSqlEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Query with @ in email string (not a parameter)",
			input:    "SELECT * FROM users WHERE email = 'test@example.com'",
			expected: "SELECT * FROM USERS WHERE EMAIL = ?",
		},
		{
			name:     "Query with multiple IN clauses",
			input:    "SELECT * FROM orders WHERE status IN ('pending', 'processing') AND priority IN (1, 2, 3)",
			expected: "SELECT * FROM ORDERS WHERE STATUS IN (?) AND PRIORITY IN (?)",
		},
		{
			name:     "Query with nested parentheses",
			input:    "SELECT * FROM users WHERE (age > 18 AND (status = 'active' OR status = 'pending'))",
			expected: "SELECT * FROM USERS WHERE (AGE > ? AND (STATUS = ? OR STATUS = ?))",
		},
		{
			name:     "Query with JOIN and multiple conditions",
			input:    "SELECT u.* FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = @status AND o.amount > 100",
			expected: "SELECT U.* FROM USERS U JOIN ORDERS O ON U.ID = O.USER_ID WHERE U.STATUS = ? AND O.AMOUNT > ?",
		},
		{
			name:     "Query with CASE statement",
			input:    "SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users",
			expected: "SELECT CASE WHEN AGE > ? THEN ? ELSE ? END FROM USERS",
		},
		{
			name:     "Query with subquery",
			input:    "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 1000)",
			expected: "SELECT * FROM USERS WHERE ID IN (SELECT USER_ID FROM ORDERS WHERE AMOUNT > ?)",
		},
		{
			name:     "Query with BETWEEN clause",
			input:    "SELECT * FROM products WHERE price BETWEEN 10.00 AND 100.00",
			expected: "SELECT * FROM PRODUCTS WHERE PRICE BETWEEN ? AND ?",
		},
		{
			name:     "Query with LIKE pattern",
			input:    "SELECT * FROM users WHERE name LIKE 'John%'",
			expected: "SELECT * FROM USERS WHERE NAME LIKE ?",
		},
		{
			name:     "Query with aggregate functions",
			input:    "SELECT COUNT(*), AVG(salary) FROM employees WHERE department_id = @deptId",
			expected: "SELECT COUNT(*), AVG(SALARY) FROM EMPLOYEES WHERE DEPARTMENT_ID = ?",
		},
		{
			name:     "Query with window functions",
			input:    "SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) FROM users WHERE status = 'active'",
			expected: "SELECT ID, ROW_NUMBER() OVER (ORDER BY CREATED_AT) FROM USERS WHERE STATUS = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeSql(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeSql(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestCrossLanguageCompatibility(t *testing.T) {
	// These tests ensure that our SQL Server implementation produces the same
	// normalized SQL as the Oracle implementation for common queries
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple SELECT - should match Oracle normalization",
			input:    "SELECT * FROM USERS WHERE ID = 123",
			expected: "SELECT * FROM USERS WHERE ID = ?",
		},
		{
			name:     "SELECT with IN clause - should match Oracle normalization",
			input:    "SELECT * FROM USERS WHERE ID IN (1, 2, 3)",
			expected: "SELECT * FROM USERS WHERE ID IN (?)",
		},
		{
			name:     "Complex query - should match Oracle normalization",
			input:    "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' AND o.total > 100",
			expected: "SELECT U.NAME, O.TOTAL FROM USERS U JOIN ORDERS O ON U.ID = O.USER_ID WHERE O.STATUS = ? AND O.TOTAL > ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeSql(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeSql(%q) = %q; want %q", tt.input, result, tt.expected)
			}

			// Verify MD5 hash is generated correctly
			hash := GenerateMD5Hash(result)
			if len(hash) != 32 {
				t.Errorf("MD5 hash length = %d; want 32", len(hash))
			}
		})
	}
}
