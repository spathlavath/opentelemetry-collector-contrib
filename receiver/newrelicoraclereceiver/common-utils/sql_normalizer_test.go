package commonutils

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
			name:     "SELECT with Oracle bind variable",
			input:    "SELECT * FROM users WHERE id = :id",
			expected: "SELECT * FROM USERS WHERE ID = ?",
		},
		{
			name:     "SELECT with IN clause - multiple values",
			input:    "SELECT * FROM users WHERE id IN (1, 2, 3)",
			expected: "SELECT * FROM USERS WHERE ID IN (?)",
		},
		{
			name:     "SELECT with IN clause - bind variables",
			input:    "SELECT * FROM users WHERE id IN (:id1, :id2, :id3)",
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
			name:     "SELECT with hash comment",
			input:    "SELECT * FROM users # this is a comment\nWHERE id = 1",
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
			name:     "Oracle-specific query with multiple bind variables",
			input:    "SELECT employee_id, salary FROM employees WHERE department_id = :dept_id AND hire_date > :hire_date",
			expected: "SELECT EMPLOYEE_ID, SALARY FROM EMPLOYEES WHERE DEPARTMENT_ID = ? AND HIRE_DATE > ?",
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

	// All three should produce the same hash
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
			expected: "bdc5b521b5217084c0cca8b05cbd8e13", // Known MD5 hash of this string
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
		expectedNrTxn     string
	}{
		{
			name:              "Valid New Relic metadata comment",
			input:             "/* nr_service=Oracle-HR-Portal-Java,nr_txn=WebTransaction/SpringController/employees (GET) */ SELECT * FROM employees",
			expectedNrService: "Oracle-HR-Portal-Java",
			expectedNrTxn:     "WebTransaction/SpringController/employees (GET)",
		},
		{
			name:              "Valid New Relic metadata with extra spaces",
			input:             "/*  nr_service=MyService, nr_txn=WebTransaction/Controller/api */ SELECT * FROM users",
			expectedNrService: "MyService",
			expectedNrTxn:     "WebTransaction/Controller/api",
		},
		{
			name:              "No New Relic metadata",
			input:             "SELECT * FROM employees WHERE id = 1",
			expectedNrService: "",
			expectedNrTxn:     "",
		},
		{
			name:              "Comment without New Relic metadata",
			input:             "/* This is a regular comment */ SELECT * FROM employees",
			expectedNrService: "",
			expectedNrTxn:     "",
		},
		{
			name:              "Only nr_service (incomplete)",
			input:             "/* nr_service=MyService */ SELECT * FROM employees",
			expectedNrService: "",
			expectedNrTxn:     "",
		},
		{
			name:              "Complex transaction name with special characters",
			input:             "/* nr_service=Oracle-DB-Service,nr_txn=WebTransaction/Servlet/OrderController/processOrder (POST) */ UPDATE orders SET status = 'processed'",
			expectedNrService: "Oracle-DB-Service",
			expectedNrTxn:     "WebTransaction/Servlet/OrderController/processOrder (POST)",
		},
		{
			name:              "Empty query",
			input:             "",
			expectedNrService: "",
			expectedNrTxn:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nrService, nrTxn := ExtractNewRelicMetadata(tt.input)
			if nrService != tt.expectedNrService {
				t.Errorf("ExtractNewRelicMetadata(%q) nrService = %q; want %q", tt.input, nrService, tt.expectedNrService)
			}
			if nrTxn != tt.expectedNrTxn {
				t.Errorf("ExtractNewRelicMetadata(%q) nrTxn = %q; want %q", tt.input, nrTxn, tt.expectedNrTxn)
			}
		})
	}
}

func TestOracleSpecificNormalization(t *testing.T) {
	// Test Oracle-specific scenarios
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Oracle SYSTIMESTAMP",
			input:    "SELECT SYSTIMESTAMP, user_id FROM sessions WHERE created_at > SYSTIMESTAMP - INTERVAL '1' DAY",
			expected: "SELECT SYSTIMESTAMP, USER_ID FROM SESSIONS WHERE CREATED_AT > SYSTIMESTAMP - INTERVAL ? DAY",
		},
		{
			name:     "Oracle DECODE function with literals",
			input:    "SELECT DECODE(status, 1, 'Active', 2, 'Inactive', 'Unknown') FROM users",
			expected: "SELECT DECODE(STATUS, ?, ?, ?, ?, ?) FROM USERS",
		},
		{
			name:     "Oracle NVL with bind variable",
			input:    "SELECT NVL(salary, 0) FROM employees WHERE dept_id = :dept",
			expected: "SELECT NVL(SALARY, ?) FROM EMPLOYEES WHERE DEPT_ID = ?",
		},
		{
			name:     "Query from V$SQLAREA",
			input:    "SELECT sql_id, sql_text, executions FROM v$sqlarea WHERE parsing_user_id = :user_id AND executions > 100",
			expected: "SELECT SQL_ID, SQL_TEXT, EXECUTIONS FROM V$SQLAREA WHERE PARSING_USER_ID = ? AND EXECUTIONS > ?",
		},
		{
			name:     "Oracle bind variable with numeric suffix",
			input:    "SELECT * FROM employees WHERE dept_id = :dept_id1 OR dept_id = :dept_id2",
			expected: "SELECT * FROM EMPLOYEES WHERE DEPT_ID = ? OR DEPT_ID = ?",
		},
		{
			name:     "Oracle JDBC style placeholder",
			input:    "SELECT * FROM employees WHERE dept_id = ?",
			expected: "SELECT * FROM EMPLOYEES WHERE DEPT_ID = ?",
		},
		{
			name:     "Mixed Oracle bind variables and JDBC placeholders",
			input:    "SELECT * FROM employees WHERE dept_id = :dept AND salary > ?",
			expected: "SELECT * FROM EMPLOYEES WHERE DEPT_ID = ? AND SALARY > ?",
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
