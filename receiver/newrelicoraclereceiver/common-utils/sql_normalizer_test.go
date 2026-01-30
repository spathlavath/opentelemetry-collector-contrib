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
			name:              "Only nr_service",
			input:             "/* nr_service=Oracle-HR-Portal-Java */ SELECT * FROM employees",
			expectedNrService: "Oracle-HR-Portal-Java",
			expectedNrTxn:     "",
		},
		{
			name:              "Both nr_service and nr_txn",
			input:             "/* nr_service=Oracle-HR-Portal-Java,nr_txn=WebTransaction/SpringController/employees (GET) */ SELECT * FROM employees",
			expectedNrService: "Oracle-HR-Portal-Java",
			expectedNrTxn:     "WebTransaction/SpringController/employees (GET)",
		},
		{
			name:              "Both with extra spaces",
			input:             "/*  nr_service=MyService, nr_txn=WebTransaction/Controller/api */ SELECT * FROM users",
			expectedNrService: "MyService",
			expectedNrTxn:     "WebTransaction/Controller/api",
		},
		{
			name:              "Real APM log example - employees GET with transaction",
			input:             "/* nr_service=Oracle-HR-Portal-Java,nr_txn=WebTransaction/SpringController/employees (GET) */ SELECT e.EMPLOYEE_ID, e.FIRST_NAME FROM EMPLOYEES e WHERE e.SALARY >= ? ORDER BY e.SALARY DESC",
			expectedNrService: "Oracle-HR-Portal-Java",
			expectedNrTxn:     "WebTransaction/SpringController/employees (GET)",
		},
		{
			name:              "Real APM log example - employees history GET with path variable",
			input:             "/* nr_service=Oracle-HR-Portal-Java,nr_txn=WebTransaction/SpringController/employees/{id}/history (GET) */ SELECT jh.* FROM JOB_HISTORY jh WHERE jh.EMPLOYEE_ID = ?",
			expectedNrService: "Oracle-HR-Portal-Java",
			expectedNrTxn:     "WebTransaction/SpringController/employees/{id}/history (GET)",
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
			name:              "Only service with additional comment text",
			input:             "/* nr_service=MyService, some other text */ SELECT * FROM employees",
			expectedNrService: "MyService",
			expectedNrTxn:     "",
		},
		{
			name:              "Transaction with PUT method",
			input:             "/* nr_service=Oracle-HR-Portal-Java,nr_txn=WebTransaction/SpringController/employees/{id} (PUT) */ UPDATE employees SET salary = ? WHERE id = ?",
			expectedNrService: "Oracle-HR-Portal-Java",
			expectedNrTxn:     "WebTransaction/SpringController/employees/{id} (PUT)",
		},
		{
			name:              "Transaction with DELETE method",
			input:             "/* nr_service=Oracle-HR-Portal-Java,nr_txn=WebTransaction/SpringController/employees/{id} (DELETE) */ DELETE FROM employees WHERE id = ?",
			expectedNrService: "Oracle-HR-Portal-Java",
			expectedNrTxn:     "WebTransaction/SpringController/employees/{id} (DELETE)",
		},
		{
			name:              "Empty query",
			input:             "",
			expectedNrService: "",
			expectedNrTxn:     "",
		},
		// Comma handling test cases
		{
			name:              "Service name with single comma (no spaces)",
			input:             "/* nr_service=MyApp,Production,nr_txn=WebTransaction/API/users */ SELECT * FROM users",
			expectedNrService: "MyApp,Production",
			expectedNrTxn:     "WebTransaction/API/users",
		},
		{
			name:              "Service name with multiple commas (no spaces)",
			input:             "/* nr_service=App,Prod,US-East,nr_txn=WebTransaction/API */ SELECT * FROM users",
			expectedNrService: "App,Prod,US-East",
			expectedNrTxn:     "WebTransaction/API",
		},
		{
			name:              "Transaction name with single comma",
			input:             "/* nr_service=MyApp,nr_txn=WebTransaction/API/users,v2 */ SELECT * FROM users",
			expectedNrService: "MyApp",
			expectedNrTxn:     "WebTransaction/API/users,v2",
		},
		{
			name:              "Transaction name with multiple commas",
			input:             "/* nr_service=MyApp,nr_txn=WebTransaction/API/users,v2,beta */ SELECT * FROM users",
			expectedNrService: "MyApp",
			expectedNrTxn:     "WebTransaction/API/users,v2,beta",
		},
		{
			name:              "Both service and transaction with commas",
			input:             "/* nr_service=MyApp,Production,nr_txn=WebTransaction/API/users,v2 */ SELECT * FROM users",
			expectedNrService: "MyApp,Production",
			expectedNrTxn:     "WebTransaction/API/users,v2",
		},
		{
			name:              "Service with commas only (no transaction)",
			input:             "/* nr_service=MyApp,Production,US-East */ SELECT * FROM users",
			expectedNrService: "MyApp,Production,US-East",
			expectedNrTxn:     "",
		},
		{
			name:              "Transaction with commas and spaces in parentheses",
			input:             "/* nr_service=MyApp,nr_txn=WebTransaction/SpringController/employees,v2 (GET) */ SELECT * FROM employees",
			expectedNrService: "MyApp",
			expectedNrTxn:     "WebTransaction/SpringController/employees,v2 (GET)",
		},
		{
			name:              "Transaction with commas and path variables",
			input:             "/* nr_service=MyApp,nr_txn=WebTransaction/API/users/{id}/history,v2 (GET) */ SELECT * FROM history",
			expectedNrService: "MyApp",
			expectedNrTxn:     "WebTransaction/API/users/{id}/history,v2 (GET)",
		},
		{
			name:              "With nr_trace_id - service and txn both have commas",
			input:             "/* nr_service=MyApp,Production,nr_txn=WebTransaction/API/users,v2,nr_trace_id=abc123def */ SELECT * FROM users",
			expectedNrService: "MyApp,Production",
			expectedNrTxn:     "WebTransaction/API/users,v2",
		},
		{
			name:              "With nr_trace_id - only service has comma",
			input:             "/* nr_service=MyApp,Production,nr_txn=WebTransaction/API,nr_trace_id=abc123 */ SELECT * FROM users",
			expectedNrService: "MyApp,Production",
			expectedNrTxn:     "WebTransaction/API",
		},
		{
			name:              "With nr_trace_id - only txn has comma",
			input:             "/* nr_service=MyApp,nr_txn=WebTransaction/API/users,v2,nr_trace_id=abc123 */ SELECT * FROM users",
			expectedNrService: "MyApp",
			expectedNrTxn:     "WebTransaction/API/users,v2",
		},
		{
			name:              "Complex service name with hyphens and commas",
			input:             "/* nr_service=Oracle-HR-Portal-Java,Production-US,nr_txn=WebTransaction/API */ SELECT * FROM employees",
			expectedNrService: "Oracle-HR-Portal-Java,Production-US",
			expectedNrTxn:     "WebTransaction/API",
		},
		{
			name:              "Complex transaction with slashes, commas, and method",
			input:             "/* nr_service=MyApp,nr_txn=WebTransaction/SpringController/api/v2/users/{id},beta (POST) */ INSERT INTO users VALUES (?)",
			expectedNrService: "MyApp",
			expectedNrTxn:     "WebTransaction/SpringController/api/v2/users/{id},beta (POST)",
		},
		{
			name:              "Service with comma at boundary of nr_txn",
			input:             "/* nr_service=App,Prod,nr_txn=WebTransaction */ SELECT * FROM users",
			expectedNrService: "App,Prod",
			expectedNrTxn:     "WebTransaction",
		},
		{
			name:              "Service with underscores and commas",
			input:             "/* nr_service=My_App,Prod_Env,nr_txn=WebTransaction/API */ SELECT * FROM users",
			expectedNrService: "My_App,Prod_Env",
			expectedNrTxn:     "WebTransaction/API",
		},
		{
			name:              "Transaction ending with comma before closing comment",
			input:             "/* nr_service=MyApp,nr_txn=WebTransaction/API/users,v2, */ SELECT * FROM users",
			expectedNrService: "MyApp",
			expectedNrTxn:     "WebTransaction/API/users,v2",
		},
		{
			name:              "Service with numbers and commas",
			input:             "/* nr_service=MyApp123,Production456,nr_txn=WebTransaction/API */ SELECT * FROM users",
			expectedNrService: "MyApp123,Production456",
			expectedNrTxn:     "WebTransaction/API",
		},
		{
			name:              "Transaction with numbers and commas",
			input:             "/* nr_service=MyApp,nr_txn=WebTransaction/API/v2/users,beta1 */ SELECT * FROM users",
			expectedNrService: "MyApp",
			expectedNrTxn:     "WebTransaction/API/v2/users,beta1",
		},
		{
			name:              "Real-world example: multi-region service with versioned API",
			input:             "/* nr_service=PaymentService,US-EAST-1,Production,nr_txn=WebTransaction/REST/api/v2/payments/{id}/confirm,beta (POST) */ UPDATE payments SET status = ? WHERE id = ?",
			expectedNrService: "PaymentService,US-EAST-1,Production",
			expectedNrTxn:     "WebTransaction/REST/api/v2/payments/{id}/confirm,beta (POST)",
		},
		{
			name:              "Service with dots and commas",
			input:             "/* nr_service=com.example.MyApp,Production,nr_txn=WebTransaction/API */ SELECT * FROM users",
			expectedNrService: "com.example.MyApp,Production",
			expectedNrTxn:     "WebTransaction/API",
		},
		{
			name:              "All three fields with commas in values",
			input:             "/* nr_service=App,Env,Region,nr_txn=Web/API,v2,nr_trace_id=trace,123,abc */ SELECT * FROM users",
			expectedNrService: "App,Env,Region",
			expectedNrTxn:     "Web/API,v2",
		},
		// Field order test cases - fields can appear in any order
		{
			name:              "Reverse order: nr_txn before nr_service",
			input:             "/* nr_txn=WebTransaction/API/users,nr_service=MyApp */ SELECT * FROM users",
			expectedNrService: "MyApp",
			expectedNrTxn:     "WebTransaction/API/users",
		},
		{
			name:              "Reverse order with commas in both",
			input:             "/* nr_txn=WebTransaction/API/users,v2,nr_service=MyApp,Production */ SELECT * FROM users",
			expectedNrService: "MyApp,Production",
			expectedNrTxn:     "WebTransaction/API/users,v2",
		},
		{
			name:              "Only nr_txn (no service)",
			input:             "/* nr_txn=WebTransaction/API/users */ SELECT * FROM users",
			expectedNrService: "",
			expectedNrTxn:     "WebTransaction/API/users",
		},
		{
			name:              "Only nr_txn with commas (no service)",
			input:             "/* nr_txn=WebTransaction/API/users,v2,beta */ SELECT * FROM users",
			expectedNrService: "",
			expectedNrTxn:     "WebTransaction/API/users,v2,beta",
		},
		{
			name:              "Reverse order with nr_trace_id in middle",
			input:             "/* nr_txn=WebTransaction/API,nr_trace_id=abc123,nr_service=MyApp */ SELECT * FROM users",
			expectedNrService: "MyApp",
			expectedNrTxn:     "WebTransaction/API",
		},
		{
			name:              "Reverse order: nr_trace_id first",
			input:             "/* nr_trace_id=abc123,nr_txn=WebTransaction/API/users,nr_service=MyApp */ SELECT * FROM users",
			expectedNrService: "MyApp",
			expectedNrTxn:     "WebTransaction/API/users",
		},
		{
			name:              "Service in middle position",
			input:             "/* nr_txn=WebTransaction/API,nr_service=MyApp,Production,nr_trace_id=abc123 */ SELECT * FROM users",
			expectedNrService: "MyApp,Production",
			expectedNrTxn:     "WebTransaction/API",
		},
		{
			name:              "Txn in middle position with commas",
			input:             "/* nr_service=MyApp,nr_txn=WebTransaction/API/users,v2,nr_trace_id=abc123 */ SELECT * FROM users",
			expectedNrService: "MyApp",
			expectedNrTxn:     "WebTransaction/API/users,v2",
		},
		{
			name:              "Reverse order: complex real-world example",
			input:             "/* nr_txn=WebTransaction/REST/api/v2/payments/{id}/confirm,beta (POST),nr_service=PaymentService,US-EAST-1,Production */ UPDATE payments SET status = ? WHERE id = ?",
			expectedNrService: "PaymentService,US-EAST-1,Production",
			expectedNrTxn:     "WebTransaction/REST/api/v2/payments/{id}/confirm,beta (POST)",
		},
		{
			name:              "All three fields in reverse order with commas",
			input:             "/* nr_trace_id=trace123,nr_txn=Web/API,v2,beta,nr_service=App,Prod,Region */ SELECT * FROM users",
			expectedNrService: "App,Prod,Region",
			expectedNrTxn:     "Web/API,v2,beta",
		},
		{
			name:              "Transaction first with spaces and commas",
			input:             "/* nr_txn=WebTransaction/SpringController/employees,v2 (GET),nr_service=MyApp,Production */ SELECT * FROM employees",
			expectedNrService: "MyApp,Production",
			expectedNrTxn:     "WebTransaction/SpringController/employees,v2 (GET)",
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
