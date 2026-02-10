// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package commonutils

import (
	"testing"
)

func TestNormalizeSQL(t *testing.T) {
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
			expected: "SELECT * FROM USERS ?WHERE ID = ?",
		},
		{
			name:     "SELECT with multi-line comment",
			input:    "SELECT * FROM users /* this is a\nmulti-line comment */ WHERE id = 1",
			expected: "SELECT * FROM USERS ? WHERE ID = ?",
		},
		{
			name:     "SELECT with hash comment",
			input:    "SELECT * FROM users # this is a comment\nWHERE id = 1",
			expected: "SELECT * FROM USERS ?WHERE ID = ?",
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
			result := NormalizeSQL(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeSQL(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestNormalizeSQLWithCommentBeforeSelect(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Comment before SELECT",
			input:    "/* text */SELECT * FROM users",
			expected: "?SELECT * FROM USERS",
		},
		{
			name:     "Comment with nrServiceGUID before SELECT",
			input:    "/* nrServiceGUID=\"ABC123\" */SELECT * FROM users WHERE id = 5",
			expected: "?SELECT * FROM USERS WHERE ID = ?",
		},
		{
			name:     "Complex query with comment",
			input:    "/* text */SELECT D.DEPARTMENT_NAME, D.DEPARTMENT_ID, COUNT(DISTINCT E.EMPLOYEE_ID) AS CURRENT_EMPLOYEES FROM DEPARTMENTS D WHERE D.DEPARTMENT_NAME LIKE ? GROUP BY D.DEPARTMENT_NAME",
			expected: "?SELECT D.DEPARTMENT_NAME, D.DEPARTMENT_ID, COUNT(DISTINCT E.EMPLOYEE_ID) AS CURRENT_EMPLOYEES FROM DEPARTMENTS D WHERE D.DEPARTMENT_NAME LIKE ? GROUP BY D.DEPARTMENT_NAME",
		},
		{
			name:     "Multiple comments in query",
			input:    "/* comment1 */SELECT * FROM users /* comment2 */ WHERE id = 1 -- inline comment",
			expected: "?SELECT * FROM USERS ? WHERE ID = ? ?",
		},
		{
			name:     "Comment in the middle of query",
			input:    "SELECT * FROM users WHERE /* filtering */ id IN (1, 2, 3)",
			expected: "SELECT * FROM USERS WHERE ? ID IN (?)",
		},
		{
			name:     "Comment with special characters",
			input:    "/* @app_name='MyApp' */SELECT * FROM orders WHERE price > 100",
			expected: "?SELECT * FROM ORDERS WHERE PRICE > ?",
		},
		{
			name:     "Multiple line breaks in comment",
			input:    "/* This is a\n   multi-line\n   comment */SELECT * FROM products",
			expected: "?SELECT * FROM PRODUCTS",
		},
		{
			name:     "Comment followed by bind variable",
			input:    "/* comment */SELECT * FROM users WHERE name = :name AND age = :age",
			expected: "?SELECT * FROM USERS WHERE NAME = ? AND AGE = ?",
		},
		{
			name:     "Hash comment with Oracle query",
			input:    "SELECT * FROM v$session # active sessions only\nWHERE status = 'ACTIVE'",
			expected: "SELECT * FROM V$SESSION ?WHERE STATUS = ?",
		},
		{
			name:     "Single-line comment at end of query",
			input:    "SELECT * FROM employees WHERE dept_id = 10 -- HR department",
			expected: "SELECT * FROM EMPLOYEES WHERE DEPT_ID = ? ?",
		},
		{
			name:     "Comment with nested slashes",
			input:    "/* path: /usr/local/bin */SELECT * FROM config",
			expected: "?SELECT * FROM CONFIG",
		},
		{
			name:     "Mixed comments and literals",
			input:    "SELECT * /* select all */ FROM users WHERE id = 123 -- get user\nAND name = 'John'",
			expected: "SELECT * ? FROM USERS WHERE ID = ? ?AND NAME = ?",
		},
		{
			name:     "Comment with SQL keywords inside",
			input:    "/* SELECT UPDATE DELETE */SELECT column1 FROM table1",
			expected: "?SELECT COLUMN1 FROM TABLE1",
		},
		{
			name:     "Empty comment",
			input:    "/**/SELECT * FROM users",
			expected: "?SELECT * FROM USERS",
		},
		{
			name:     "Comment with quotes inside",
			input:    "/* This is 'quoted' text */SELECT * FROM products WHERE category = 'Electronics'",
			expected: "?SELECT * FROM PRODUCTS WHERE CATEGORY = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeSQL(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeSQL(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestNormalizeSQLAndHash(t *testing.T) {
	// Test that the same normalized query produces the same hash
	query1 := "SELECT * FROM users WHERE id = 123"
	query2 := "SELECT * FROM users WHERE id = 456"
	query3 := "select * from users where id = 789" // lowercase version

	normalized1, hash1 := NormalizeSQLAndHash(query1)
	normalized2, hash2 := NormalizeSQLAndHash(query2)
	normalized3, hash3 := NormalizeSQLAndHash(query3)

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
			expected: "d1c08094cf228a33039e9ee0387ab83c", // MD5 hash of this string
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
		name         string
		input        string
		expectedGUID string
	}{
		{
			name:         "No New Relic metadata",
			input:        "SELECT * FROM employees WHERE id = 1",
			expectedGUID: "",
		},
		{
			name:         "Comment without New Relic metadata",
			input:        "/* This is a regular comment */ SELECT * FROM employees",
			expectedGUID: "",
		},
		{
			name:         "Empty query",
			input:        "",
			expectedGUID: "",
		},
		{
			name:         "Quoted GUID (basic)",
			input:        "/*nrServiceGUID=\"MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI4MzA5MDIxMQ\"*/ SELECT * FROM pets",
			expectedGUID: "MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI4MzA5MDIxMQ",
		},
		{
			name:         "Quoted GUID with spaces in comment",
			input:        "/* nrServiceGUID=\"MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI4MzA5MDIxMQ\" */ SELECT * FROM pets",
			expectedGUID: "MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI4MzA5MDIxMQ",
		},
		{
			name:         "GUID with different base64 value",
			input:        "/* nrServiceGUID=\"ABC123XYZ789==\",other=\"value\" */ SELECT * FROM users",
			expectedGUID: "ABC123XYZ789==",
		},
		{
			name:         "Empty quoted GUID",
			input:        "/* nrServiceGUID=\"\" */ SELECT * FROM users",
			expectedGUID: "",
		},
		{
			name:         "GUID in middle of query",
			input:        "SELECT /* nrServiceGUID=\"MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI4MzA5MDIxMQ\" */ * FROM users",
			expectedGUID: "MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI4MzA5MDIxMQ",
		},
		{
			name:         "Real-world GUID with UPDATE statement",
			input:        "/* nrServiceGUID=\"MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI4MzA5MDIxMQ\" */ UPDATE payments SET status = ?",
			expectedGUID: "MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI4MzA5MDIxMQ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nrApmGUID := ExtractNewRelicMetadata(tt.input)
			if nrApmGUID != tt.expectedGUID {
				t.Errorf("ExtractNewRelicMetadata(%q) = %q; want %q", tt.input, nrApmGUID, tt.expectedGUID)
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
			result := NormalizeSQL(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeSQL(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestPrintNormalizedQueries(t *testing.T) {
	queries := []string{
		"/* text */SELECT D.DEPARTMENT_NAME FROM DEPARTMENTS",
		"/* nrServiceGUID=\"ABC\" */SELECT * FROM users WHERE id = 5",
	}

	for _, query := range queries {
		normalized := NormalizeSQL(query)
		t.Logf("Input:      %s", query)
		t.Logf("Normalized: %s\n", normalized)
	}
}
