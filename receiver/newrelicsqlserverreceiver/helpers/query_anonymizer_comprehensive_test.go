package helpers

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnonymizeQueryTextComprehensive(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		shouldContain    []string
		shouldNotContain []string
	}{
		{
			name:             "Query with string literals",
			input:            "SELECT * FROM users WHERE name = 'John Doe' AND city = 'Seattle'",
			shouldContain:    []string{"SELECT", "FROM", "users", "WHERE", "name", "=", "?", "AND", "city"},
			shouldNotContain: []string{"John Doe", "Seattle"},
		},
		{
			name:             "Query with numeric literals",
			input:            "SELECT * FROM orders WHERE amount > 1000 AND quantity < 50",
			shouldContain:    []string{"SELECT", "FROM", "orders", "WHERE", "amount", ">", "?"},
			shouldNotContain: []string{"1000", "50"},
		},
		{
			name:             "Query with mixed literals",
			input:            "INSERT INTO products (name, price, stock) VALUES ('iPhone', 999.99, 100)",
			shouldContain:    []string{"INSERT", "INTO", "products", "VALUES", "(", "?", ",", ")"},
			shouldNotContain: []string{"iPhone", "999.99", "100"},
		},
		{
			name:             "Query with IN clause",
			input:            "SELECT * FROM items WHERE id IN (1, 2, 3, 4, 5)",
			shouldContain:    []string{"SELECT", "FROM", "items", "WHERE", "id", "IN"},
			shouldNotContain: []string{},
		},
		{
			name:             "Query with comments",
			input:            "-- This is a comment\nSELECT * FROM /* inline comment */ users WHERE id = 123",
			shouldContain:    []string{"SELECT", "FROM", "users", "WHERE", "id", "=", "?"},
			shouldNotContain: []string{"123"},
		},
		{
			name:             "Empty query",
			input:            "",
			shouldContain:    []string{},
			shouldNotContain: []string{},
		},
		{
			name:             "Query with special characters",
			input:            "SELECT * FROM [dbo].[Users] WHERE [Email] = 'test@example.com'",
			shouldContain:    []string{"SELECT", "FROM", "Users", "WHERE"},
			shouldNotContain: []string{"test@example.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AnonymizeQueryText(tt.input)

			for _, expected := range tt.shouldContain {
				assert.Contains(t, result, expected,
					"Anonymized query should contain '%s'", expected)
			}

			for _, forbidden := range tt.shouldNotContain {
				assert.NotContains(t, result, forbidden,
					"Anonymized query should NOT contain sensitive data '%s'", forbidden)
			}

			// Verify result is not empty unless input was empty
			if tt.input != "" {
				assert.NotEmpty(t, result, "Anonymized query should not be empty")
			}
		})
	}
}

func TestAnonymizeExecutionPlanXMLComprehensive(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		shouldContain    []string
		shouldNotContain []string
	}{
		{
			name: "XML with ParameterList",
			input: `<ShowPlanXML>
				<ParameterList>
					<ColumnReference Column="@name" ParameterDataType="varchar(100)" ParameterCompiledValue="'John Doe'" />
					<ColumnReference Column="@age" ParameterDataType="int" ParameterCompiledValue="30" />
				</ParameterList>
			</ShowPlanXML>`,
			shouldContain:    []string{"<ShowPlanXML>", "<ParameterList>", "@name", "@age"},
			shouldNotContain: []string{"John Doe", "30"},
		},
		{
			name: "XML with QueryPlan",
			input: `<ShowPlanXML>
				<QueryPlan>
					<RelOp LogicalOp="Table Scan" PhysicalOp="Table Scan">
						<Object Database="[MyDB]" Schema="[dbo]" Table="[Users]" />
					</RelOp>
				</QueryPlan>
			</ShowPlanXML>`,
			shouldContain:    []string{"<ShowPlanXML>", "<QueryPlan>", "Table Scan", "Users"},
			shouldNotContain: []string{},
		},
		{
			name: "XML with StatementText",
			input: `<ShowPlanXML>
				<Statements>
					<StmtSimple StatementText="SELECT * FROM users WHERE name = 'Alice'" />
				</Statements>
			</ShowPlanXML>`,
			shouldContain:    []string{"<ShowPlanXML>", "<Statements>", "SELECT", "FROM", "users"},
			shouldNotContain: []string{"Alice"},
		},
		{
			name:             "Empty XML",
			input:            "",
			shouldContain:    []string{},
			shouldNotContain: []string{},
		},
		{
			name:             "Invalid XML",
			input:            "Not valid XML at all",
			shouldContain:    []string{},
			shouldNotContain: []string{},
		},
		{
			name: "XML with SET options",
			input: `<ShowPlanXML>
				<BatchSequence>
					<Batch>
						<Statements>
							<StmtSimple StatementText="SET QUOTED_IDENTIFIER ON" />
						</Statements>
					</Batch>
				</BatchSequence>
			</ShowPlanXML>`,
			shouldContain:    []string{"<ShowPlanXML>", "SET"},
			shouldNotContain: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AnonymizeExecutionPlanXML(tt.input)

			// For non-empty input, result should have some content
			if tt.input != "" && !strings.Contains(tt.input, "Not valid") {
				assert.NotEmpty(t, result, "Anonymized XML should not be empty for valid input")
			}

			for _, expected := range tt.shouldContain {
				assert.Contains(t, result, expected,
					"Anonymized XML should contain '%s'", expected)
			}

			for _, forbidden := range tt.shouldNotContain {
				assert.NotContains(t, result, forbidden,
					"Anonymized XML should NOT contain sensitive data '%s'", forbidden)
			}
		})
	}
}

func TestSafeAnonymizeQueryTextComprehensive(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Valid query",
			input:    "SELECT * FROM users WHERE id = 123",
			expected: "", // Should be anonymized successfully (not empty, but transformed)
		},
		{
			name:     "Empty query",
			input:    "",
			expected: "",
		},
		{
			name:     "Very long query",
			input:    strings.Repeat("SELECT * FROM users WHERE id = 1 AND ", 1000),
			expected: "", // Should handle long queries
		},
		{
			name:     "Query with special characters",
			input:    "SELECT * FROM users WHERE name = 'O''Brien' AND age > 30",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SafeAnonymizeQueryText(&tt.input)

			// Should never panic
			assert.NotEmpty(t, result)

			// For non-empty input, should produce some output
			if tt.input != "" {
				assert.NotEmpty(t, result, "SafeAnonymizeQueryText should produce output for non-empty input")
			}
		})
	}
}

// TODO: TestIsQueryTextSafeComprehensive - IsQueryTextSafe function not implemented yet
// func TestIsQueryTextSafeComprehensive(t *testing.T) {
// 	tests := []struct {
// 		name     string
// 		input    string
// 		expected bool
// 	}{
// 		{
// 			name:     "Safe query - SELECT",
// 			input:    "SELECT * FROM users",
// 			expected: true,
// 		},
// 		{
// 			name:     "Safe query - INSERT",
// 			input:    "INSERT INTO users VALUES (1, 'test')",
// 			expected: true,
// 		},
// 		{
// 			name:     "Safe query - UPDATE",
// 			input:    "UPDATE users SET name = 'test' WHERE id = 1",
// 			expected: true,
// 		},
// 		{
// 			name:     "Safe query - DELETE",
// 			input:    "DELETE FROM users WHERE id = 1",
// 			expected: true,
// 		},
// 		{
// 			name:     "Empty query",
// 			input:    "",
// 			expected: true,
// 		},
// 		{
// 			name:     "Short query",
// 			input:    "SELECT 1",
// 			expected: true,
// 		},
// 		{
// 			name:     "Long but safe query",
// 			input:    strings.Repeat("SELECT column FROM table WHERE id = 1 AND ", 100),
// 			expected: true,
// 		},
// 		{
// 			name:     "Query with common keywords",
// 			input:    "SELECT name, email FROM users WHERE age > 18 ORDER BY name",
// 			expected: true,
// 		},
// 	}
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			result := IsQueryTextSafe(tt.input)
// 			assert.Equal(t, tt.expected, result,
// 				"IsQueryTextSafe should return %v for input: %s", tt.expected, tt.input)
// 		})
// 	}
// }

func TestIsXMLContentSafeComprehensive(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "Safe XML - ShowPlanXML",
			input:    "<ShowPlanXML><QueryPlan></QueryPlan></ShowPlanXML>",
			expected: true,
		},
		{
			name:     "Empty XML",
			input:    "",
			expected: true,
		},
		{
			name:     "Short XML",
			input:    "<xml/>",
			expected: true,
		},
		{
			name:     "Complex safe XML",
			input:    `<ShowPlanXML><Statements><StmtSimple StatementText="SELECT * FROM users" /></Statements></ShowPlanXML>`,
			expected: true,
		},
		{
			name:     "XML with attributes",
			input:    `<ShowPlanXML Version="1.2"><QueryPlan DegreeOfParallelism="1" /></ShowPlanXML>`,
			expected: true,
		},
		{
			name:     "Long but safe XML",
			input:    strings.Repeat("<node>content</node>", 100),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsXMLContentSafe(tt.input)
			assert.Equal(t, tt.expected, result,
				"IsXMLContentSafe should return %v for input length: %d", tt.expected, len(tt.input))
		})
	}
}

func TestAnonymizationConsistency(t *testing.T) {
	t.Run("Same query produces same anonymized output", func(t *testing.T) {
		query := "SELECT * FROM users WHERE id = 123 AND name = 'John'"

		result1 := SafeAnonymizeQueryText(&query)
		result2 := SafeAnonymizeQueryText(&query)

		assert.Equal(t, result1, result2, "Same query should produce consistent anonymization")
	})

	t.Run("Different literals same structure", func(t *testing.T) {
		query1 := "SELECT * FROM users WHERE id = 123"
		query2 := "SELECT * FROM users WHERE id = 456"

		result1 := SafeAnonymizeQueryText(&query1)
		result2 := SafeAnonymizeQueryText(&query2)

		// Results should be identical since literals are replaced
		assert.Equal(t, result1, result2, "Queries with different literals should anonymize identically")
	})
}

func TestAnonymizationEdgeCases(t *testing.T) {
	t.Run("Very nested query", func(t *testing.T) {
		query := `
			SELECT * FROM (
				SELECT * FROM (
					SELECT * FROM (
						SELECT * FROM users WHERE id = 1
					) AS t1 WHERE name = 'test'
				) AS t2 WHERE age > 30
			) AS t3 WHERE city = 'Seattle'
		`

		result := SafeAnonymizeQueryText(&query)
		assert.NotEmpty(t, result)
		assert.NotContains(t, result, "Seattle")
		assert.NotContains(t, result, "test")
	})

	t.Run("Query with Unicode characters", func(t *testing.T) {
		query := "SELECT * FROM users WHERE name = '日本語' OR city = 'Москва'"

		result := SafeAnonymizeQueryText(&query)
		assert.NotEmpty(t, result)
		assert.NotContains(t, result, "日本語")
		assert.NotContains(t, result, "Москва")
	})

	t.Run("Query with escaped quotes", func(t *testing.T) {
		query := "SELECT * FROM users WHERE name = 'O''Brien' AND title = 'CEO''s Assistant'"

		result := SafeAnonymizeQueryText(&query)
		assert.NotEmpty(t, result)
		assert.NotContains(t, result, "O''Brien")
		assert.NotContains(t, result, "CEO''s Assistant")
	})
}
