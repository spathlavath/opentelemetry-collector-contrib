package helpers

import (
	"crypto/md5"
	"encoding/hex"
	"regexp"
	"strings"
	"unicode"
)

// NormalizeSqlAndHash normalizes a SQL statement following New Relic Java agent logic
// and returns both the normalized SQL and its MD5 hash.
// This is used for cross-language SQL comparison and query identification.
//
// Normalization rules (SQL Server T-SQL specific):
// - Converts to uppercase
// - Normalizes T-SQL bind variables (@name, @1) to '?'
// - Normalizes JDBC placeholders (?) to '?'
// - Replaces string and numeric literals with '?'
// - Removes comments (single-line and multi-line)
// - Normalizes whitespace (collapses multiple spaces into single space)
// - Normalizes IN clauses with multiple values to IN (?)
func NormalizeSqlAndHash(sql string) (normalizedSQL, hash string) {
	normalizedSQL = NormalizeSql(sql)
	hash = GenerateMD5Hash(normalizedSQL)
	return normalizedSQL, hash
}

// NormalizeSql normalizes a SQL statement based on New Relic Java agent rules.
func NormalizeSql(sql string) string {
	if sql == "" {
		return ""
	}
	// Force uppercase BEFORE normalization starts (matches Java Agent behavior)
	sql = strings.ToUpper(sql)
	sql = normalizeParametersAndLiterals(sql)
	return removeCommentsAndNormalizeWhitespace(sql)
}

// GenerateMD5Hash generates an MD5 hash of the normalized SQL
func GenerateMD5Hash(normalizedSQL string) string {
	hash := md5.Sum([]byte(normalizedSQL))
	return hex.EncodeToString(hash[:])
}

// ExtractNewRelicMetadata extracts nr_apm_guid and nr_service from New Relic query comments
// REQUIRED FORMAT: Values must be enclosed in double quotes to handle commas and special characters
//
// Supported formats:
// 1. APM GUID and Service: /* nr_apm_guid="MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI5MjMzNDQwNw", nr_service="order-service" */
// 2. Service GUID variant: /* nr_service_guid="MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI5MjMzNDQwNw", nr_service="order-service" */
// 3. Service only: /* nr_service="MyApp-SQLServer, Background Job" */
// 4. Any order: /* nr_service="MyApp", nr_apm_guid="XYZ789" */
// 5. With spaces: /* nr_service = "MyApp" , nr_apm_guid = "ABC" */
// 6. APM GUID only: /* nr_apm_guid="ABC123" */
//
// Returns: (nr_apm_guid, client_name)
func ExtractNewRelicMetadata(sql string) (nrApmGuid, nrService string) {
	// Match nr_apm_guid OR nr_service_guid OR nr_guid with quoted values (spaces around = are optional)
	// Format: nr_apm_guid = "base64_encoded_guid" OR nr_service_guid = "base64_encoded_guid" OR nr_guid = "base64_encoded_guid"
	// Try in order: nr_apm_guid, nr_service_guid, nr_guid (shortest)
	apmGuidRegex := regexp.MustCompile(`nr_apm_guid\s*=\s*"([^"]+)"`)
	serviceGuidRegex := regexp.MustCompile(`nr_service_guid\s*=\s*"([^"]+)"`)
	guidRegex := regexp.MustCompile(`nr_guid\s*=\s*"([^"]+)"`)

	// Match nr_service with quoted values (spaces around = are optional)
	// Format: nr_service = "value with, commas and special chars"
	serviceRegex := regexp.MustCompile(`nr_service\s*=\s*"([^"]+)"`)

	// Extract nr_apm_guid (try standard format first, then variants)
	apmGuidMatch := apmGuidRegex.FindStringSubmatch(sql)
	if len(apmGuidMatch) > 1 {
		nrApmGuid = strings.TrimSpace(apmGuidMatch[1])
	} else {
		// Try nr_service_guid variant
		serviceGuidMatch := serviceGuidRegex.FindStringSubmatch(sql)
		if len(serviceGuidMatch) > 1 {
			nrApmGuid = strings.TrimSpace(serviceGuidMatch[1])
		} else {
			// Try nr_guid (shortest variant)
			guidMatch := guidRegex.FindStringSubmatch(sql)
			if len(guidMatch) > 1 {
				nrApmGuid = strings.TrimSpace(guidMatch[1])
			}
		}
	}

	// Extract nr_service
	serviceMatch := serviceRegex.FindStringSubmatch(sql)
	if len(serviceMatch) > 1 {
		nrService = strings.TrimSpace(serviceMatch[1])
	}

	return nrApmGuid, nrService
}

// sqlNormalizerState holds state during SQL normalization
type sqlNormalizerState struct {
	sql               string
	length            int
	idx               int
	lastWasWhitespace bool
}

func newSqlNormalizerState(sql string) *sqlNormalizerState {
	return &sqlNormalizerState{
		sql:               sql,
		length:            len(sql),
		idx:               0,
		lastWasWhitespace: true, // Start as true to trim leading whitespace
	}
}

func (s *sqlNormalizerState) hasMore() bool {
	return s.idx < s.length
}

func (s *sqlNormalizerState) hasNext() bool {
	return s.idx+1 < s.length
}

func (s *sqlNormalizerState) current() byte {
	return s.sql[s.idx]
}

func (s *sqlNormalizerState) peek() byte {
	return s.sql[s.idx+1]
}

func (s *sqlNormalizerState) advance() {
	s.idx++
}

func (s *sqlNormalizerState) advanceBy(count int) {
	s.idx += count
}

// normalizeParametersAndLiterals normalizes all parameter placeholders and literals
func normalizeParametersAndLiterals(sql string) string {
	if sql == "" {
		return ""
	}

	var result strings.Builder
	result.Grow(len(sql))
	state := newSqlNormalizerState(sql)

	for state.hasMore() {
		current := state.current()

		if current == '\'' {
			// Replace string literals with ?
			skipStringLiteral(state)
			result.WriteByte('?')
		} else if current == '(' {
			// Check for IN clause with multiple values/placeholders
			if isPrecededByIn(&result) {
				inClause := tryNormalizeInClause(state)
				result.WriteString(inClause)
			} else {
				result.WriteByte('(')
				state.advance()
			}
		} else if isNumericLiteral(state) {
			// Numeric literals
			skipNumericLiteral(state)
			result.WriteByte('?')
		} else if isPlaceholder(state) {
			// Any placeholder type (T-SQL @param or JDBC ?) --> ?
			skipPlaceholder(state)
			result.WriteByte('?')
		} else {
			// Just append anything else
			result.WriteByte(current)
			state.advance()
		}
	}

	return result.String()
}

// isPrecededByIn checks if the result is preceded by "IN"
func isPrecededByIn(result *strings.Builder) bool {
	str := result.String()
	if len(str) < 2 {
		return false
	}

	// Scan backwards, skipping whitespace
	idx := len(str) - 1
	for idx >= 0 && unicode.IsSpace(rune(str[idx])) {
		idx--
	}

	// Check if we have at least "IN" (2 characters)
	if idx < 1 {
		return false
	}

	// Check for "IN" - scanning backwards we see 'N' first, then 'I'
	if str[idx] == 'N' && str[idx-1] == 'I' {
		// Make sure "IN" is a complete token, not part of a larger word like "WITHIN"
		return idx < 2 || !isIdentifierChar(rune(str[idx-2]))
	}

	return false
}

// isIdentifierChar checks if a character is valid in an identifier
func isIdentifierChar(c rune) bool {
	return unicode.IsLetter(c) || unicode.IsDigit(c) || c == '_' || c == '@'
}

// isPlaceholder checks if current position is a parameter placeholder
// T-SQL: @paramname or @1
// JDBC: ?
func isPlaceholder(state *sqlNormalizerState) bool {
	current := state.current()

	// JDBC-style placeholder
	if current == '?' {
		return true
	}

	// T-SQL named parameter: @paramname
	if current == '@' {
		// Make sure it's not just a lone @
		if !state.hasNext() {
			return false
		}

		next := state.peek()
		// @ followed by letter, digit, or underscore is a parameter
		return unicode.IsLetter(rune(next)) || unicode.IsDigit(rune(next)) || next == '_'
	}

	return false
}

// skipPlaceholder skips over a placeholder
func skipPlaceholder(state *sqlNormalizerState) {
	current := state.current()

	if current == '?' {
		state.advance()
		return
	}

	// T-SQL parameter: @name or @123
	if current == '@' {
		state.advance()
		// Skip the parameter name
		for state.hasMore() {
			c := state.current()
			if unicode.IsLetter(rune(c)) || unicode.IsDigit(rune(c)) || c == '_' {
				state.advance()
			} else {
				break
			}
		}
	}
}

// isNumericLiteral checks if current position is a numeric literal
func isNumericLiteral(state *sqlNormalizerState) bool {
	current := state.current()

	// Must start with a digit or minus sign (for negative numbers)
	if !unicode.IsDigit(rune(current)) && current != '-' && current != '+' {
		return false
	}

	// If it's a sign, next must be a digit
	if current == '-' || current == '+' {
		if !state.hasNext() {
			return false
		}
		next := state.peek()
		if !unicode.IsDigit(rune(next)) {
			return false
		}
	}

	// Make sure it's not part of an identifier (e.g., table1, _2column)
	// Check if preceded by identifier character
	if state.idx > 0 {
		prev := state.sql[state.idx-1]
		if unicode.IsLetter(rune(prev)) || prev == '_' || prev == '@' {
			return false
		}
	}

	return true
}

// skipNumericLiteral skips over a numeric literal (including scientific notation)
func skipNumericLiteral(state *sqlNormalizerState) {
	// Skip optional sign
	if state.current() == '-' || state.current() == '+' {
		state.advance()
	}

	// Skip digits before decimal point
	for state.hasMore() && unicode.IsDigit(rune(state.current())) {
		state.advance()
	}

	// Skip decimal point and digits after
	if state.hasMore() && state.current() == '.' {
		state.advance()
		for state.hasMore() && unicode.IsDigit(rune(state.current())) {
			state.advance()
		}
	}

	// Skip scientific notation (e.g., 1.5E6, 2e-3)
	if state.hasMore() && (state.current() == 'E' || state.current() == 'e') {
		state.advance()
		// Skip optional sign in exponent
		if state.hasMore() && (state.current() == '+' || state.current() == '-') {
			state.advance()
		}
		// Skip exponent digits
		for state.hasMore() && unicode.IsDigit(rune(state.current())) {
			state.advance()
		}
	}
}

// skipStringLiteral skips over a string literal
func skipStringLiteral(state *sqlNormalizerState) {
	// Assume current character is opening quote
	state.advance()

	for state.hasMore() {
		current := state.current()

		if current == '\'' {
			// Check for escaped quote (two single quotes in T-SQL)
			if state.hasNext() && state.peek() == '\'' {
				state.advanceBy(2) // Skip both quotes
			} else {
				state.advance() // Skip closing quote
				return
			}
		} else {
			state.advance()
		}
	}
}

// tryNormalizeInClause attempts to normalize an IN clause
func tryNormalizeInClause(state *sqlNormalizerState) string {
	startIdx := state.idx

	// Skip opening parenthesis
	if state.current() != '(' {
		return string(state.sql[startIdx])
	}
	state.advance()

	hasMultipleValues := false
	valueCount := 0

	// Scan through the clause
	for state.hasMore() {
		current := state.current()

		if current == ')' {
			state.advance()
			break
		} else if current == ',' {
			hasMultipleValues = true
			state.advance()
		} else if current == '\'' {
			skipStringLiteral(state)
			valueCount++
		} else if unicode.IsSpace(rune(current)) {
			state.advance()
		} else if isNumericLiteral(state) {
			skipNumericLiteral(state)
			valueCount++
		} else if isPlaceholder(state) {
			skipPlaceholder(state)
			valueCount++
		} else {
			// Not a simple IN clause, return original
			length := state.idx - startIdx
			return string(state.sql[startIdx : startIdx+length])
		}
	}

	// If multiple values or placeholders found, normalize to IN (?)
	if hasMultipleValues || valueCount > 1 {
		return "(?)"
	}

	// Single value, return IN (?)
	if valueCount == 1 {
		return "(?)"
	}

	// Empty or invalid, return what we consumed
	length := state.idx - startIdx
	if length > 0 {
		return string(state.sql[startIdx : startIdx+length])
	}
	return "()"
}

// removeCommentsAndNormalizeWhitespace removes comments and normalizes whitespace
func removeCommentsAndNormalizeWhitespace(sql string) string {
	if sql == "" {
		return ""
	}

	var result strings.Builder
	result.Grow(len(sql))
	state := newSqlNormalizerState(sql)

	for state.hasMore() {
		current := state.current()

		if current == '-' && state.hasNext() && state.peek() == '-' {
			// Skip single-line comment
			skipSingleLineComment(state)
			state.lastWasWhitespace = true
		} else if current == '/' && state.hasNext() && state.peek() == '*' {
			// Skip multi-line comment
			skipMultiLineComment(state)
			state.lastWasWhitespace = true
		} else if unicode.IsSpace(rune(current)) {
			// Collapse multiple whitespace to single space
			if !state.lastWasWhitespace {
				result.WriteByte(' ')
				state.lastWasWhitespace = true
			}
			state.advance()
		} else {
			result.WriteByte(current)
			state.lastWasWhitespace = false
			state.advance()
		}
	}

	// Trim trailing whitespace
	return strings.TrimSpace(result.String())
}

// skipSingleLineComment skips a single-line comment (-- comment)
func skipSingleLineComment(state *sqlNormalizerState) {
	// Skip until newline or end of string
	for state.hasMore() {
		current := state.current()
		state.advance()
		if current == '\n' {
			break
		}
	}
}

// skipMultiLineComment skips a multi-line comment (/* comment */)
func skipMultiLineComment(state *sqlNormalizerState) {
	// Skip opening /*
	state.advanceBy(2)

	// Skip until closing */
	for state.hasMore() {
		if state.current() == '*' && state.hasNext() && state.peek() == '/' {
			state.advanceBy(2)
			return
		}
		state.advance()
	}
}
