package commonutils

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
// Normalization rules (Oracle-specific):
// - Converts to uppercase
// - Normalizes Oracle bind variables (:name, :1) to '?'
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
	// Java Agent forces uppercase BEFORE normalization starts
	sql = strings.ToUpper(sql)
	sql = normalizeParametersAndLiterals(sql)
	return removeCommentsAndNormalizeWhitespace(sql)
}

// GenerateMD5Hash generates an MD5 hash of the normalized SQL
func GenerateMD5Hash(normalizedSQL string) string {
	hash := md5.Sum([]byte(normalizedSQL))
	return hex.EncodeToString(hash[:])
}

// ExtractNewRelicMetadata extracts nr_service_guid from New Relic query comments
// Only supports quoted format: /* nr_service_guid="VALUE" */
// Returns: nr_service_guid value or empty string if not found
func ExtractNewRelicMetadata(sql string) string {
	quotedGuidRegex := regexp.MustCompile(`nr_service_guid="([^"]*)"`)
	if match := quotedGuidRegex.FindStringSubmatch(sql); len(match) > 1 {
		return match[1]
	}
	return ""
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
			// Any placeholder type --> ?
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
	// Added backtick (`) to match isNumericLiteral logic
	return unicode.IsLetter(c) || unicode.IsDigit(c) || c == '_' || c == '`'
}

// isPlaceholder checks if current position is a parameter placeholder
func isPlaceholder(state *sqlNormalizerState) bool {
	c := state.current()

	// JDBC style: ?
	if c == '?' {
		return true
	}

	// Oracle bind variable style: :name or :1
	if c == ':' && state.hasNext() && isIdentifierChar(rune(state.peek())) {
		return true
	}

	return false
}

// skipPlaceholder skips over any type of prepared statement placeholder
func skipPlaceholder(state *sqlNormalizerState) {
	c := state.current()

	if c == '?' {
		// JDBC placeholder
		state.advance()
	} else if c == ':' {
		// Oracle bind variable: :NAME or :1
		state.advance() // Skip :
		for state.hasMore() && isIdentifierChar(rune(state.current())) {
			state.advance()
		}
	}
}

// isNumericLiteral checks if current position is a numeric literal
func isNumericLiteral(state *sqlNormalizerState) bool {
	c := state.current()

	// Check for digit, minus, plus, or decimal point
	if !unicode.IsDigit(rune(c)) && c != '-' && c != '+' && c != '.' {
		return false
	}

	// Make sure it's not part of an identifier
	if state.idx > 0 {
		prev := state.sql[state.idx-1]
		// If preceded by letter, digit, underscore, or backtick, it's part of identifier
		if unicode.IsLetter(rune(prev)) || prev == '_' || prev == '`' {
			return false
		}
	}

	// Look ahead to confirm it's a complete number
	savedIdx := state.idx

	// Handle optional sign
	if c == '-' || c == '+' {
		state.advance()
		if !state.hasMore() {
			state.idx = savedIdx
			return false
		}
		c = state.current()
	}

	// Numbers starting with decimal point
	if c == '.' {
		state.advance()
		if !state.hasMore() || !unicode.IsDigit(rune(state.current())) {
			state.idx = savedIdx
			return false
		}
		// Looks like an actual decimal number
		state.idx = savedIdx
		return true
	}

	// Must have at least one digit before optional decimal point
	if !unicode.IsDigit(rune(c)) {
		state.idx = savedIdx
		return false
	}

	state.idx = savedIdx
	return true
}

// skipNumericLiteral skips over a numeric literal
func skipNumericLiteral(state *sqlNormalizerState) {
	// + or - sign
	c := state.current()
	if c == '-' || c == '+' {
		state.advance()
	}

	// Skip any digits
	for state.hasMore() && unicode.IsDigit(rune(state.current())) {
		state.advance()
	}

	// Decimal points
	if state.hasMore() && state.current() == '.' {
		state.advance()
		for state.hasMore() && unicode.IsDigit(rune(state.current())) {
			state.advance()
		}
	}

	// Scientific notation (1e10, 1E-5)
	if state.hasMore() && state.current() == 'E' {
		state.advance()
		if state.hasMore() && (state.current() == '+' || state.current() == '-') {
			state.advance()
		}
		for state.hasMore() && unicode.IsDigit(rune(state.current())) {
			state.advance()
		}
	}
}

// skipStringLiteral skips over a string literal, handling escaped quotes
func skipStringLiteral(state *sqlNormalizerState) {
	state.advance() // Skip the opening quote

	for state.hasMore() {
		c := state.current()

		if c == '\'' {
			// Check for escaped quote ''
			if state.hasNext() && state.peek() == '\'' {
				state.advanceBy(2) // Skip both quotes
			} else {
				state.advance() // Skip closing quote
				break
			}
		} else if c == '\\' {
			// Handle backslash escaping (MySQL, PostgreSQL)
			state.advance()
			if state.hasMore() {
				state.advance()
			}
		} else {
			state.advance()
		}
	}
}

// tryNormalizeInClause tries to normalize an IN clause like IN (1,2,3) or IN (?,?,?) to IN (?)
func tryNormalizeInClause(state *sqlNormalizerState) string {
	// Save position in case we need to backtrack
	saveIdx := state.idx

	state.advance() // Opening (

	itemCount := 0
	allParametersOrLiterals := true
	foundNonWhitespace := false

	// Scan the contents of the parentheses
	for state.hasMore() && state.current() != ')' {
		c := state.current()

		if unicode.IsSpace(rune(c)) {
			state.advance()
		} else if c == ',' {
			state.advance()
		} else if isPlaceholder(state) {
			foundNonWhitespace = true
			itemCount++
			skipPlaceholder(state)
		} else if isNumericLiteral(state) {
			foundNonWhitespace = true
			itemCount++
			skipNumericLiteral(state)
		} else if c == '\'' {
			foundNonWhitespace = true
			itemCount++
			skipStringLiteral(state)
		} else {
			// Not a list, bail
			allParametersOrLiterals = false
			break
		}
	}

	// Check if we found a closing paren and have multiple items
	if allParametersOrLiterals && foundNonWhitespace && itemCount > 1 &&
		state.hasMore() && state.current() == ')' {
		state.advance() // Skip closing )
		return "(?)"
	}

	// Not a normalizable IN clause, restore position
	state.idx = saveIdx
	state.advance()
	return "("
}

// removeCommentsAndNormalizeWhitespace strips comments and normalizes whitespace
func removeCommentsAndNormalizeWhitespace(sql string) string {
	var result strings.Builder
	result.Grow(len(sql))
	state := newSqlNormalizerState(sql)

	for state.hasMore() {
		current := state.current()

		if current == '\'' {
			processStringLiteral(&result, state)
		} else if isMultilineCommentStart(state) {
			processMultilineComment(state)
			result.WriteByte('?')
			state.lastWasWhitespace = false
		} else if isSingleLineCommentStart(state) {
			processSingleLineComment(state)
			result.WriteByte('?')
			state.lastWasWhitespace = false
		} else if current == '#' {
			processHashComment(state)
			result.WriteByte('?')
			state.lastWasWhitespace = false
		} else if unicode.IsSpace(rune(current)) {
			processWhitespace(&result, state)
		} else {
			processRegularCharacter(&result, state)
		}
	}

	return strings.TrimSpace(result.String())
}

func processStringLiteral(result *strings.Builder, state *sqlNormalizerState) {
	result.WriteByte(state.current())
	state.lastWasWhitespace = false
	state.advance()

	for state.hasMore() {
		c := state.current()
		result.WriteByte(c)

		if c == '\'' {
			// Escaped quote '' check
			if state.hasNext() && state.peek() == '\'' {
				result.WriteByte('\'')
				state.advanceBy(2)
			} else {
				state.advance()
				break
			}
		} else {
			state.advance()
		}
	}
	state.lastWasWhitespace = false
}

func isMultilineCommentStart(state *sqlNormalizerState) bool {
	return state.current() == '/' && state.hasNext() && state.peek() == '*'
}

func processMultilineComment(state *sqlNormalizerState) {
	state.advanceBy(2) // Skip /*

	for state.idx < state.length-1 {
		if state.current() == '*' && state.peek() == '/' {
			state.advanceBy(2)
			break
		}
		state.advance()
	}

	// Handle unclosed comment
	if state.idx == state.length-1 {
		state.idx = state.length
	}
}

func isSingleLineCommentStart(state *sqlNormalizerState) bool {
	return state.current() == '-' && state.hasNext() && state.peek() == '-'
}

func processSingleLineComment(state *sqlNormalizerState) {
	state.advanceBy(2) // Skip --
	skipToEndOfLine(state)
}

func processHashComment(state *sqlNormalizerState) {
	state.advance() // Skip #
	skipToEndOfLine(state)
}

func skipToEndOfLine(state *sqlNormalizerState) {
	// Skip until newline
	for state.hasMore() && state.current() != '\n' && state.current() != '\r' {
		state.advance()
	}
	// Skip the newline character(s)
	for state.hasMore() && (state.current() == '\n' || state.current() == '\r') {
		state.advance()
	}
}

// IMPORTANT: Ensure your processWhitespace matches the Java trim logic
func processWhitespace(result *strings.Builder, state *sqlNormalizerState) {
	if !state.lastWasWhitespace && result.Len() > 0 {
		result.WriteByte(' ')
		state.lastWasWhitespace = true
	}
	state.advance()
}

func processRegularCharacter(result *strings.Builder, state *sqlNormalizerState) {
	result.WriteByte(state.current())
	state.lastWasWhitespace = false
	state.advance()
}
