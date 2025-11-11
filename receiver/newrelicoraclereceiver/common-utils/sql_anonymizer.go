package commonutils

import (
	"regexp"
	"strings"
)

// AnonymizeAndNormalize anonymizes literal values and normalizes a SQL query.
func AnonymizeAndNormalize(query string) string {
	// Replace numeric literals with a placeholder.
	reNumbers := regexp.MustCompile(`\d+`)
	cleanedQuery := reNumbers.ReplaceAllString(query, "?")

	// Replace single-quoted string literals with a placeholder.
	reSingleQuotes := regexp.MustCompile(`'[^']*'`)
	cleanedQuery = reSingleQuotes.ReplaceAllString(cleanedQuery, "?")

	// Convert to lowercase for normalization.
	cleanedQuery = strings.ToLower(cleanedQuery)

	// Remove semicolons.
	cleanedQuery = strings.ReplaceAll(cleanedQuery, ";", "")

	// Trim leading/trailing whitespace.
	cleanedQuery = strings.TrimSpace(cleanedQuery)

	// Normalize internal whitespace (collapse multiple spaces into single spaces).
	cleanedQuery = strings.Join(strings.Fields(cleanedQuery), " ")

	return cleanedQuery
}

func AnonymizePlanData(planText string) string {
	if planText == "" {
		return ""
	}

	// 1. Pattern for Numeric Literals: (e.g., = 123, > 5000)
	// Looks for comparison operators followed by a space and a number.
	// ([\=\<\>\!]\s+) matches (=, <, >, !=) followed by space.
	// (\d+) matches one or more digits.
	// Note: We avoid lookbehinds in standard Go regex; this version is simpler.
	const patternNumbers = `([\=\<\>\!]\s+)\d+`
	reNumbers := regexp.MustCompile(patternNumbers)
	
	// Replacement: \1 references the matched comparison operator and space group.
	// This preserves the operator but replaces the sensitive number.
	anonymizedText := reNumbers.ReplaceAllString(planText, `${1}[NUMERIC_LITERAL]`)

	// 2. Pattern for String Literals: (e.g., 'SMITH', 'user@domain.com')
	// Matches any content enclosed in single quotes.
	const patternStrings = `'[^']+'`
	reStrings := regexp.MustCompile(patternStrings)
	
	// Replacement: Replaces the entire quoted string with the masked label.
	anonymizedText = reStrings.ReplaceAllString(anonymizedText, `'[STRING_LITERAL]'`)

	// 3. Pattern for Sensitive Column Names: (e.g., SSN, ACCOUNT_NO)
	// Define sensitive column names for case-insensitive masking.
	sensitiveCols := []string{"SSN", "TAX_ID", "ACCOUNT_NO", "PASSWORD"}
	
	// Create a dynamic regex that matches any of the sensitive names globally and case-insensitively.
	const flags = "(?i)" // Case-insensitive flag
	patternCols := flags + "(" + strings.Join(sensitiveCols, "|") + ")"
	reCols := regexp.MustCompile(patternCols)
	
	// Replacement: Replaces the sensitive column name itself.
	anonymizedText = reCols.ReplaceAllString(anonymizedText, "[REDACTED_COLUMN]")

	return anonymizedText
}