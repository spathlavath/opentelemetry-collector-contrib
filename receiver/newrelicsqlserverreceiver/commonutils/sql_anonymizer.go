// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package commonutils

import (
	"regexp"
	"strings"
)

// AnonymizeSQL anonymizes SQL queries by removing literal values while preserving structure
func AnonymizeSQL(query string) string {
	if query == "" {
		return query
	}

	// Remove single-quoted string literals
	query = regexp.MustCompile("'[^']*'").ReplaceAllString(query, "'?'")

	// Remove numeric literals
	query = regexp.MustCompile(`\b\d+\b`).ReplaceAllString(query, "?")

	// Normalize whitespace
	query = regexp.MustCompile(`\s+`).ReplaceAllString(query, " ")

	return strings.TrimSpace(query)
}
