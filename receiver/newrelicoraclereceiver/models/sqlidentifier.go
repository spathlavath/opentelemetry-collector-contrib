// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

// SQLIdentifier represents a unique SQL statement identifier with its child number
// This is used to precisely identify which execution plan a session is using
type SQLIdentifier struct {
	SQLID       string
	ChildNumber int64
}

// NewSQLIdentifier creates a new SQLIdentifier
func NewSQLIdentifier(sqlID string, childNumber int64) SQLIdentifier {
	return SQLIdentifier{
		SQLID:       sqlID,
		ChildNumber: childNumber,
	}
}

// String returns a formatted string representation for logging
func (s SQLIdentifier) String() string {
	if s.ChildNumber >= 0 {
		return s.SQLID + "#" + string(rune(s.ChildNumber))
	}
	return s.SQLID
}
