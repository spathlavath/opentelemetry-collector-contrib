// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "time"

// SQLIdentifier represents a unique SQL statement identifier with its child cursor number
// and the timestamp when the query was captured (for correlation with execution plans)
type SQLIdentifier struct {
	SQLID             string
	ChildNumber       int64
	Timestamp         time.Time // Timestamp when the query was captured (from wait event or slow query)
	PlanHash          string    // Plan hash value associated with the SQL statement
	ClientName        string    // Client service name extracted from nr_service comment
	TransactionName   string    // Transaction name extracted from nr_txn comment
	NormalisedSQLHash string    // MD5 hash of normalized SQL query
}
