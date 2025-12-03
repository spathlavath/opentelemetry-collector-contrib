// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

// SQLIdentifier represents a unique SQL statement identifier with its child cursor number
type SQLIdentifier struct {
	SQLID       string
	ChildNumber int64
}
