// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package models

// PDBSysMetric represents a PDB system metric
type PDBSysMetric struct {
	InstID     int
	PDBName    string
	MetricName string
	Value      float64
}

// CDBCapability represents CDB feature detection result
type CDBCapability struct {
	IsCDB int64
}

// PDBCapability represents PDB capability detection result
type PDBCapability struct {
	PDBCount int64
}
