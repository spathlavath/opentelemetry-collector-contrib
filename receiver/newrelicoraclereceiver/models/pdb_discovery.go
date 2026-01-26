// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// DiscoveredPDB represents a discovered Pluggable Database from the CDB
type DiscoveredPDB struct {
	ConID          sql.NullInt64  // Container ID
	PDBName        sql.NullString // PDB Name (e.g., "PDB1")
	PDBServiceFQDN sql.NullString // Fully qualified service name (e.g., "pdb1.domain.com")
	OpenMode       sql.NullString // PDB Open Mode (e.g., "READ WRITE")
}

// GetPDBName returns the PDB name as a string
func (p *DiscoveredPDB) GetPDBName() string {
	if p.PDBName.Valid {
		return p.PDBName.String
	}
	return ""
}

// GetServiceName returns the service name for connecting to this PDB
// Uses PDBServiceFQDN if available, otherwise uses PDBName
func (p *DiscoveredPDB) GetServiceName() string {
	if p.PDBServiceFQDN.Valid && p.PDBServiceFQDN.String != "" {
		return p.PDBServiceFQDN.String
	}
	return p.GetPDBName()
}

// GetConID returns the container ID
func (p *DiscoveredPDB) GetConID() int64 {
	if p.ConID.Valid {
		return p.ConID.Int64
	}
	return 0
}

// IsReadWrite returns true if the PDB is in READ WRITE mode
func (p *DiscoveredPDB) IsReadWrite() bool {
	if p.OpenMode.Valid {
		return p.OpenMode.String == "READ WRITE"
	}
	return false
}
