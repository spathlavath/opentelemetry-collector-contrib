// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// PDB Discovery Queries for dynamic PDB discovery from CDB

const (
	// DiscoverPDBServicesSQL discovers all available PDBs and their service names
	// This query is run against the CDB to find all PDBs that can be connected to
	// It excludes PDB$SEED (the seed template) and only returns PDBs in READ WRITE mode
	DiscoverPDBServicesSQL = `
		SELECT
			p.CON_ID,
			p.NAME AS PDB_NAME,
			CASE
				WHEN (SELECT value FROM v$parameter WHERE name = 'db_domain') IS NOT NULL
					AND (SELECT value FROM v$parameter WHERE name = 'db_domain') != ''
				THEN LOWER(p.NAME) || '.' || (SELECT value FROM v$parameter WHERE name = 'db_domain')
				ELSE LOWER(p.NAME)
			END AS PDB_SERVICE_FQDN,
			p.OPEN_MODE
		FROM V$PDBS p
		WHERE p.NAME != 'PDB$SEED'
			AND p.OPEN_MODE = 'READ WRITE'
		ORDER BY p.CON_ID`

	// DiscoverSpecificPDBsSQL is a template for discovering specific PDBs by name
	// This query template is used when a list of specific PDBs is provided
	// The %s placeholder should be replaced with a comma-separated list of quoted PDB names
	DiscoverSpecificPDBsSQL = `
		SELECT
			p.CON_ID,
			p.NAME AS PDB_NAME,
			CASE
				WHEN (SELECT value FROM v$parameter WHERE name = 'db_domain') IS NOT NULL
					AND (SELECT value FROM v$parameter WHERE name = 'db_domain') != ''
				THEN LOWER(p.NAME) || '.' || (SELECT value FROM v$parameter WHERE name = 'db_domain')
				ELSE LOWER(p.NAME)
			END AS PDB_SERVICE_FQDN,
			p.OPEN_MODE
		FROM V$PDBS p
		WHERE p.NAME != 'PDB$SEED'
			AND UPPER(p.NAME) IN (%s)
		ORDER BY p.CON_ID`
)

// BuildDiscoverSpecificPDBsQuery builds the query for discovering specific PDBs
func BuildDiscoverSpecificPDBsQuery(pdbNames []string) string {
	if len(pdbNames) == 0 {
		return DiscoverPDBServicesSQL
	}

	// Build the IN clause with quoted, uppercase PDB names
	inClause := ""
	for i, name := range pdbNames {
		if i > 0 {
			inClause += ", "
		}
		// Use UPPER for case-insensitive matching
		inClause += "'" + escapeSQL(name) + "'"
	}

	return "SELECT p.CON_ID, p.NAME AS PDB_NAME, " +
		"CASE WHEN (SELECT value FROM v$parameter WHERE name = 'db_domain') IS NOT NULL " +
		"AND (SELECT value FROM v$parameter WHERE name = 'db_domain') != '' " +
		"THEN LOWER(p.NAME) || '.' || (SELECT value FROM v$parameter WHERE name = 'db_domain') " +
		"ELSE LOWER(p.NAME) END AS PDB_SERVICE_FQDN, " +
		"p.OPEN_MODE FROM V$PDBS p WHERE p.NAME != 'PDB$SEED' " +
		"AND UPPER(p.NAME) IN (" + inClause + ") ORDER BY p.CON_ID"
}

// escapeSQL performs basic SQL escaping to prevent SQL injection
func escapeSQL(s string) string {
	result := ""
	for _, c := range s {
		if c == '\'' {
			result += "''"
		} else {
			result += string(c)
		}
	}
	return result
}
