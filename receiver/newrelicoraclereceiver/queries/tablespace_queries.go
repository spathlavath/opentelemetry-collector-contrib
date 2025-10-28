// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// Oracle tablespace and storage related queries

// Core Tablespace Metrics
const (
	// TablespaceMetricsSQL returns tablespace usage statistics
	TablespaceMetricsSQL = `
		SELECT 
			a.TABLESPACE_NAME,
			a.USED_PERCENT,
			a.USED_SPACE * b.BLOCK_SIZE AS "USED",
			a.TABLESPACE_SIZE * b.BLOCK_SIZE AS "SIZE",
			b.TABLESPACE_OFFLINE AS "OFFLINE"
		FROM DBA_TABLESPACE_USAGE_METRICS a
		JOIN (
			SELECT
				TABLESPACE_NAME,
				BLOCK_SIZE,
				MAX(CASE WHEN status = 'OFFLINE' THEN 1 ELSE 0 END) AS "TABLESPACE_OFFLINE"
			FROM DBA_TABLESPACES
			GROUP BY TABLESPACE_NAME, BLOCK_SIZE
		) b
		ON a.TABLESPACE_NAME = b.TABLESPACE_NAME`

	// GlobalNameTablespaceSQL returns tablespace names with global database name
	GlobalNameTablespaceSQL = `
		SELECT
			t1.TABLESPACE_NAME,
			t2.GLOBAL_NAME
		FROM (SELECT TABLESPACE_NAME FROM DBA_TABLESPACES) t1,
			 (SELECT GLOBAL_NAME FROM global_name) t2`

	// DBIDTablespaceSQL returns tablespace names with database ID
	DBIDTablespaceSQL = `
		SELECT
			t1.TABLESPACE_NAME,
			t2.DBID
		FROM (SELECT TABLESPACE_NAME FROM DBA_TABLESPACES) t1,
			 (SELECT DBID FROM v$database) t2`
)

// Datafile Status Queries
const (
	// CDBDatafilesOfflineTablespaceSQL returns offline datafiles count per tablespace (CDB context)
	CDBDatafilesOfflineTablespaceSQL = `
		SELECT
			SUM(CASE WHEN ONLINE_STATUS IN ('ONLINE', 'SYSTEM','RECOVER') THEN 0 ELSE 1 END) AS CDB_DATAFILES_OFFLINE,
			TABLESPACE_NAME
		FROM dba_data_files
		GROUP BY TABLESPACE_NAME`

	// PDBDatafilesOfflineTablespaceSQL returns offline datafiles count per tablespace (PDB context)
	PDBDatafilesOfflineTablespaceSQL = `
		SELECT
			SUM(CASE WHEN ONLINE_STATUS IN ('ONLINE','SYSTEM','RECOVER') THEN 0 ELSE 1 END) AS PDB_DATAFILES_OFFLINE,
			a.TABLESPACE_NAME
		FROM cdb_data_files a, cdb_pdbs b
		WHERE a.con_id = b.con_id
		GROUP BY a.TABLESPACE_NAME`

	// PDBDatafilesOfflineCurrentContainerSQL returns offline datafiles for current container
	PDBDatafilesOfflineCurrentContainerSQL = `
		SELECT
			SUM(CASE WHEN ONLINE_STATUS IN ('ONLINE','SYSTEM','RECOVER') THEN 0 ELSE 1 END) AS PDB_DATAFILES_OFFLINE,
			TABLESPACE_NAME
		FROM dba_data_files
		GROUP BY TABLESPACE_NAME`

	// PDBNonWriteTablespaceSQL returns non-writable tablespaces count (PDB context)
	PDBNonWriteTablespaceSQL = `
		SELECT 
			TABLESPACE_NAME, 
			SUM(CASE WHEN ONLINE_STATUS IN ('ONLINE','SYSTEM','RECOVER') THEN 0 ELSE 1 END) AS PDB_NON_WRITE_MODE
		FROM cdb_data_files a, cdb_pdbs b
		WHERE a.con_id = b.con_id
		GROUP BY TABLESPACE_NAME`

	// PDBNonWriteCurrentContainerSQL returns non-writable tablespaces for current container
	PDBNonWriteCurrentContainerSQL = `
		SELECT 
			TABLESPACE_NAME, 
			SUM(CASE WHEN ONLINE_STATUS IN ('ONLINE','SYSTEM','RECOVER') THEN 0 ELSE 1 END) AS PDB_NON_WRITE_MODE
		FROM dba_data_files
		GROUP BY TABLESPACE_NAME`
)

// Container Database Tablespace Queries
const (
	// CDBTablespaceUsageSQL returns tablespace usage across all containers
	CDBTablespaceUsageSQL = `
		SELECT 
			con_id,
			tablespace_name,
			used_space * block_size AS used_bytes,
			tablespace_size * block_size AS total_bytes,
			used_percent
		FROM CDB_TABLESPACE_USAGE_METRICS
		WHERE ROWNUM <= 5000`

	// CDBDataFilesSQL returns datafile information across all containers
	CDBDataFilesSQL = `
		SELECT 
			con_id,
			file_name,
			tablespace_name,
			bytes,
			status,
			autoextensible,
			maxbytes,
			user_bytes
		FROM CDB_DATA_FILES
		WHERE ROWNUM <= 10000`
)
