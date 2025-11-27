// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package helpers

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// MetadataCache provides cached lookups for SQL Server metadata to enrich wait_resource descriptions
type MetadataCache struct {
	databases       map[int]string              // database_id -> database_name
	objects         map[int]ObjectMetadata      // object_id -> schema.object_name
	files           map[FileKey]string          // (database_id, file_id) -> file_name
	hobts           map[int64]HOBTMetadata      // hobt_id -> object/index info
	partitions      map[int64]PartitionMetadata // partition_id -> partition info
	allocationUnits map[int64]string            // allocation_unit_id -> container description

	mu              sync.RWMutex
	lastRefresh     time.Time
	refreshInterval time.Duration
	db              *sql.DB
}

// ObjectMetadata contains information about a database object
type ObjectMetadata struct {
	SchemaName string
	ObjectName string
	ObjectType string // U=Table, V=View, P=Procedure, etc.
}

// HOBTMetadata contains information about a Heap or B-Tree
type HOBTMetadata struct {
	DatabaseName string
	SchemaName   string
	ObjectName   string
	IndexName    string
	IndexType    string // HEAP, CLUSTERED, NONCLUSTERED
}

// PartitionMetadata contains partition information
type PartitionMetadata struct {
	ObjectID        int
	PartitionNumber int
}

// FileKey is a composite key for file lookups
type FileKey struct {
	DatabaseID int
	FileID     int
}

// NewMetadataCache creates a new metadata cache with the specified refresh interval
func NewMetadataCache(db *sql.DB, refreshInterval time.Duration) *MetadataCache {
	return &MetadataCache{
		databases:       make(map[int]string),
		objects:         make(map[int]ObjectMetadata),
		files:           make(map[FileKey]string),
		hobts:           make(map[int64]HOBTMetadata),
		partitions:      make(map[int64]PartitionMetadata),
		allocationUnits: make(map[int64]string),
		refreshInterval: refreshInterval,
		db:              db,
	}
}

// Refresh updates the metadata cache from SQL Server
func (mc *MetadataCache) Refresh(ctx context.Context) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Check if refresh is needed
	if time.Since(mc.lastRefresh) < mc.refreshInterval {
		return nil
	}

	// Fetch all metadata in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, 6)

	wg.Add(6)
	go func() {
		defer wg.Done()
		if err := mc.refreshDatabases(ctx); err != nil {
			errChan <- fmt.Errorf("refresh databases: %w", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := mc.refreshObjects(ctx); err != nil {
			errChan <- fmt.Errorf("refresh objects: %w", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := mc.refreshFiles(ctx); err != nil {
			errChan <- fmt.Errorf("refresh files: %w", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := mc.refreshHOBTs(ctx); err != nil {
			errChan <- fmt.Errorf("refresh hobts: %w", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := mc.refreshPartitions(ctx); err != nil {
			errChan <- fmt.Errorf("refresh partitions: %w", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := mc.refreshAllocationUnits(ctx); err != nil {
			errChan <- fmt.Errorf("refresh allocation units: %w", err)
		}
	}()

	wg.Wait()
	close(errChan)

	// Collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("metadata refresh errors: %v", errors)
	}

	mc.lastRefresh = time.Now()
	return nil
}

func (mc *MetadataCache) refreshDatabases(ctx context.Context) error {
	query := `SELECT database_id, name FROM sys.databases WHERE state = 0` // ONLINE only

	rows, err := mc.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	newDatabases := make(map[int]string)
	for rows.Next() {
		var dbID int
		var dbName string
		if err := rows.Scan(&dbID, &dbName); err != nil {
			return err
		}
		newDatabases[dbID] = dbName
	}

	mc.databases = newDatabases
	return rows.Err()
}

func (mc *MetadataCache) refreshObjects(ctx context.Context) error {
	query := `
		SELECT
			o.object_id,
			SCHEMA_NAME(o.schema_id) AS schema_name,
			o.name AS object_name,
			o.type AS object_type
		FROM sys.objects o
		WHERE o.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF') -- Tables, Views, Procedures, Functions
	`

	rows, err := mc.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	newObjects := make(map[int]ObjectMetadata)
	for rows.Next() {
		var objectID int
		var meta ObjectMetadata
		if err := rows.Scan(&objectID, &meta.SchemaName, &meta.ObjectName, &meta.ObjectType); err != nil {
			return err
		}
		newObjects[objectID] = meta
	}

	mc.objects = newObjects
	return rows.Err()
}

func (mc *MetadataCache) refreshFiles(ctx context.Context) error {
	query := `
		SELECT
			DB_ID() AS database_id,
			file_id,
			name AS file_name
		FROM sys.database_files
	`

	// We need to iterate through all databases
	// For now, just fetch files from the current database context
	// In a full implementation, we'd need to sp_MSforeachdb or similar

	rows, err := mc.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	newFiles := make(map[FileKey]string)
	for rows.Next() {
		var dbID, fileID int
		var fileName string
		if err := rows.Scan(&dbID, &fileID, &fileName); err != nil {
			return err
		}
		newFiles[FileKey{DatabaseID: dbID, FileID: fileID}] = fileName
	}

	mc.files = newFiles
	return rows.Err()
}

func (mc *MetadataCache) refreshHOBTs(ctx context.Context) error {
	query := `
		SELECT
			p.hobt_id,
			DB_NAME() AS database_name,
			SCHEMA_NAME(o.schema_id) AS schema_name,
			o.name AS object_name,
			i.name AS index_name,
			i.type_desc AS index_type
		FROM sys.partitions p
		JOIN sys.objects o ON p.object_id = o.object_id
		LEFT JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
		WHERE p.hobt_id > 0
	`

	rows, err := mc.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	newHOBTs := make(map[int64]HOBTMetadata)
	for rows.Next() {
		var hobtID int64
		var meta HOBTMetadata
		var indexName sql.NullString
		if err := rows.Scan(&hobtID, &meta.DatabaseName, &meta.SchemaName, &meta.ObjectName, &indexName, &meta.IndexType); err != nil {
			return err
		}
		if indexName.Valid {
			meta.IndexName = indexName.String
		} else {
			meta.IndexName = "HEAP"
		}
		newHOBTs[hobtID] = meta
	}

	mc.hobts = newHOBTs
	return rows.Err()
}

func (mc *MetadataCache) refreshPartitions(ctx context.Context) error {
	query := `
		SELECT
			partition_id,
			object_id,
			partition_number
		FROM sys.partitions
	`

	rows, err := mc.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	newPartitions := make(map[int64]PartitionMetadata)
	for rows.Next() {
		var partitionID int64
		var meta PartitionMetadata
		if err := rows.Scan(&partitionID, &meta.ObjectID, &meta.PartitionNumber); err != nil {
			return err
		}
		newPartitions[partitionID] = meta
	}

	mc.partitions = newPartitions
	return rows.Err()
}

func (mc *MetadataCache) refreshAllocationUnits(ctx context.Context) error {
	query := `
		SELECT
			au.allocation_unit_id,
			SCHEMA_NAME(o.schema_id) + '.' + o.name + ' (' + au.type_desc + ')' AS description
		FROM sys.allocation_units au
		JOIN sys.partitions p ON au.container_id = p.partition_id
		JOIN sys.objects o ON p.object_id = o.object_id
	`

	rows, err := mc.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	newAllocationUnits := make(map[int64]string)
	for rows.Next() {
		var auID int64
		var description string
		if err := rows.Scan(&auID, &description); err != nil {
			return err
		}
		newAllocationUnits[auID] = description
	}

	mc.allocationUnits = newAllocationUnits
	return rows.Err()
}

// Lookup methods with read locks

func (mc *MetadataCache) GetDatabaseName(dbID int) (string, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	name, ok := mc.databases[dbID]
	return name, ok
}

func (mc *MetadataCache) GetObjectMetadata(objectID int) (ObjectMetadata, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	meta, ok := mc.objects[objectID]
	return meta, ok
}

func (mc *MetadataCache) GetFileName(dbID, fileID int) (string, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	name, ok := mc.files[FileKey{DatabaseID: dbID, FileID: fileID}]
	return name, ok
}

func (mc *MetadataCache) GetHOBTMetadata(hobtID int64) (HOBTMetadata, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	meta, ok := mc.hobts[hobtID]
	return meta, ok
}

func (mc *MetadataCache) GetPartitionMetadata(partitionID int64) (PartitionMetadata, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	meta, ok := mc.partitions[partitionID]
	return meta, ok
}

func (mc *MetadataCache) GetAllocationUnitDescription(auID int64) (string, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	desc, ok := mc.allocationUnits[auID]
	return desc, ok
}

// GetCacheStats returns statistics about the cache
func (mc *MetadataCache) GetCacheStats() map[string]int {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	return map[string]int{
		"databases":        len(mc.databases),
		"objects":          len(mc.objects),
		"files":            len(mc.files),
		"hobts":            len(mc.hobts),
		"partitions":       len(mc.partitions),
		"allocation_units": len(mc.allocationUnits),
	}
}
