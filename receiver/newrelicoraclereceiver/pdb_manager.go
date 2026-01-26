// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicoraclereceiver

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// PDBConnectionState holds the state for a PDB connection
type PDBConnectionState struct {
	PDBName     string               // Name of the PDB
	ServiceName string               // Service name used for connection
	ConID       int64                // Container ID
	DB          *sql.DB              // Database connection
	Client      client.OracleClient  // Oracle client for this PDB
	LastError   error                // Last error encountered
	LastRefresh time.Time            // Last successful refresh time
}

// PDBManager manages CDB and PDB connections for dynamic PDB discovery
type PDBManager struct {
	// Configuration
	config     *Config
	logger     *zap.Logger
	sqlOpener  func(dataSourceName string) (*sql.DB, error)
	hostAddr   string
	hostPort   int64

	// CDB connection
	cdbDB     *sql.DB
	cdbClient client.OracleClient

	// PDB connections
	pdbConnections map[string]*PDBConnectionState
	pdbMutex       sync.RWMutex

	// Discovery state
	lastDiscovery    time.Time
	discoveryMinutes int // Minimum minutes between discovery refreshes
}

// NewPDBManager creates a new PDB Manager
func NewPDBManager(
	config *Config,
	logger *zap.Logger,
	sqlOpener func(dataSourceName string) (*sql.DB, error),
	hostAddr string,
	hostPort int64,
) *PDBManager {
	return &PDBManager{
		config:           config,
		logger:           logger,
		sqlOpener:        sqlOpener,
		hostAddr:         hostAddr,
		hostPort:         hostPort,
		pdbConnections:   make(map[string]*PDBConnectionState),
		discoveryMinutes: 5, // Default: re-discover every 5 minutes
	}
}

// Initialize sets up the CDB connection and performs initial PDB discovery
func (m *PDBManager) Initialize(ctx context.Context) error {
	m.logger.Info("Initializing PDB Manager",
		zap.String("pdb_list", m.config.PDBList),
		zap.Bool("discover_all", m.config.IsDiscoverAllPDBs()))

	// Connect to CDB first
	if err := m.connectToCDB(); err != nil {
		return fmt.Errorf("failed to connect to CDB: %w", err)
	}

	// Perform initial PDB discovery
	if err := m.RefreshPDBConnections(ctx); err != nil {
		return fmt.Errorf("failed initial PDB discovery: %w", err)
	}

	return nil
}

// connectToCDB establishes connection to the Container Database
func (m *PDBManager) connectToCDB() error {
	dataSource := getDataSource(*m.config)

	db, err := m.sqlOpener(dataSource)
	if err != nil {
		return fmt.Errorf("failed to open CDB connection: %w", err)
	}

	// Configure connection pool
	if !m.config.DisableConnectionPool {
		db.SetMaxOpenConns(m.config.MaxOpenConnections)
	} else {
		db.SetMaxOpenConns(1)
	}
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetConnMaxIdleTime(30 * time.Second)

	m.cdbDB = db
	m.cdbClient = client.NewSQLClient(db)

	m.logger.Info("Connected to CDB successfully")
	return nil
}

// RefreshPDBConnections discovers PDBs and establishes connections
func (m *PDBManager) RefreshPDBConnections(ctx context.Context) error {
	m.pdbMutex.Lock()
	defer m.pdbMutex.Unlock()

	// Check if we should skip discovery based on timing
	if time.Since(m.lastDiscovery) < time.Duration(m.discoveryMinutes)*time.Minute && len(m.pdbConnections) > 0 {
		m.logger.Debug("Skipping PDB discovery, within refresh interval",
			zap.Duration("since_last", time.Since(m.lastDiscovery)))
		return nil
	}

	var discoveredPDBs []models.DiscoveredPDB
	var err error

	if m.config.IsDiscoverAllPDBs() {
		// Discover all PDBs
		m.logger.Info("Discovering all PDBs from CDB")
		discoveredPDBs, err = m.cdbClient.DiscoverPDBs(ctx, nil)
	} else {
		// Discover specific PDBs
		explicitPDBs := m.config.GetExplicitPDBs()
		m.logger.Info("Discovering specific PDBs from CDB",
			zap.Strings("requested_pdbs", explicitPDBs))
		discoveredPDBs, err = m.cdbClient.DiscoverPDBs(ctx, explicitPDBs)
	}

	if err != nil {
		return fmt.Errorf("failed to discover PDBs: %w", err)
	}

	m.logger.Info("Discovered PDBs",
		zap.Int("count", len(discoveredPDBs)))

	// Track which PDBs we currently have
	currentPDBs := make(map[string]bool)
	for name := range m.pdbConnections {
		currentPDBs[name] = true
	}

	// Connect to newly discovered PDBs
	for _, pdb := range discoveredPDBs {
		pdbName := pdb.GetPDBName()
		if pdbName == "" {
			continue
		}

		// Check if PDB is in READ WRITE mode
		if !pdb.IsReadWrite() {
			m.logger.Warn("Skipping PDB not in READ WRITE mode",
				zap.String("pdb_name", pdbName),
				zap.String("open_mode", pdb.OpenMode.String))
			continue
		}

		// Mark this PDB as discovered
		delete(currentPDBs, pdbName)

		// Check if we already have a connection
		if existing, ok := m.pdbConnections[pdbName]; ok {
			// Verify connection is still valid
			if existing.DB != nil {
				if err := existing.DB.PingContext(ctx); err == nil {
					m.logger.Debug("Existing PDB connection still valid",
						zap.String("pdb_name", pdbName))
					continue
				}
				// Connection is invalid, close it
				m.logger.Warn("Existing PDB connection invalid, reconnecting",
					zap.String("pdb_name", pdbName))
				existing.DB.Close()
			}
		}

		// Create new connection
		if err := m.connectToPDB(ctx, &pdb); err != nil {
			m.logger.Error("Failed to connect to PDB",
				zap.String("pdb_name", pdbName),
				zap.Error(err))
			// Record the error but continue with other PDBs
			m.pdbConnections[pdbName] = &PDBConnectionState{
				PDBName:     pdbName,
				ServiceName: pdb.GetServiceName(),
				ConID:       pdb.GetConID(),
				LastError:   err,
				LastRefresh: time.Now(),
			}
			continue
		}

		m.logger.Info("Successfully connected to PDB",
			zap.String("pdb_name", pdbName),
			zap.Int64("con_id", pdb.GetConID()))
	}

	// Close connections to PDBs that are no longer discovered
	for pdbName := range currentPDBs {
		m.logger.Info("Closing connection to PDB no longer discovered",
			zap.String("pdb_name", pdbName))
		if conn, ok := m.pdbConnections[pdbName]; ok && conn.DB != nil {
			conn.DB.Close()
		}
		delete(m.pdbConnections, pdbName)
	}

	m.lastDiscovery = time.Now()
	return nil
}

// connectToPDB establishes a connection to a specific PDB
func (m *PDBManager) connectToPDB(ctx context.Context, pdb *models.DiscoveredPDB) error {
	pdbName := pdb.GetPDBName()
	serviceName := pdb.GetServiceName()

	// Build connection string for PDB
	// Format: user/password@host:port/pdb_service_name
	dataSource := fmt.Sprintf("%s/%s@%s:%d/%s",
		m.config.Username,
		m.config.Password,
		m.hostAddr,
		m.hostPort,
		serviceName)

	db, err := m.sqlOpener(dataSource)
	if err != nil {
		return fmt.Errorf("failed to open PDB connection: %w", err)
	}

	// Configure connection pool for PDB connections
	// Use smaller pool since we have multiple PDB connections
	if !m.config.DisableConnectionPool {
		maxConn := m.config.MaxOpenConnections / 2
		if maxConn < 1 {
			maxConn = 1
		}
		db.SetMaxOpenConns(maxConn)
	} else {
		db.SetMaxOpenConns(1)
	}
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetConnMaxIdleTime(30 * time.Second)

	// Verify connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping PDB: %w", err)
	}

	// Create client
	pdbClient := client.NewSQLClient(db)

	// Store connection state
	m.pdbConnections[pdbName] = &PDBConnectionState{
		PDBName:     pdbName,
		ServiceName: serviceName,
		ConID:       pdb.GetConID(),
		DB:          db,
		Client:      pdbClient,
		LastRefresh: time.Now(),
	}

	return nil
}

// GetCDBClient returns the CDB client
func (m *PDBManager) GetCDBClient() client.OracleClient {
	return m.cdbClient
}

// GetCDBDB returns the CDB database connection
func (m *PDBManager) GetCDBDB() *sql.DB {
	return m.cdbDB
}

// GetPDBConnections returns a copy of all active PDB connection states
func (m *PDBManager) GetPDBConnections() map[string]*PDBConnectionState {
	m.pdbMutex.RLock()
	defer m.pdbMutex.RUnlock()

	result := make(map[string]*PDBConnectionState)
	for name, conn := range m.pdbConnections {
		if conn.Client != nil && conn.LastError == nil {
			result[name] = conn
		}
	}
	return result
}

// GetPDBClient returns the client for a specific PDB
func (m *PDBManager) GetPDBClient(pdbName string) (client.OracleClient, error) {
	m.pdbMutex.RLock()
	defer m.pdbMutex.RUnlock()

	conn, ok := m.pdbConnections[pdbName]
	if !ok {
		return nil, fmt.Errorf("PDB not found: %s", pdbName)
	}
	if conn.Client == nil {
		return nil, fmt.Errorf("PDB client not available: %s (last error: %v)", pdbName, conn.LastError)
	}
	return conn.Client, nil
}

// GetDiscoveredPDBNames returns the names of all discovered PDBs
func (m *PDBManager) GetDiscoveredPDBNames() []string {
	m.pdbMutex.RLock()
	defer m.pdbMutex.RUnlock()

	names := make([]string, 0, len(m.pdbConnections))
	for name, conn := range m.pdbConnections {
		if conn.Client != nil && conn.LastError == nil {
			names = append(names, name)
		}
	}
	return names
}

// GetHostInfo returns the host address and port for a PDB
func (m *PDBManager) GetHostInfo(pdbName string) (string, int64, string) {
	m.pdbMutex.RLock()
	defer m.pdbMutex.RUnlock()

	conn, ok := m.pdbConnections[pdbName]
	if !ok {
		return m.hostAddr, m.hostPort, pdbName
	}
	return m.hostAddr, m.hostPort, conn.ServiceName
}

// Shutdown closes all PDB connections and the CDB connection
func (m *PDBManager) Shutdown() error {
	m.pdbMutex.Lock()
	defer m.pdbMutex.Unlock()

	var errs []error

	// Close all PDB connections
	for name, conn := range m.pdbConnections {
		if conn.DB != nil {
			if err := conn.DB.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close PDB %s: %w", name, err))
			}
		}
	}
	m.pdbConnections = make(map[string]*PDBConnectionState)

	// Close CDB connection
	if m.cdbDB != nil {
		if err := m.cdbDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close CDB: %w", err))
		}
		m.cdbDB = nil
		m.cdbClient = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during shutdown: %v", errs)
	}

	m.logger.Info("PDB Manager shutdown complete")
	return nil
}
