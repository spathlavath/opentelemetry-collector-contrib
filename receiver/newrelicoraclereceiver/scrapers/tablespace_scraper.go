// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/errors"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

type TablespaceScraper struct {
	db           *sql.DB
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	config       metadata.MetricsBuilderConfig

	isCDBCapable       *bool
	isPDBCapable       *bool
	environmentChecked bool
	currentContainer   string
	currentContainerID string
	contextChecked     bool
	detectionMutex     sync.RWMutex
}

func NewTablespaceScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, config metadata.MetricsBuilderConfig) *TablespaceScraper {
	return &TablespaceScraper{
		db:           db,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		config:       config,
	}
}

func (s *TablespaceScraper) ScrapeTablespaceMetrics(ctx context.Context) []error {
	var errors []error
	metricCount := 0

	if err := s.checkEnvironmentCapability(ctx); err != nil {
		return []error{err}
	}

	if err := s.checkCurrentContext(ctx); err != nil {
		return []error{err}
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	errors = append(errors, s.scrapeTablespaceUsageMetrics(ctx, now, &metricCount)...)
	errors = append(errors, s.scrapeGlobalNameTablespaceMetrics(ctx, now, &metricCount)...)
	errors = append(errors, s.scrapeDBIDTablespaceMetrics(ctx, now, &metricCount)...)

	if s.isCDBSupported() {
		errors = append(errors, s.scrapeCDBDatafilesOfflineTablespaceMetrics(ctx, now, &metricCount)...)

		if s.isPDBSupported() {
			errors = append(errors, s.scrapePDBDatafilesOfflineTablespaceMetrics(ctx, now, &metricCount)...)
			errors = append(errors, s.scrapePDBNonWriteTablespaceMetrics(ctx, now, &metricCount)...)
		}
	}

	s.logger.Debug("Tablespace metrics scrape completed",
		zap.Int("metrics", metricCount),
		zap.Int("errors", len(errors)))

	return errors
}

func (s *TablespaceScraper) scrapeTablespaceUsageMetrics(ctx context.Context, now pcommon.Timestamp, metricCount *int) []error {
	if !s.isAnyTablespaceMetricEnabled() {
		return nil
	}

	rows, err := s.db.QueryContext(ctx, queries.TablespaceMetricsSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing tablespace metrics query: %w", err)}
	}
	defer rows.Close()

	return s.processTablespaceUsageRows(rows, now, metricCount)
}

// isAnyTablespaceMetricEnabled checks if any tablespace usage metric is enabled
func (s *TablespaceScraper) isAnyTablespaceMetricEnabled() bool {
	return s.config.Metrics.NewrelicoracledbTablespaceSpaceConsumedBytes.Enabled ||
		s.config.Metrics.NewrelicoracledbTablespaceSpaceReservedBytes.Enabled ||
		s.config.Metrics.NewrelicoracledbTablespaceSpaceUsedPercentage.Enabled ||
		s.config.Metrics.NewrelicoracledbTablespaceIsOffline.Enabled
}

func (s *TablespaceScraper) processTablespaceUsageRows(rows *sql.Rows, now pcommon.Timestamp, metricCount *int) []error {
	var errors []error

	for rows.Next() {
		var tablespaceName string
		var usedPercent, used, size, offline float64

		err := rows.Scan(&tablespaceName, &usedPercent, &used, &size, &offline)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning tablespace row: %w", err))
			continue
		}

		if s.config.Metrics.NewrelicoracledbTablespaceSpaceConsumedBytes.Enabled {
			s.mb.RecordNewrelicoracledbTablespaceSpaceConsumedBytesDataPoint(now, int64(used), s.instanceName, tablespaceName)
		}
		if s.config.Metrics.NewrelicoracledbTablespaceSpaceReservedBytes.Enabled {
			s.mb.RecordNewrelicoracledbTablespaceSpaceReservedBytesDataPoint(now, int64(size), s.instanceName, tablespaceName)
		}
		if s.config.Metrics.NewrelicoracledbTablespaceSpaceUsedPercentage.Enabled {
			s.mb.RecordNewrelicoracledbTablespaceSpaceUsedPercentageDataPoint(now, int64(usedPercent), s.instanceName, tablespaceName)
		}
		if s.config.Metrics.NewrelicoracledbTablespaceIsOffline.Enabled {
			s.mb.RecordNewrelicoracledbTablespaceIsOfflineDataPoint(now, int64(offline), s.instanceName, tablespaceName)
		}

		*metricCount++
	}

	if err := rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating tablespace rows: %w", err))
	}

	return errors
}

func (s *TablespaceScraper) scrapeGlobalNameTablespaceMetrics(ctx context.Context, now pcommon.Timestamp, metricCount *int) []error {
	if !s.config.Metrics.NewrelicoracledbTablespaceGlobalName.Enabled {
		return nil
	}

	return s.executeSimpleTablespaceQuery(ctx, now, queries.GlobalNameTablespaceSQL,
		"global name tablespace", s.processGlobalNameRow, metricCount)
}

func (s *TablespaceScraper) scrapeDBIDTablespaceMetrics(ctx context.Context, now pcommon.Timestamp, metricCount *int) []error {
	if !s.config.Metrics.NewrelicoracledbTablespaceDbID.Enabled {
		return nil
	}

	return s.executeSimpleTablespaceQuery(ctx, now, queries.DBIDTablespaceSQL,
		"DB ID tablespace", s.processDBIDRow, metricCount)
}

func (s *TablespaceScraper) scrapeCDBDatafilesOfflineTablespaceMetrics(ctx context.Context, now pcommon.Timestamp, metricCount *int) []error {
	if !s.config.Metrics.NewrelicoracledbTablespaceOfflineCdbDatafiles.Enabled {
		return nil
	}

	return s.executeSimpleTablespaceQuery(ctx, now, queries.CDBDatafilesOfflineTablespaceSQL,
		"CDB datafiles offline tablespace", s.processCDBDatafilesOfflineRow, metricCount)
}

func (s *TablespaceScraper) executeSimpleTablespaceQuery(ctx context.Context, now pcommon.Timestamp,
	querySQL, description string, rowProcessor func(*sql.Rows, pcommon.Timestamp) error, metricCount *int) []error {

	rows, err := s.db.QueryContext(ctx, querySQL)
	if err != nil {
		return []error{fmt.Errorf("error executing %s query: %w", description, err)}
	}
	defer rows.Close()

	var errors []error

	for rows.Next() {
		if err := rowProcessor(rows, now); err != nil {
			errors = append(errors, fmt.Errorf("error processing %s row: %w", description, err))
			continue
		}
		*metricCount++
	}

	if err := rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating %s rows: %w", description, err))
	}

	return errors
}

func (s *TablespaceScraper) processGlobalNameRow(rows *sql.Rows, now pcommon.Timestamp) error {
	var tablespaceName, globalName string
	if err := rows.Scan(&tablespaceName, &globalName); err != nil {
		return err
	}

	s.mb.RecordNewrelicoracledbTablespaceGlobalNameDataPoint(now, 1, s.instanceName, tablespaceName)
	return nil
}

func (s *TablespaceScraper) processDBIDRow(rows *sql.Rows, now pcommon.Timestamp) error {
	var tablespaceName string
	var dbID int64
	if err := rows.Scan(&tablespaceName, &dbID); err != nil {
		return err
	}

	s.mb.RecordNewrelicoracledbTablespaceDbIDDataPoint(now, dbID, s.instanceName, tablespaceName)
	return nil
}

func (s *TablespaceScraper) processCDBDatafilesOfflineRow(rows *sql.Rows, now pcommon.Timestamp) error {
	var offlineCount int64
	var tablespaceName string
	if err := rows.Scan(&offlineCount, &tablespaceName); err != nil {
		return err
	}

	s.mb.RecordNewrelicoracledbTablespaceOfflineCdbDatafilesDataPoint(now, offlineCount, s.instanceName, tablespaceName)
	return nil
}

func (s *TablespaceScraper) scrapePDBDatafilesOfflineTablespaceMetrics(ctx context.Context, now pcommon.Timestamp, metricCount *int) []error {
	if !s.config.Metrics.NewrelicoracledbTablespaceOfflinePdbDatafiles.Enabled {
		return nil
	}

	var querySQL string

	if s.isConnectedToCDBRoot() {
		querySQL = queries.PDBDatafilesOfflineTablespaceSQL
	} else if s.isConnectedToPDB() {
		querySQL = queries.PDBDatafilesOfflineCurrentContainerSQL
	} else {
		return nil
	}

	rows, err := s.db.QueryContext(ctx, querySQL)
	if err != nil {
		return []error{fmt.Errorf("error executing PDB datafiles offline query: %w", err)}
	}
	defer rows.Close()

	return s.processPDBDatafilesOfflineRows(rows, now, metricCount)
}

func (s *TablespaceScraper) processPDBDatafilesOfflineRows(rows *sql.Rows, now pcommon.Timestamp, metricCount *int) []error {
	var errors []error

	for rows.Next() {
		var offlineCount int64
		var tablespaceName string

		err := rows.Scan(&offlineCount, &tablespaceName)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning PDB datafiles offline row: %w", err))
			continue
		}

		s.mb.RecordNewrelicoracledbTablespaceOfflinePdbDatafilesDataPoint(now, offlineCount, s.instanceName, tablespaceName)
		*metricCount++
	}

	if err := rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating PDB datafiles offline rows: %w", err))
	}

	return errors
}

func (s *TablespaceScraper) scrapePDBNonWriteTablespaceMetrics(ctx context.Context, now pcommon.Timestamp, metricCount *int) []error {
	if !s.config.Metrics.NewrelicoracledbTablespacePdbNonWriteMode.Enabled {
		return nil
	}

	var querySQL string

	if s.isConnectedToCDBRoot() {
		querySQL = queries.PDBNonWriteTablespaceSQL
	} else if s.isConnectedToPDB() {
		querySQL = queries.PDBNonWriteCurrentContainerSQL
	} else {
		return nil
	}

	rows, err := s.db.QueryContext(ctx, querySQL)
	if err != nil {
		return []error{fmt.Errorf("error executing PDB non-write mode query: %w", err)}
	}
	defer rows.Close()

	return s.processPDBNonWriteRows(rows, now, metricCount)
}

func (s *TablespaceScraper) processPDBNonWriteRows(rows *sql.Rows, now pcommon.Timestamp, metricCount *int) []error {
	var errors []error

	for rows.Next() {
		var tablespaceName string
		var nonWriteCount int64

		err := rows.Scan(&tablespaceName, &nonWriteCount)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning PDB non-write mode row: %w", err))
			continue
		}

		s.mb.RecordNewrelicoracledbTablespacePdbNonWriteModeDataPoint(now, nonWriteCount, s.instanceName, tablespaceName)
		*metricCount++
	}

	if err := rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating PDB non-write mode rows: %w", err))
	}

	return errors
}

func (s *TablespaceScraper) checkEnvironmentCapability(ctx context.Context) error {
	s.detectionMutex.Lock()
	defer s.detectionMutex.Unlock()

	if s.environmentChecked {
		return nil
	}

	var isCDB int64
	err := s.db.QueryRowContext(ctx, queries.CheckCDBFeatureSQL).Scan(&isCDB)
	if err != nil {
		if errors.IsPermanentError(err) {
			cdbCapable := false
			pdbCapable := false
			s.isCDBCapable = &cdbCapable
			s.isPDBCapable = &pdbCapable
			s.environmentChecked = true
			return nil
		}
		return errors.NewQueryError("cdb_capability_check", queries.CheckCDBFeatureSQL, err,
			map[string]interface{}{"instance": s.instanceName})
	}

	cdbCapable := isCDB == 1
	s.isCDBCapable = &cdbCapable

	if cdbCapable {
		var pdbCount int
		err = s.db.QueryRowContext(ctx, queries.CheckPDBCapabilitySQL).Scan(&pdbCount)
		if err != nil {
			pdbCapable := false
			s.isPDBCapable = &pdbCapable
		} else {
			pdbCapable := pdbCount > 0
			s.isPDBCapable = &pdbCapable
		}
	} else {
		pdbCapable := false
		s.isPDBCapable = &pdbCapable
	}

	s.environmentChecked = true

	return nil
}

// isCDBSupported returns true if the database supports CDB features
func (s *TablespaceScraper) isCDBSupported() bool {
	s.detectionMutex.RLock()
	defer s.detectionMutex.RUnlock()
	return s.isCDBCapable != nil && *s.isCDBCapable
}

// isPDBSupported returns true if the database supports PDB features
func (s *TablespaceScraper) isPDBSupported() bool {
	s.detectionMutex.RLock()
	defer s.detectionMutex.RUnlock()
	return s.isPDBCapable != nil && *s.isPDBCapable
}

func (s *TablespaceScraper) checkCurrentContext(ctx context.Context) error {
	s.detectionMutex.Lock()
	defer s.detectionMutex.Unlock()

	if s.contextChecked {
		return nil
	}

	var containerName, containerID string
	err := s.db.QueryRowContext(ctx, queries.CheckCurrentContainerSQL).Scan(&containerName, &containerID)
	if err != nil {
		return errors.NewQueryError("container_context_check", queries.CheckCurrentContainerSQL, err,
			map[string]interface{}{"instance": s.instanceName})
	}

	s.currentContainer = containerName
	s.currentContainerID = containerID
	s.contextChecked = true

	return nil
}

func (s *TablespaceScraper) isConnectedToCDBRoot() bool {
	s.detectionMutex.RLock()
	defer s.detectionMutex.RUnlock()
	return s.currentContainer == "CDB$ROOT"
}

func (s *TablespaceScraper) isConnectedToPDB() bool {
	s.detectionMutex.RLock()
	defer s.detectionMutex.RUnlock()
	return s.currentContainer != "" && s.currentContainer != "CDB$ROOT"
}
