// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/scrapers"

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// DatabaseInfoScraper collects Oracle database version and hosting environment info
type DatabaseInfoScraper struct {
	client client.OracleClient
	mb     *metadata.MetricsBuilder
	logger *zap.Logger
	config metadata.MetricsBuilderConfig

	// Cache static info to avoid repeated DB queries
	cachedInfo      *DatabaseInfo
	cacheValidUntil time.Time
	cacheDuration   time.Duration
	cacheMutex      sync.RWMutex // Thread-safe cache access
}

// DatabaseInfo holds database and system environment details
type DatabaseInfo struct {
	// DB version info from Oracle instance
	Version     string
	VersionFull string
	Edition     string
	Compatible  string

	// System environment
	Architecture    string // amd64, arm64, 386
	OperatingSystem string // linux, windows, darwin
}

// NewDatabaseInfoScraper creates a new database info scraper
func NewDatabaseInfoScraper(c client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, config metadata.MetricsBuilderConfig) *DatabaseInfoScraper {
	return &DatabaseInfoScraper{
		client:        c,
		mb:            mb,
		logger:        logger,
		config:        config,
		cacheDuration: 1 * time.Hour, // Version info rarely changes
	}
}

func (s *DatabaseInfoScraper) ScrapeDatabaseInfo(ctx context.Context) []error {
	var errs []error

	if !s.config.Metrics.NewrelicoracledbDatabaseInfo.Enabled {
		return errs
	}

	if err := s.ensureCacheValid(ctx); err != nil {
		errs = append(errs, err)
		return errs
	}

	s.cacheMutex.RLock()
	cachedInfo := s.cachedInfo
	s.cacheMutex.RUnlock()

	if cachedInfo == nil {
		return errs
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	s.mb.RecordNewrelicoracledbDatabaseInfoDataPoint(
		now,
		int64(1),
		cachedInfo.Version,
		cachedInfo.VersionFull,
		cachedInfo.Edition,
		cachedInfo.Compatible,
	)

	return errs
}

func (s *DatabaseInfoScraper) ScrapeHostingInfo(ctx context.Context) []error {
	var errs []error

	if !s.config.Metrics.NewrelicoracledbHostingInfo.Enabled {
		return errs
	}

	if err := s.ensureCacheValid(ctx); err != nil {
		errs = append(errs, err)
		return errs
	}

	s.cacheMutex.RLock()
	cachedInfo := s.cachedInfo
	s.cacheMutex.RUnlock()

	if cachedInfo == nil {
		return errs
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	s.mb.RecordNewrelicoracledbHostingInfoDataPoint(
		now,
		int64(1),
		cachedInfo.Architecture,
		cachedInfo.OperatingSystem,
	)

	return errs
}

// ScrapeDatabaseRole scrapes the database role information (PRIMARY, STANDBY, etc.)
func (s *DatabaseInfoScraper) ScrapeDatabaseRole(ctx context.Context) []error {
	var errs []error

	if !s.config.Metrics.NewrelicoracledbDatabaseRole.Enabled {
		return errs
	}

	role, err := s.client.QueryDatabaseRole(ctx)
	if err != nil {
		errs = append(errs, err)
		return errs
	}

	if role != nil {
		roleStr := "UNKNOWN"
		if role.DatabaseRole.Valid {
			roleStr = role.DatabaseRole.String
		}

		openMode := "UNKNOWN"
		if role.OpenMode.Valid {
			openMode = role.OpenMode.String
		}

		protectionMode := "UNKNOWN"
		if role.ProtectionMode.Valid {
			protectionMode = role.ProtectionMode.String
		}

		protectionLevel := "UNKNOWN"
		if role.ProtectionLevel.Valid {
			protectionLevel = role.ProtectionLevel.String
		}

		now := pcommon.NewTimestampFromTime(time.Now())
		s.mb.RecordNewrelicoracledbDatabaseRoleDataPoint(
			now,
			int64(1),
			roleStr,
			openMode,
			protectionMode,
			protectionLevel,
		)
	}

	return errs
}

func extractCloudProviderForOTEL(hostingProvider string) string {
	switch hostingProvider {
	case "aws", "azure", "gcp", "oci":
		return hostingProvider
	default:
		return ""
	}
}

func (s *DatabaseInfoScraper) ensureCacheValid(ctx context.Context) error {
	now := time.Now()

	s.cacheMutex.RLock()
	if s.cachedInfo != nil && now.Before(s.cacheValidUntil) {
		s.cacheMutex.RUnlock()
		return nil
	}
	s.cacheMutex.RUnlock()

	s.cacheMutex.Lock()
	defer s.cacheMutex.Unlock()

	if s.cachedInfo != nil && now.Before(s.cacheValidUntil) {
		return nil
	}

	s.logger.Debug("Refreshing database info cache")
	return s.refreshCacheUnsafe(ctx)
}

func (s *DatabaseInfoScraper) refreshCacheUnsafe(ctx context.Context) error {
	s.logger.Debug("Executing database info query")

	metrics, err := s.client.QueryDatabaseInfo(ctx)
	if err != nil {
		return err
	}

	return s.processDatabaseInfoMetrics(metrics)
}

func (s *DatabaseInfoScraper) processDatabaseInfoMetrics(metrics []models.DatabaseInfoMetric) error {
	for _, metric := range metrics {
		cleanVersion := extractVersionFromFull(metric.VersionFull.String)
		detectedEdition := detectEditionFromVersion(metric.VersionFull.String)

		s.cachedInfo = &DatabaseInfo{
			Version:         cleanVersion,
			VersionFull:     metric.VersionFull.String,
			Edition:         detectedEdition,
			Compatible:      cleanVersion,
			Architecture:    runtime.GOARCH,
			OperatingSystem: runtime.GOOS,
		}

		s.cacheValidUntil = time.Now().Add(s.cacheDuration)
		break
	}

	return nil
}

func extractVersionFromFull(versionFull string) string {
	if versionFull == "" {
		return "unknown"
	}

	versionParts := strings.Split(strings.TrimSpace(versionFull), ".")
	if len(versionParts) >= 2 {
		return strings.Join(versionParts[:2], ".")
	}

	if len(versionParts) == 1 {
		return versionParts[0]
	}

	return versionFull
}

func detectEditionFromVersion(versionFull string) string {
	if versionFull == "" {
		return "unknown"
	}

	majorVersion := strings.Split(versionFull, ".")[0]

	if majorVersion == "23" {
		return "free"
	}

	return "standard"
}
