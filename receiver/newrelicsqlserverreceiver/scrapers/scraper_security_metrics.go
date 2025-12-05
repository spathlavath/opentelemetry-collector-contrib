// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
package scrapers

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/internal/metadata"
)

// SecurityScraper handles SQL Server security-level metrics collection
type SecurityScraper struct {
	client        client.SQLServerClient
	logger        *zap.Logger
	mb            *metadata.MetricsBuilder
	engineEdition int
}

// NewSecurityScraper creates a new security scraper
func NewSecurityScraper(sqlClient client.SQLServerClient, logger *zap.Logger, engineEdition int, mb *metadata.MetricsBuilder) *SecurityScraper {
	return &SecurityScraper{
		client:        sqlClient,
		logger:        logger,
		engineEdition: engineEdition,
		mb:            mb,
	}
}

// ScrapeSecurityPrincipalsMetrics scrapes server principals count metrics
func (s *SecurityScraper) ScrapeSecurityPrincipalsMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server security principals metrics")

	results, err := s.client.QuerySecurityPrincipals(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute security principals query", zap.Error(err))
		return fmt.Errorf("failed to execute security principals query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from security principals query")
		return fmt.Errorf("no results returned from security principals query")
	}

	result := results[0]
	if result.ServerPrincipalsCount == nil {
		s.logger.Error("Security principals metric is null - invalid query result")
		return fmt.Errorf("security principals metric is null in query result")
	}

	timestamp := pcommon.NewTimestampFromTime(time.Now())
	s.mb.RecordSqlserverSecurityPrincipalsCountDataPoint(timestamp, *result.ServerPrincipalsCount)

	s.logger.Debug("Successfully scraped security principals metrics")
	return nil
}

// ScrapeSecurityRoleMembersMetrics scrapes server role membership count metrics
func (s *SecurityScraper) ScrapeSecurityRoleMembersMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server security role members metrics")

	results, err := s.client.QuerySecurityRoleMembers(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute security role members query", zap.Error(err))
		return fmt.Errorf("failed to execute security role members query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from security role members query")
		return fmt.Errorf("no results returned from security role members query")
	}

	result := results[0]
	if result.ServerRoleMembersCount == nil {
		s.logger.Error("Security role members metric is null - invalid query result")
		return fmt.Errorf("security role members metric is null in query result")
	}

	timestamp := pcommon.NewTimestampFromTime(time.Now())
	s.mb.RecordSqlserverSecurityRoleMembersCountDataPoint(timestamp, *result.ServerRoleMembersCount)

	s.logger.Debug("Successfully scraped security role members metrics")
	return nil
}
