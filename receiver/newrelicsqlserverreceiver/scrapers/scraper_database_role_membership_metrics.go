// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// DatabaseRoleMembershipScraper implements scraping for database role membership metrics
// This scraper provides comprehensive monitoring of database role-member relationships
// for security auditing and access control visibility
type DatabaseRoleMembershipScraper struct {
	logger        *zap.Logger
	connection    SQLConnectionInterface
	mb            *metadata.MetricsBuilder
	engineEdition int
}

// NewDatabaseRoleMembershipScraper creates a new scraper for database role membership metrics
// This constructor initializes the scraper with necessary dependencies for metric collection
func NewDatabaseRoleMembershipScraper(logger *zap.Logger, connection SQLConnectionInterface, engineEdition int, mb *metadata.MetricsBuilder) *DatabaseRoleMembershipScraper {
	return &DatabaseRoleMembershipScraper{
		connection:    connection,
		logger:        logger,
		mb:            mb,
		engineEdition: engineEdition,
	}
}

// ScrapeDatabaseRoleMembershipMetrics collects individual database role membership metrics using engine-specific queries
func (s *DatabaseRoleMembershipScraper) ScrapeDatabaseRoleMembershipMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server database role membership metrics")

	// Get the appropriate query for this engine edition
	query, found := s.getQueryForMetric("database_role_membership")
	if !found {
		return fmt.Errorf("no database role membership query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing database role membership query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.DatabaseRoleMembershipMetrics
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute database role membership query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))
		return fmt.Errorf("failed to query database role membership: %w", err)
	}

	s.logger.Debug("Database role membership query completed",
		zap.Int("result_count", len(results)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	// Process each role membership result
	for _, result := range results {
		if err := s.processDatabaseRoleMembershipMetrics(result); err != nil {
			s.logger.Error("Failed to process database role membership metrics",
				zap.Error(err),
				zap.String("role_name", result.RoleName),
				zap.String("member_name", result.MemberName),
				zap.String("database_name", result.DatabaseName))
			return err
		}
	}

	return nil
}

// ScrapeDatabaseRoleMembershipSummaryMetrics collects aggregated database role membership statistics
func (s *DatabaseRoleMembershipScraper) ScrapeDatabaseRoleMembershipSummaryMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server database role membership summary metrics")

	// Get the appropriate summary query for this engine edition
	query, found := s.getQueryForMetric("database_role_membership_summary")
	if !found {
		return fmt.Errorf("no database role membership summary query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing database role membership summary query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.DatabaseRoleMembershipSummary
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute database role membership summary query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))
		return fmt.Errorf("failed to query database role membership summary: %w", err)
	}

	s.logger.Debug("Database role membership summary query completed",
		zap.Int("result_count", len(results)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	// Process each summary result
	for _, result := range results {
		if err := s.processDatabaseRoleMembershipSummaryMetrics(result); err != nil {
			s.logger.Error("Failed to process database role membership summary metrics",
				zap.Error(err),
				zap.String("database_name", result.DatabaseName))
			return err
		}
	}

	return nil
}

// ScrapeDatabaseRoleHierarchyMetrics collects database role hierarchy and nesting information
func (s *DatabaseRoleMembershipScraper) ScrapeDatabaseRoleHierarchyMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server database role hierarchy metrics")

	// Get the appropriate hierarchy query for this engine edition
	query, found := s.getQueryForMetric("database_role_hierarchy")
	if !found {
		return fmt.Errorf("no database role hierarchy query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing database role hierarchy query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.DatabaseRoleHierarchy
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute database role hierarchy query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))
		return fmt.Errorf("failed to query database role hierarchy: %w", err)
	}

	s.logger.Debug("Database role hierarchy query completed",
		zap.Int("result_count", len(results)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	// Process each hierarchy result
	for _, result := range results {
		if err := s.processDatabaseRoleHierarchyMetrics(result); err != nil {
			s.logger.Error("Failed to process database role hierarchy metrics",
				zap.Error(err),
				zap.String("parent_role", result.ParentRoleName),
				zap.String("child_role", result.ChildRoleName),
				zap.String("database_name", result.DatabaseName))
			return err
		}
	}

	return nil
}

// ScrapeDatabaseRoleActivityMetrics collects database role activity and usage metrics
func (s *DatabaseRoleMembershipScraper) ScrapeDatabaseRoleActivityMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server database role activity metrics")

	// Get the appropriate activity query for this engine edition
	query, found := s.getQueryForMetric("database_role_activity")
	if !found {
		return fmt.Errorf("no database role activity query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing database role activity query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.DatabaseRoleActivity
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute database role activity query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))
		return fmt.Errorf("failed to query database role activity: %w", err)
	}

	s.logger.Debug("Database role activity query completed",
		zap.Int("result_count", len(results)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	// Process each activity result
	for _, result := range results {
		if err := s.processDatabaseRoleActivityMetrics(result); err != nil {
			s.logger.Error("Failed to process database role activity metrics",
				zap.Error(err),
				zap.String("database_name", result.DatabaseName))
			return err
		}
	}

	return nil
}

// ScrapeDatabaseRolePermissionMatrixMetrics collects database role permission analysis
func (s *DatabaseRoleMembershipScraper) ScrapeDatabaseRolePermissionMatrixMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server database role permission matrix metrics")

	// Get the appropriate permission matrix query for this engine edition
	query, found := s.getQueryForMetric("database_role_permission_matrix")
	if !found {
		return fmt.Errorf("no database role permission matrix query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing database role permission matrix query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.DatabaseRolePermissionMatrix
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute database role permission matrix query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))
		return fmt.Errorf("failed to query database role permission matrix: %w", err)
	}

	s.logger.Debug("Database role permission matrix query completed",
		zap.Int("result_count", len(results)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	// Process each permission matrix result
	for _, result := range results {
		if err := s.processDatabaseRolePermissionMatrixMetrics(result); err != nil {
			s.logger.Error("Failed to process database role permission matrix metrics",
				zap.Error(err),
				zap.String("role_name", result.RoleName),
				zap.String("database_name", result.DatabaseName))
			return err
		}
	}

	return nil
}

// getQueryForMetric retrieves the appropriate query for a metric based on engine edition with Default fallback
func (s *DatabaseRoleMembershipScraper) getQueryForMetric(metricName string) (string, bool) {
	query, found := queries.GetQueryForMetric(queries.DatabaseRoleMembershipQueries, metricName, s.engineEdition)
	if found {
		s.logger.Debug("Using query for metric",
			zap.String("metric_name", metricName),
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))
	}
	return query, found
}

// processDatabaseRoleMembershipMetrics processes individual role membership metrics using MetricsBuilder
func (s *DatabaseRoleMembershipScraper) processDatabaseRoleMembershipMetrics(result models.DatabaseRoleMembershipMetrics) error {
	// Currently no individual membership metrics are defined in metadata.yaml
	// This method is kept for future extensibility
	return nil
}

// processDatabaseRoleMembershipSummaryMetrics processes summary metrics using MetricsBuilder
func (s *DatabaseRoleMembershipScraper) processDatabaseRoleMembershipSummaryMetrics(result models.DatabaseRoleMembershipSummary) error {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	if result.TotalMemberships != nil {
		s.mb.RecordSqlserverDatabaseRoleMembershipsTotalDataPoint(timestamp, *result.TotalMemberships)
	}
	if result.UniqueMembers != nil {
		s.mb.RecordSqlserverDatabaseRoleMembersUniqueDataPoint(timestamp, *result.UniqueMembers)
	}

	return nil
}

// processDatabaseRoleHierarchyMetrics processes hierarchy metrics using MetricsBuilder
func (s *DatabaseRoleMembershipScraper) processDatabaseRoleHierarchyMetrics(result models.DatabaseRoleHierarchy) error {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	if result.NestingLevel != nil {
		s.mb.RecordSqlserverDatabaseRoleNestingLevelDataPoint(timestamp, *result.NestingLevel)
	}
	if result.EffectivePermissions != nil {
		s.mb.RecordSqlserverDatabaseRolePermissionsInheritedDataPoint(timestamp, *result.EffectivePermissions)
	}

	return nil
}

// processDatabaseRoleActivityMetrics processes activity metrics using MetricsBuilder
func (s *DatabaseRoleMembershipScraper) processDatabaseRoleActivityMetrics(result models.DatabaseRoleActivity) error {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	if result.ActiveMemberships != nil {
		s.mb.RecordSqlserverDatabaseRoleMembershipsActiveDataPoint(timestamp, *result.ActiveMemberships)
	}

	return nil
}

// processDatabaseRolePermissionMatrixMetrics processes permission matrix metrics using MetricsBuilder
func (s *DatabaseRoleMembershipScraper) processDatabaseRolePermissionMatrixMetrics(result models.DatabaseRolePermissionMatrix) error {
	// Currently no permission matrix metrics are defined in metadata.yaml
	// This method is kept for future extensibility
	return nil
}
