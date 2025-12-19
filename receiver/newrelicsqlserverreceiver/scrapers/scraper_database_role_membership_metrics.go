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
func NewDatabaseRoleMembershipScraper(logger *zap.Logger, connection SQLConnectionInterface, mb *metadata.MetricsBuilder, engineEdition int) *DatabaseRoleMembershipScraper {
	return &DatabaseRoleMembershipScraper{
		connection:    connection,
		logger:        logger,
		mb:            mb,
		engineEdition: engineEdition,
	}
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
// Keeping memberCount and riskLevel metrics as they are aggregated summaries
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

// processDatabaseRoleMembershipSummaryMetrics processes summary metrics and adds them to the scope
func (s *DatabaseRoleMembershipScraper) processDatabaseRoleMembershipSummaryMetrics(result models.DatabaseRoleMembershipSummary) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	databaseName := ""
	if result.DatabaseName != "" {
		databaseName = result.DatabaseName
	}

	if result.TotalMemberships != nil {
		s.mb.RecordSqlserverDatabaseRoleMembershipsTotalDataPoint(now, *result.TotalMemberships, databaseName)
	}
	if result.UniqueRoles != nil {
		s.mb.RecordSqlserverDatabaseRoleRolesWithMembersDataPoint(now, *result.UniqueRoles, databaseName)
	}
	if result.UniqueMembers != nil {
		s.mb.RecordSqlserverDatabaseRoleMembersUniqueDataPoint(now, *result.UniqueMembers, databaseName)
	}
	if result.CustomRoleMemberships != nil {
		s.mb.RecordSqlserverDatabaseRoleMembershipsCustomDataPoint(now, *result.CustomRoleMemberships, databaseName)
	}
	if result.NestedRoleMemberships != nil {
		s.mb.RecordSqlserverDatabaseRoleMembershipsNestedDataPoint(now, *result.NestedRoleMemberships, databaseName)
	}
	if result.UserRoleMemberships != nil {
		s.mb.RecordSqlserverDatabaseRoleMembershipsUsersDataPoint(now, *result.UserRoleMemberships, databaseName)
	}

	s.logger.Debug("Recorded database role membership summary metrics",
		zap.String("database_name", result.DatabaseName))

	return nil
}

// processDatabaseRoleActivityMetrics processes activity metrics and adds them to the scope
func (s *DatabaseRoleMembershipScraper) processDatabaseRoleActivityMetrics(result models.DatabaseRoleActivity) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	databaseName := ""
	if result.DatabaseName != "" {
		databaseName = result.DatabaseName
	}

	if result.ActiveMemberships != nil {
		s.mb.RecordSqlserverDatabaseRoleMembershipsActiveDataPoint(now, *result.ActiveMemberships, databaseName)
	}
	if result.EmptyRoles != nil {
		s.mb.RecordSqlserverDatabaseRoleRolesEmptyDataPoint(now, *result.EmptyRoles, databaseName)
	}
	if result.HighPrivilegeMembers != nil {
		s.mb.RecordSqlserverDatabaseRoleMembersHighPrivilegeDataPoint(now, *result.HighPrivilegeMembers, databaseName)
	}
	if result.ApplicationRoleMembers != nil {
		s.mb.RecordSqlserverDatabaseRoleMembersApplicationRolesDataPoint(now, *result.ApplicationRoleMembers, databaseName)
	}
	if result.CrossRoleMembers != nil {
		s.mb.RecordSqlserverDatabaseRoleMembersCrossRoleDataPoint(now, *result.CrossRoleMembers, databaseName)
	}

	s.logger.Debug("Recorded database role activity metrics",
		zap.String("database_name", result.DatabaseName))

	return nil
}

// processDatabaseRolePermissionMatrixMetrics processes permission matrix metrics and adds them to the scope
func (s *DatabaseRoleMembershipScraper) processDatabaseRolePermissionMatrixMetrics(result models.DatabaseRolePermissionMatrix) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	databaseName := ""
	if result.DatabaseName != "" {
		databaseName = result.DatabaseName
	}

	roleName := ""
	if result.RoleName != "" {
		roleName = result.RoleName
	}

	permissionScope := ""
	if result.PermissionScope != "" {
		permissionScope = result.PermissionScope
	}

	if result.MemberCount != nil {
		s.mb.RecordSqlserverDatabaseRolePermissionMemberCountDataPoint(now, *result.MemberCount, databaseName, roleName, permissionScope)
	}
	if result.RiskLevel != nil {
		s.mb.RecordSqlserverDatabaseRolePermissionRiskLevelDataPoint(now, *result.RiskLevel, databaseName, roleName, permissionScope)
	}

	s.logger.Debug("Recorded database role permission matrix metrics",
		zap.String("database_name", result.DatabaseName),
		zap.String("role_name", result.RoleName))

	return nil
}
