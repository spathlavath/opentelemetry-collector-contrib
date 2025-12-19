// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// showing relationships between roles and their members.
//
// Database Role Membership Metrics Overview:
//
// Database role membership represents the security relationships within a SQL Server database,
// showing which users, roles, or other principals belong to which database roles. This is
// critical for:
//
// 1. Security Auditing: Understanding who has what permissions through role membership
// 2. Compliance Reporting: Documenting access control structures for audit requirements
// 3. Access Management: Tracking role assignments and permission inheritance
// 4. Security Analysis: Identifying potential privilege escalation paths
//
// Role Membership Structure:
//
// In SQL Server, database roles can contain:
// - Database Users (SQL_USER, WINDOWS_USER, etc.)
// - Other Database Roles (nested role membership)
// - Application Roles (APPLICATION_ROLE)
// - Certificate-mapped Users (CERTIFICATE_MAPPED_USER)
// - Asymmetric Key-mapped Users (ASYMMETRIC_KEY_MAPPED_USER)
//
// Key Relationships:
// - Role → Member: A role can have multiple members
// - Member → Roles: A member can belong to multiple roles
// - Nested Roles: Roles can be members of other roles
// - Permission Inheritance: Members inherit permissions from their roles
//
// Query Source:
// Based on the provided query that joins sys.database_role_members with
// sys.database_principals to show role-member relationships:
// ```sql
// SELECT roles.name AS role_name, members.name AS member_name
// FROM sys.database_role_members AS drm
// JOIN sys.database_principals AS roles ON drm.role_principal_id = roles.principal_id
// JOIN sys.database_principals AS members ON drm.member_principal_id = members.principal_id
// ORDER BY role_name, member_name;
// ```
//
// Monitoring Use Cases:
// - Track role membership changes over time
// - Identify users with elevated privileges
// - Monitor nested role assignments
// - Audit role-based access control configuration
// - Detect unauthorized role assignments
// - Support compliance requirements (SOX, PCI-DSS, etc.)
//
// Security Considerations:
// - Focuses on role-member relationships, not sensitive authentication data
// - Excludes fixed database roles by default (configurable)
// - Provides visibility into custom security configurations
// - Supports principle of least privilege monitoring
//
// Engine Compatibility:
// - Standard SQL Server: Full access to all role membership information
// - Azure SQL Database: Database-scoped role membership
// - Azure SQL Managed Instance: Complete functionality with enterprise features
package models

// DatabaseRoleMembershipSummary represents aggregated statistics about role memberships
// This model provides summary metrics for monitoring and alerting on role membership patterns
type DatabaseRoleMembershipSummary struct {
	// DatabaseName is the name of the database
	DatabaseName string `db:"database_name"`

	// TotalMemberships is the total count of role membership relationships
	// Each role-member pair counts as one membership
	TotalMemberships *int64 `db:"total_memberships" metric_name:"sqlserver.database.role.memberships.total" source_type:"gauge"`

	// UniqueRoles is the count of distinct roles that have members
	// Roles without members are not counted
	UniqueRoles *int64 `db:"unique_roles" metric_name:"sqlserver.database.role.roles.withMembers" source_type:"gauge"`

	// UniqueMembers is the count of distinct principals that are members of roles
	// Principals without role memberships are not counted
	UniqueMembers *int64 `db:"unique_members" metric_name:"sqlserver.database.role.members.unique" source_type:"gauge"`

	// CustomRoleMemberships is the count of memberships in user-defined roles
	// Excludes fixed database roles to focus on custom security configuration
	CustomRoleMemberships *int64 `db:"custom_role_memberships" metric_name:"sqlserver.database.role.memberships.custom" source_type:"gauge"`

	// NestedRoleMemberships is the count of role-to-role memberships
	// When one role is a member of another role
	NestedRoleMemberships *int64 `db:"nested_role_memberships" metric_name:"sqlserver.database.role.memberships.nested" source_type:"gauge"`

	// UserRoleMemberships is the count of user-to-role memberships
	// Direct user assignments to roles
	UserRoleMemberships *int64 `db:"user_role_memberships" metric_name:"sqlserver.database.role.memberships.users" source_type:"gauge"`
}

// DatabaseRoleActivity represents role membership activity and changes
// This model tracks the lifecycle and activity of role memberships
type DatabaseRoleActivity struct {
	// DatabaseName is the name of the database
	DatabaseName string `db:"database_name"`

	// ActiveMemberships is the count of currently active role memberships
	ActiveMemberships *int64 `db:"active_memberships" metric_name:"sqlserver.database.role.memberships.active" source_type:"gauge"`

	// EmptyRoles is the count of roles that have no members
	// Useful for identifying unused or orphaned roles
	EmptyRoles *int64 `db:"empty_roles" metric_name:"sqlserver.database.role.roles.empty" source_type:"gauge"`

	// HighPrivilegeMembers is the count of members in high-privilege roles
	// Members of roles like db_owner, db_securityadmin, etc.
	HighPrivilegeMembers *int64 `db:"high_privilege_members" metric_name:"sqlserver.database.role.members.highPrivilege" source_type:"gauge"`

	// ApplicationRoleMembers is the count of application role memberships
	// Special focus on application roles which require activation
	ApplicationRoleMembers *int64 `db:"app_role_members" metric_name:"sqlserver.database.role.members.applicationRoles" source_type:"gauge"`

	// CrossRoleMembers is the count of principals that belong to multiple roles
	// Useful for identifying principals with potentially excessive privileges
	CrossRoleMembers *int64 `db:"cross_role_members" metric_name:"sqlserver.database.role.members.crossRole" source_type:"gauge"`
}

// DatabaseRolePermissionMatrix represents role-based permission analysis
// Retaining memberCount and riskLevel as they are aggregated per-role summaries
type DatabaseRolePermissionMatrix struct {
	// DatabaseName is the name of the database
	DatabaseName string `db:"database_name"`

	// RoleName is the role being analyzed (kept as required attribute for metrics)
	RoleName string `db:"role_name"`

	// PermissionScope indicates the scope of permissions (kept as required attribute for metrics)
	PermissionScope string `db:"permission_scope"`

	// MemberCount is the number of members in this role
	MemberCount *int64 `db:"member_count" metric_name:"sqlserver.database.role.permission.memberCount" source_type:"gauge"`

	// RiskLevel indicates the risk level of this role based on its permissions
	// 1=Low, 2=Medium, 3=High, 4=Critical
	RiskLevel *int64 `db:"risk_level" metric_name:"sqlserver.database.role.permission.riskLevel" source_type:"gauge"`
}
