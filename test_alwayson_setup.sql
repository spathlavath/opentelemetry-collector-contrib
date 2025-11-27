-- Test script to verify Always On Availability Group setup
-- This script checks if Always On AGs are enabled and if any AGs exist

-- Check if Always On is enabled
SELECT 
    SERVERPROPERTY('IsHadrEnabled') AS IsAlwaysOnEnabled,
    SERVERPROPERTY('HadrManagerStatus') AS HadrManagerStatus;

-- Check for existing Availability Groups
SELECT 
    ag.name AS AvailabilityGroupName,
    ag.group_id,
    ag.primary_replica,
    ag.automated_backup_preference_desc,
    ag.failure_condition_level,
    ag.health_check_timeout
FROM sys.availability_groups ag;

-- Check for Availability Group Replicas
SELECT 
    agr.replica_server_name,
    agr.endpoint_url,
    agr.availability_mode_desc,
    agr.failover_mode_desc,
    agr.primary_role_allow_connections_desc,
    agr.secondary_role_allow_connections_desc
FROM sys.availability_replicas agr
JOIN sys.availability_groups ag ON agr.group_id = ag.group_id;

-- Check for Availability Group Database Replica States (this is where the redo queue metrics come from)
SELECT 
    agdrs.replica_server_name,
    agdrs.database_name,
    agdrs.is_primary_replica,
    agdrs.synchronization_state_desc,
    agdrs.log_send_queue_size,
    agdrs.redo_queue_size,
    agdrs.redo_rate,
    agdrs.last_sent_time,
    agdrs.last_received_time,
    agdrs.last_hardened_time,
    agdrs.last_redone_time,
    agdrs.last_commit_time
FROM sys.dm_hadr_database_replica_states agdrs
JOIN sys.availability_replicas agr ON agdrs.replica_id = agr.replica_id
JOIN sys.availability_groups ag ON agr.group_id = ag.group_id;