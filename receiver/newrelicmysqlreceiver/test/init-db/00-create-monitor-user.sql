-- Create monitor user for OpenTelemetry collector
CREATE USER IF NOT EXISTS 'monitor'@'%' IDENTIFIED BY 'monitorpass';

-- Grant necessary privileges for monitoring
GRANT SELECT, PROCESS, REPLICATION CLIENT ON *.* TO 'monitor'@'%';

-- Grant access to performance_schema for detailed metrics
GRANT SELECT ON performance_schema.* TO 'monitor'@'%';

-- Flush privileges
FLUSH PRIVILEGES;
