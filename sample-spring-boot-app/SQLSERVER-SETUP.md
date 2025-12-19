# SQL Server Connection Setup Guide

This guide explains how to update your Spring Boot application to connect to SQL Server instead of H2.

## Changes Made

### 1. Updated pom.xml
- Replaced H2 database dependency with SQL Server JDBC driver
- Added `mssql-jdbc` dependency

### 2. Updated application.properties
- Changed database connection to SQL Server at `74.225.3.34:1433`
- Connected to `AdventureWorks2022` database
- Using credentials: `newrelicOtel` / `newrelic@123`
- Changed Hibernate dialect to `SQLServerDialect`
- Set `ddl-auto=update` (will create tables if they don't exist)

## Transfer Updated Files to Windows

### Option 1: Using SCP (From Mac)

```bash
cd /Users/pkulkarni/workspace/mssql-otel/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/sample-spring-boot-app

# Transfer pom.xml
scp pom.xml username@windows-server:C:/Users/mssql/sample-spring-boot-app/

# Transfer application.properties
scp src/main/resources/application.properties username@windows-server:C:/Users/mssql/sample-spring-boot-app/src/main/resources/
```

### Option 2: Manual Copy (On Windows)

Create/edit these files on Windows:

#### pom.xml
Replace the H2 dependency (around line 38-43) with:
```xml
<!-- SQL Server JDBC Driver -->
<dependency>
    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>mssql-jdbc</artifactId>
    <scope>runtime</scope>
</dependency>
```

#### application.properties
Replace entire content with:
```properties
# Application Configuration
spring.application.name=demo-apm-app
server.port=8080

# SQL Server Database Configuration
spring.datasource.url=jdbc:sqlserver://74.225.3.34:1433;databaseName=AdventureWorks2022;encrypt=true;trustServerCertificate=true
spring.datasource.username=newrelicOtel
spring.datasource.password=newrelic@123
spring.datasource.driverClassName=com.microsoft.sqlserver.jdbc.SQLServerDriver

# JPA Configuration
spring.jpa.database-platform=org.hibernate.dialect.SQLServerDialect
spring.jpa.hibernate.ddl-auto=update
spring.jpa.show-sql=true
spring.jpa.properties.hibernate.format_sql=true

# Actuator Configuration
management.endpoints.web.exposure.include=health,info,metrics
management.endpoint.health.show-details=always
```

## Rebuild the Application on Windows

```cmd
cd C:\Users\mssql\sample-spring-boot-app

# Clean and rebuild
mvnd clean package
```

This will:
1. Download the SQL Server JDBC driver
2. Compile the application with new dependencies
3. Create a new JAR file with SQL Server support

## Run the Application

```cmd
.\start-with-apm.bat
```

## Verify SQL Server Connection

When the application starts, you should see logs like:

```
HikariPool-1 - Starting...
HikariPool-1 - Start completed.
Hibernate: create table users (...)
```

If successful, the application will:
1. Connect to SQL Server at `74.225.3.34:1433`
2. Use the `AdventureWorks2022` database
3. Create a `users` table (if it doesn't exist)
4. Be ready to accept API requests

## Test the Connection

```powershell
# Health check
curl http://localhost:8080/api/health

# Create a user (will be stored in SQL Server)
curl -X POST http://localhost:8080/api/users -H "Content-Type: application/json" -d '{\"name\":\"Test User\",\"email\":\"test@example.com\",\"role\":\"Admin\"}'

# Get all users (from SQL Server)
curl http://localhost:8080/api/users
```

## Verify Data in SQL Server

You can verify the data was written to SQL Server using SQL Server Management Studio (SSMS) or Azure Data Studio:

```sql
USE AdventureWorks2022;
GO

-- Check if users table was created
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'users';

-- View user data
SELECT * FROM users;
```

## Connection String Breakdown

```
jdbc:sqlserver://74.225.3.34:1433;databaseName=AdventureWorks2022;encrypt=true;trustServerCertificate=true
```

- `74.225.3.34` - SQL Server hostname/IP
- `1433` - SQL Server port
- `AdventureWorks2022` - Database name
- `encrypt=true` - Enable SSL/TLS encryption
- `trustServerCertificate=true` - Trust self-signed certificates

## Troubleshooting

### Error: "Login failed for user 'newrelicOtel'"

**Solution:** Verify the username and password are correct:
```sql
-- On SQL Server, verify the login exists
SELECT name FROM sys.server_principals WHERE name = 'newrelicOtel';

-- Verify database access
USE AdventureWorks2022;
SELECT dp.name
FROM sys.database_principals dp
WHERE dp.name = 'newrelicOtel';
```

### Error: "Cannot open server requested by login"

**Solution:** The database name might be incorrect. Check available databases:
```sql
SELECT name FROM sys.databases;
```

Update `application.properties` with the correct database name.

### Error: "Connection refused" or "Network error"

**Solution:**
1. Verify SQL Server is accessible from Windows server:
```cmd
Test-NetConnection -ComputerName 74.225.3.34 -Port 1433
```

2. Check SQL Server allows remote connections
3. Verify firewall rules allow port 1433

### Error: "The driver could not establish a secure connection"

**Solution:** If SQL Server doesn't have a valid SSL certificate, use:
```properties
spring.datasource.url=jdbc:sqlserver://74.225.3.34:1433;databaseName=AdventureWorks2022;encrypt=false
```

Or keep encryption but trust the certificate:
```properties
spring.datasource.url=jdbc:sqlserver://74.225.3.34:1433;databaseName=AdventureWorks2022;encrypt=true;trustServerCertificate=true
```

### Error: "Table 'users' already exists"

**Solution:** If you need to recreate the table, either:

1. Drop the table manually:
```sql
DROP TABLE users;
```

2. Or change `ddl-auto` to `create-drop` (will drop tables on shutdown):
```properties
spring.jpa.hibernate.ddl-auto=create-drop
```

## Using Multiple Databases

If you want to query from both monitored databases, you can switch databases by updating `application.properties`:

For **AdventureWorks2022**:
```properties
spring.datasource.url=jdbc:sqlserver://74.225.3.34:1433;databaseName=AdventureWorks2022;encrypt=true;trustServerCertificate=true
```

For **LoadTestDB**:
```properties
spring.datasource.url=jdbc:sqlserver://74.225.3.34:1433;databaseName=LoadTestDB;encrypt=true;trustServerCertificate=true
```

## APM Monitoring with SQL Server

Once connected to SQL Server, the New Relic APM agent will automatically:
- Capture SQL queries to AdventureWorks2022
- Trace database transactions
- Monitor query performance
- Report slow queries
- Show database operations in transaction traces

You'll see in New Relic:
- **Databases** tab showing SQL Server queries
- **Transaction traces** with SQL statements
- **Slow SQL queries** report
- Database throughput and response times

## Production Recommendations

For production use:

1. **Use environment variables** for sensitive data:
```properties
spring.datasource.url=${DB_URL}
spring.datasource.username=${DB_USERNAME}
spring.datasource.password=${DB_PASSWORD}
```

Set via:
```cmd
setx DB_URL "jdbc:sqlserver://74.225.3.34:1433;databaseName=AdventureWorks2022;encrypt=true;trustServerCertificate=true"
setx DB_USERNAME "newrelicOtel"
setx DB_PASSWORD "newrelic@123"
```

2. **Use connection pooling settings**:
```properties
spring.datasource.hikari.maximum-pool-size=10
spring.datasource.hikari.minimum-idle=5
spring.datasource.hikari.connection-timeout=30000
```

3. **Enable SSL properly** with valid certificates
4. **Set ddl-auto to validate or none** in production:
```properties
spring.jpa.hibernate.ddl-auto=validate
```

## Summary

✅ Application now connects to SQL Server at `74.225.3.34:1433`
✅ Uses `AdventureWorks2022` database
✅ Creates `users` table automatically
✅ All API operations now persist to SQL Server
✅ APM monitoring includes SQL Server query traces

Rebuild the application and test the SQL Server connection!
