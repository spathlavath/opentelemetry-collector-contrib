# New Relic PostgreSQL Receiver

The New Relic PostgreSQL Receiver is an OpenTelemetry receiver that collects metrics from PostgreSQL databases.

## Features

### Basic Capabilities
- PostgreSQL database connection support
- SSL/TLS connection encryption
- Certificate-based authentication
- Connection pooling configuration

### Environment Compatibility
- PostgreSQL 10 and later versions
- Amazon RDS for PostgreSQL
- Amazon Aurora PostgreSQL
- Azure Database for PostgreSQL
- Google Cloud SQL for PostgreSQL
- Self-hosted PostgreSQL instances

## Configuration

### Basic Configuration

```yaml
receivers:
  newrelicpostgressqlreceiver:
    hostname: "localhost"
    port: "5432"
    username: "monitoring_user"
    password: "secure_password"
    database: "postgres"
    collection_interval: 15s
```

### Full Configuration Options

```yaml
receivers:
  newrelicpostgressqlreceiver:
    # Connection settings
    hostname: "postgres-server.example.com"  # Required: PostgreSQL hostname
    port: "5432"                             # Port number (default: 5432)
    username: "monitor_user"                 # Required: PostgreSQL username
    password: "secure_password"              # PostgreSQL password
    database: "postgres"                     # Required: Database name to connect to
    
    # SSL Configuration
    ssl_mode: "disable"                      # SSL mode: disable, require, verify-ca, verify-full
    ssl_cert: "/path/to/client-cert.pem"    # Path to SSL client certificate
    ssl_key: "/path/to/client-key.pem"      # Path to SSL client key
    ssl_root_cert: "/path/to/ca-cert.pem"   # Path to SSL root certificate
    ssl_password: ""                         # SSL certificate password (if encrypted)
    
    # Collection settings
    collection_interval: 15s                 # How often to collect metrics
    timeout: 30s                            # Query timeout duration
```

## Prerequisites

### Database User Permissions

The monitoring user needs the following minimum permissions:

```sql
-- Create monitoring user
CREATE USER monitoring_user WITH PASSWORD 'secure_password';

-- Grant connect permission
GRANT CONNECT ON DATABASE postgres TO monitoring_user;

-- Grant usage on schema
GRANT USAGE ON SCHEMA public TO monitoring_user;
```

## Example Configuration

### Monitoring with SSL

```yaml
receivers:
  newrelicpostgressqlreceiver:
    hostname: "postgres.example.com"
    port: "5432"
    username: "monitor_user"
    password: "${env:PG_PASSWORD}"
    database: "production"
    ssl_mode: "verify-full"
    ssl_root_cert: "/etc/ssl/certs/ca-bundle.crt"
    collection_interval: 30s
```

## Development

To generate internal metadata code:

```bash
cd receiver/newrelicpostgressqlreceiver
make generate
```

To run tests:

```bash
make test
```

## Support

For issues, questions, or contributions, please refer to the OpenTelemetry Collector Contrib repository.
