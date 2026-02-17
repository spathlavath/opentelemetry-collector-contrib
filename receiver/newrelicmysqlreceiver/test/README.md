# New Relic MySQL Receiver - Test Environment

This directory contains Docker Compose setup for testing the `newrelicmysqlreceiver` component.

## 📚 Documentation

**[MONITORING_QUERIES_COMPARISON.md](./MONITORING_QUERIES_COMPARISON.md)** - Complete comparison of Datadog, New Relic, and Oracle receiver patterns with MySQL queries for slow queries, wait events, blocking sessions, and execution plans.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- New Relic Ingest License key
- MySQL 8.0+ with Performance Schema enabled

## Usage

Replace the endpoint and api-key values in the otel-config.yaml with appropriate values.

### Standard MySQL Setup

1. **Build & Start the Services**

   From this directory, run:

   ```sh
   docker compose build --no-cache
   ```

   ```sh
   docker compose up -d mysql otelcol
   ```

   This will start the MySQL server and the OpenTelemetry Collector.

2. **Access the MySQL Server**

   You can connect to the MySQL server using:

   ```sh
   docker exec -it newrelicmysql-test mysql -u monitor -p
   ```

   The default password is `monitorpass`.

### Galera Cluster Setup

For testing Galera cluster metrics, use the MariaDB Galera cluster services:

1. **Build & Start the Galera Cluster**

   ```sh
   docker compose build --no-cache
   ```

   ```sh
   docker compose up -d galera1 galera2 galera3 otelcol-galera
   ```

   This will start a 3-node Galera cluster and the OpenTelemetry Collector configured to monitor all nodes.

2. **Access a Galera Node**

   You can connect to any Galera node using:

   ```sh
   # Node 1 (port 3307)
   docker exec -it galera-node1 mysql -u root -pgalerapass
   
   # Node 2 (port 3308)
   docker exec -it galera-node2 mysql -u root -pgalerapass
   
   # Node 3 (port 3309)
   docker exec -it galera-node3 mysql -u root -pgalerapass
   ```

3. **Verify Galera Cluster Status**

   Use the provided health check script:

   ```sh
   chmod +x check-galera-health.sh
   ./check-galera-health.sh
   ```

   Or manually check cluster status on any node:

   ```sql
   SHOW STATUS LIKE 'wsrep_cluster_size';
   SHOW STATUS LIKE 'wsrep_cluster_status';
   SHOW STATUS LIKE 'wsrep_local_state_comment';
   SHOW STATUS LIKE 'wsrep_%';
   ```

   Expected cluster size: `3`
   Expected cluster status: `Primary`
   Expected local state: `Synced`

4. **Test Galera Replication**

   Use the provided replication test script:

   ```sh
   chmod +x test-galera-replication.sh
   ./test-galera-replication.sh
   ```

   Or manually create a test table on one node:

   ```sql
   -- On galera-node1
   CREATE DATABASE IF NOT EXISTS test_replication;
   USE test_replication;
   CREATE TABLE items (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100));
   INSERT INTO items (name) VALUES ('test-item-1'), ('test-item-2');
   ```

   Verify replication on another node:

   ```sql
   -- On galera-node2 or galera-node3
   USE test_replication;
   SELECT * FROM items;
   ```

5. **Query Galera Metrics in New Relic**

   After the collector is running, query the Galera-specific metrics:

   ```
   FROM Metric SELECT * WHERE galera.cluster.name = 'galera-cluster'
   ```

   Galera metrics include:
   - `newrelicmysql.galera.wsrep_cluster_size` - Number of nodes in the cluster
   - `newrelicmysql.galera.wsrep_local_state` - Node state
   - `newrelicmysql.galera.wsrep_flow_control_paused` - Flow control pause fraction
   - `newrelicmysql.galera.wsrep_cert_deps_distance` - Replication parallelization
   - And 11 more Galera-specific metrics

3. **Query Metrics in New Relic**

   After the collector is running and exporting metrics, you can query the ingested metrics in New Relic using NRQL:

   ```
   FROM Metric SELECT * WHERE mysql.service.name = 'local-newrelicmysql-monitoring'
   ```

   Use this query in the New Relic Query Builder to view all metrics collected for your test MySQL service.

## Stopping the Services

To stop and remove the containers:

```sh
# Stop all services
docker compose down

# Stop only standard MySQL
docker compose down mysql otelcol

# Stop only Galera cluster
docker compose down galera1 galera2 galera3 otelcol-galera

# Stop and remove volumes (WARNING: deletes all data)
docker compose down -v
```

## Monitoring Queries

See **[MONITORING_QUERIES_COMPARISON.md](./MONITORING_QUERIES_COMPARISON.md)** for complete query specifications.

The receiver implements these query patterns:

1. **Slow Queries + Wait Events + Blocking** (10-30s) - Complete session context
2. **Blocking Sessions Detail** (10s) - Quick blocking detection  
3. **Query Digest Summary** (60s) - Historical aggregates (requires delta calculation)
4. **Wait Event Summary** (60s) - System-wide statistics
5. **Execution Plans** (on-demand) - Dynamic EXPLAIN

## Performance Schema Setup

The receiver requires Performance Schema to be enabled:

```sql
-- Verify Performance Schema is enabled
SHOW VARIABLES LIKE 'performance_schema';

-- Enable required consumers
UPDATE performance_schema.setup_consumers SET enabled = 'YES' 
WHERE name IN ('events_statements_current', 'events_waits_current');

-- Enable instrumentation
UPDATE performance_schema.setup_instruments SET enabled = 'YES', timed = 'YES' 
WHERE name LIKE 'statement/%' OR name LIKE 'wait/io/%';
```

## Configuration

### Standard MySQL Configuration

The receiver configuration (`otel-config.yaml`) includes:

```yaml
receivers:
  newrelicmysql:
    endpoint: mysql:3306
    username: monitor
    password: monitorpass
    collection_interval: 10s
    extra_status_metrics: true  # Enables 36 additional status metrics
```

### Galera Cluster Configuration

The Galera configuration (`otel-galera-config.yaml`) includes:

```yaml
receivers:
  newrelicmysql/galera1:
    endpoint: galera1:3306
    username: root
    password: galerapass
    collection_interval: 10s
    extra_status_metrics: true  # Datadog extra_status_metrics
    galera_cluster: true        # Enables 15 Galera wsrep_* metrics
```

**Galera Metrics Collected:**

When `galera_cluster: true` is enabled, the following metrics are collected:

| Metric | Type | Description |
|--------|------|-------------|
| `wsrep_cluster_size` | gauge | Number of nodes in the cluster |
| `wsrep_local_state` | gauge | Node state (4 = Synced) |
| `wsrep_flow_control_paused` | gauge | Fraction of time paused due to flow control |
| `wsrep_flow_control_paused_ns` | sum | Total pause time in nanoseconds |
| `wsrep_flow_control_recv` | sum | FC_PAUSE events received |
| `wsrep_flow_control_sent` | sum | FC_PAUSE events sent |
| `wsrep_local_cert_failures` | sum | Failed certifications |
| `wsrep_local_recv_queue` | gauge | Received queue size |
| `wsrep_local_recv_queue_avg` | gauge | Average received queue size |
| `wsrep_local_send_queue` | gauge | Send queue size |
| `wsrep_local_send_queue_avg` | gauge | Average send queue size |
| `wsrep_cert_deps_distance` | gauge | Replication parallelization potential |
| `wsrep_received` | sum | Write-sets received |
| `wsrep_received_bytes` | sum | Bytes received |
| `wsrep_replicated_bytes` | sum | Bytes replicated |

## Troubleshooting

### Connection Issues
- Ensure the containers are running and the ports are not blocked
- Check logs with: `docker compose logs`

### No Metrics Collected
- Verify Performance Schema is enabled: `SELECT @@performance_schema;`
- Check consumer status: `SELECT * FROM performance_schema.setup_consumers;`
- Ensure user has permissions: `GRANT SELECT ON performance_schema.* TO 'monitor'@'%';`

### High Overhead
- Disable unnecessary wait event instrumentation
- Reduce collection frequency for digest summary
- Limit EXPLAIN executions (use rate limiting)

### Galera Cluster Issues

**Cluster Not Forming:**
```sh
# Check cluster status on each node
docker exec -it galera-node1 mysql -u root -pgalerapass -e "SHOW STATUS LIKE 'wsrep_cluster_size';"
docker exec -it galera-node2 mysql -u root -pgalerapass -e "SHOW STATUS LIKE 'wsrep_cluster_size';"
docker exec -it galera-node3 mysql -u root -pgalerapass -e "SHOW STATUS LIKE 'wsrep_cluster_size';"

# Check node state
docker exec -it galera-node1 mysql -u root -pgalerapass -e "SHOW STATUS LIKE 'wsrep_local_state_comment';"

# Check logs
docker compose logs galera1
docker compose logs galera2
docker compose logs galera3
```

**Node Not Syncing:**
- Ensure all nodes have the same `wsrep_cluster_name`
- Check network connectivity between containers
- Verify ports 4567, 4568, 4444 are accessible
- Restart nodes in order: galera1 (bootstrap), then galera2, then galera3

**No Galera Metrics Appearing:**
- Verify `galera_cluster: true` is set in the receiver configuration
- Check if wsrep variables are available: `SHOW STATUS LIKE 'wsrep_%';`
- Ensure using MariaDB/Percona XtraDB with Galera (not standard MySQL)
- Check collector logs: `docker compose logs otelcol-galera`

**Replication Lag:**
```sql
-- Check replication status
SHOW STATUS LIKE 'wsrep_local_recv_queue%';
SHOW STATUS LIKE 'wsrep_flow_control%';

-- If flow control is active, check:
SHOW STATUS LIKE 'wsrep_local_send_queue%';
```

---

## Quick Reference

### Ports

| Service | MySQL Port | Galera Ports |
|---------|-----------|--------------|
| Standard MySQL | 3306 | N/A |
| Galera Node 1 | 3307 | 4567, 4568, 4444 |
| Galera Node 2 | 3308 | 4577, 4578, 4454 |
| Galera Node 3 | 3309 | 4587, 4588, 4464 |

### Helper Scripts

| Script | Purpose |
|--------|---------|
| `check-galera-health.sh` | Check health and status of all Galera nodes |
| `test-galera-replication.sh` | Test multi-master replication functionality |

### Default Credentials

| Service | Username | Password |
|---------|----------|----------|
| Standard MySQL (monitor user) | monitor | monitorpass |
| Standard MySQL (root) | root | rootpassword |
| Galera Cluster | root | galerapass |

### Configuration Files

| File | Purpose |
|------|---------|
| `otel-config.yaml` | OpenTelemetry Collector config for standard MySQL |
| `otel-galera-config.yaml` | OpenTelemetry Collector config for Galera cluster (monitors all 3 nodes) |
| `docker-compose.yml` | Docker Compose configuration with both MySQL and Galera services |

---

**Note:**  
This setup is intended for local development and testing only.