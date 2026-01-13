# Docker Compose Setup for Integration Testing

This directory contains a `docker-compose.yaml` file to help you run integration tests for the `newrelicmysqlreceiver` component.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- New Relic Ingest License key

## Usage

Replace the endpoint and api-key values in the otel-config.yaml with appropriate values.

1. **Build & Start the Services**

   From this directory, run:

   ```sh
   docker compose build --no-cache
   ```

   ```sh
   docker compose up -d
   ```

   This will start the MySQL server and any other services defined in `docker-compose.yaml`.

2. **Access the MySQL Server**

   You can connect to the MySQL server using:

   ```sh
   docker exec -it <mysql_container_name> mysql -u monitor -p
   ```

   The default password is `monitorpass`.

3. **Query Metrics in New Relic**

   After the collector is running and exporting metrics, you can query the ingested metrics in New Relic using NRQL:

   ```
   FROM Metric SELECT * WHERE mysql.service.name = 'local-newrelicmysql-monitoring'
   ```

   Use this query in the New Relic Query Builder to view all metrics collected for your test MySQL service.

## Stopping the Services

To stop and remove the containers:

```sh
docker compose down
```

## Troubleshooting

- If you encounter connection errors, ensure the containers are running and the ports are not blocked.
- Check logs with:

  ```sh
  docker compose logs
  ```

---

**Note:**  
This setup is intended for local development and testing only.