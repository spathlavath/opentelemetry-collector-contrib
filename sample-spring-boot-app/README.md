# Spring Boot Demo Application with APM Instrumentation

This is a sample Java Spring Boot application that demonstrates how to instrument a Java application with APM (Application Performance Monitoring) agent.

## Application Overview

This demo application includes:
- RESTful API endpoints for user management (CRUD operations)
- H2 in-memory database integration
- Spring Data JPA for data persistence
- Health check endpoints
- Actuator endpoints for monitoring

## Project Structure

```
sample-spring-boot-app/
├── pom.xml                                 # Maven configuration
├── src/
│   └── main/
│       ├── java/com/example/demo/
│       │   ├── DemoApplication.java        # Main application class
│       │   ├── controller/
│       │   │   ├── UserController.java     # User REST endpoints
│       │   │   └── HealthController.java   # Health check endpoints
│       │   ├── service/
│       │   │   └── UserService.java        # Business logic layer
│       │   ├── model/
│       │   │   └── User.java               # User entity
│       │   └── repository/
│       │       └── UserRepository.java     # Data access layer
│       └── resources/
│           └── application.properties      # Application configuration
└── apm/
    └── newrelic.yml                        # APM agent configuration
```

## Prerequisites

- Java 17 or higher
- Maven 3.6 or higher
- APM Java agent JAR file (e.g., `newrelic-agent.jar`)

## Setup Instructions

### Step 1: Build the Application

Navigate to the project directory and build the application:

```bash
cd sample-spring-boot-app
mvn clean package
```

This will create a JAR file in the `target/` directory: `demo-0.0.1-SNAPSHOT.jar`

### Step 2: Configure the APM Agent

1. Place your APM agent JAR file in the `apm/` directory:
   ```bash
   cp /path/to/your/newrelic-agent.jar apm/
   ```

2. Update the `apm/newrelic.yml` configuration file:
   - Replace `<%= license_key %>` with your actual New Relic license key
   - Or set the environment variable: `export NEW_RELIC_LICENSE_KEY=your_license_key`

3. The configuration file includes settings for:
   - Application name
   - Transaction tracing
   - Error collection
   - Distributed tracing
   - Application logging
   - Browser monitoring

### Step 3: Run the Application with APM Agent

#### Option 1: Using the -javaagent flag

```bash
java -javaagent:apm/newrelic-agent.jar \
     -Dnewrelic.config.file=apm/newrelic.yml \
     -jar target/demo-0.0.1-SNAPSHOT.jar
```

#### Option 2: Using environment variables

```bash
export NEW_RELIC_LICENSE_KEY=your_license_key
export NEW_RELIC_APP_NAME="Demo Spring Boot APM App"

java -javaagent:apm/newrelic-agent.jar \
     -jar target/demo-0.0.1-SNAPSHOT.jar
```

#### Option 3: Using a startup script

Create a file `start-with-apm.sh`:

```bash
#!/bin/bash

# APM Configuration
APM_AGENT_PATH="apm/newrelic-agent.jar"
APM_CONFIG_PATH="apm/newrelic.yml"
APP_JAR="target/demo-0.0.1-SNAPSHOT.jar"

# Java Options
JAVA_OPTS="-Xmx512m -Xms256m"

# Run the application with APM agent
java $JAVA_OPTS \
     -javaagent:$APM_AGENT_PATH \
     -Dnewrelic.config.file=$APM_CONFIG_PATH \
     -Dnewrelic.environment=production \
     -jar $APP_JAR
```

Make it executable and run:

```bash
chmod +x start-with-apm.sh
./start-with-apm.sh
```

### Step 4: Verify the Application is Running

Once the application starts, you should see logs indicating:
- Spring Boot application has started
- APM agent has connected
- Application is listening on port 8080

Check the health endpoint:
```bash
curl http://localhost:8080/api/health
```

Expected response:
```json
{
  "status": "UP",
  "message": "Application is running"
}
```

## API Endpoints

### Health Check Endpoints

- `GET /api/health` - Application health status
- `GET /api/hello` - Simple hello endpoint
- `GET /actuator/health` - Spring Boot actuator health endpoint

### User Management Endpoints

- `GET /api/users` - Get all users
- `GET /api/users/{id}` - Get user by ID
- `GET /api/users/email/{email}` - Get user by email
- `POST /api/users` - Create a new user
- `PUT /api/users/{id}` - Update user by ID
- `DELETE /api/users/{id}` - Delete user by ID
- `GET /api/users/count` - Get total user count

### Example API Calls

#### Create a User

```bash
curl -X POST http://localhost:8080/api/users \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Doe",
    "email": "john.doe@example.com",
    "role": "Admin"
  }'
```

#### Get All Users

```bash
curl http://localhost:8080/api/users
```

#### Get User by ID

```bash
curl http://localhost:8080/api/users/1
```

#### Update a User

```bash
curl -X PUT http://localhost:8080/api/users/1 \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Smith",
    "email": "john.smith@example.com",
    "role": "User"
  }'
```

#### Delete a User

```bash
curl -X DELETE http://localhost:8080/api/users/1
```

## Testing APM Integration

Once the application is running with the APM agent, perform the following to generate telemetry data:

### 1. Generate Traffic

Use the provided test script to generate traffic:

```bash
# Create multiple users
for i in {1..10}; do
  curl -X POST http://localhost:8080/api/users \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"User $i\",\"email\":\"user$i@example.com\",\"role\":\"User\"}"
done

# Query users
for i in {1..20}; do
  curl http://localhost:8080/api/users
  sleep 1
done

# Query individual users
for i in {1..10}; do
  curl http://localhost:8080/api/users/$i
done
```

### 2. Verify APM Data in New Relic

After generating traffic, check your New Relic APM dashboard:

1. Log in to New Relic One
2. Navigate to APM & Services
3. Find your application: "Demo Spring Boot APM App"
4. Verify the following data:
   - **Transactions**: HTTP requests should appear in the transactions list
   - **Transaction traces**: Slow transactions with detailed traces
   - **Errors**: Any application errors should be captured
   - **Databases**: Database queries should be visible
   - **JVM metrics**: Java heap memory, garbage collection, thread pools
   - **Distributed tracing**: End-to-end transaction traces

### 3. Key Metrics to Monitor

- **Response time**: Average response time for transactions
- **Throughput**: Requests per minute
- **Error rate**: Percentage of failed requests
- **Apdex score**: Application performance index
- **Database performance**: Query time and throughput
- **JVM metrics**: Memory usage, GC activity

## APM Configuration Options

The `apm/newrelic.yml` file includes several configuration sections:

### Transaction Tracer
- Captures detailed information about slow transactions
- Configurable threshold for trace collection
- SQL statement recording (obfuscated by default)

### Error Collector
- Captures uncaught exceptions
- Excludes specific exceptions or HTTP status codes

### Distributed Tracing
- Tracks requests across multiple services
- Enabled by default

### Application Logging
- Forwards application logs to New Relic
- Captures log metrics
- Supports log decoration with trace context

## Environment-Specific Configuration

The configuration file supports different environments:

```bash
# Development
java -javaagent:apm/newrelic-agent.jar \
     -Dnewrelic.config.file=apm/newrelic.yml \
     -Dnewrelic.environment=development \
     -jar target/demo-0.0.1-SNAPSHOT.jar

# Staging
java -javaagent:apm/newrelic-agent.jar \
     -Dnewrelic.config.file=apm/newrelic.yml \
     -Dnewrelic.environment=staging \
     -jar target/demo-0.0.1-SNAPSHOT.jar

# Production (default)
java -javaagent:apm/newrelic-agent.jar \
     -Dnewrelic.config.file=apm/newrelic.yml \
     -Dnewrelic.environment=production \
     -jar target/demo-0.0.1-SNAPSHOT.jar
```

## Troubleshooting

### Agent Not Starting

If the APM agent doesn't start:

1. Check the agent logs (usually in `logs/newrelic/` directory)
2. Verify the license key is correct
3. Ensure the agent JAR path is correct
4. Check network connectivity to New Relic

### No Data in APM Dashboard

If no data appears in the APM dashboard:

1. Wait 2-3 minutes for initial data to appear
2. Verify the agent is enabled: `agent_enabled: true`
3. Check the application name in the configuration
4. Generate some traffic to the application
5. Review agent logs for errors

### High Overhead

If the agent causes performance issues:

1. Adjust transaction tracer threshold
2. Reduce `max_samples_stored` for events
3. Disable thread profiler if not needed
4. Use high security mode only if required

## Additional Resources

- [New Relic Java Agent Documentation](https://docs.newrelic.com/docs/agents/java-agent/)
- [Java Agent Configuration](https://docs.newrelic.com/docs/agents/java-agent/configuration/java-agent-configuration-config-file/)
- [Spring Boot Integration](https://docs.newrelic.com/docs/agents/java-agent/frameworks/spring-boot/)

## H2 Database Console

The application includes H2 console for database debugging:

- URL: http://localhost:8080/h2-console
- JDBC URL: `jdbc:h2:mem:testdb`
- Username: `sa`
- Password: (leave empty)

## License

This is a demo application for educational purposes.
