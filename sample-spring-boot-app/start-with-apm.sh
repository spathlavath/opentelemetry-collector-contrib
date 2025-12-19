#!/bin/bash

# APM Configuration
APM_AGENT_PATH="apm/newrelic-agent.jar"
APM_CONFIG_PATH="apm/newrelic.yml"
APP_JAR="target/demo-0.0.1-SNAPSHOT.jar"

# Java Options - Increased heap size to prevent OOM
JAVA_OPTS="-Xmx2g -Xms512m"

# Check if APM agent JAR exists
if [ ! -f "$APM_AGENT_PATH" ]; then
    echo "Error: APM agent JAR not found at $APM_AGENT_PATH"
    echo "Please copy your newrelic-agent.jar to the apm/ directory"
    exit 1
fi

# Check if application JAR exists
if [ ! -f "$APP_JAR" ]; then
    echo "Error: Application JAR not found at $APP_JAR"
    echo "Please run 'mvn clean package' first"
    exit 1
fi

# Check if config file exists
if [ ! -f "$APM_CONFIG_PATH" ]; then
    echo "Error: APM config file not found at $APM_CONFIG_PATH"
    exit 1
fi

echo "=========================================="
echo "Starting Spring Boot App with APM Agent"
echo "=========================================="
echo "APM Agent: $APM_AGENT_PATH"
echo "Config File: $APM_CONFIG_PATH"
echo "Application JAR: $APP_JAR"
echo "=========================================="
echo ""

# Run the application with APM agent
java $JAVA_OPTS \
     -javaagent:$APM_AGENT_PATH \
     -Dnewrelic.config.file=$APM_CONFIG_PATH \
     -Dnewrelic.environment=production \
     -jar $APP_JAR
