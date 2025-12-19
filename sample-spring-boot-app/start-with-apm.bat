@echo off
REM APM Configuration
SET APM_AGENT_PATH=apm\newrelic-agent.jar
SET APM_CONFIG_PATH=apm\newrelic.yml
SET APP_JAR=target\demo-0.0.1-SNAPSHOT.jar

REM Java Options - Increased heap size to prevent OOM
SET JAVA_OPTS=-Xmx2g -Xms512m

REM Check if APM agent JAR exists
IF NOT EXIST "%APM_AGENT_PATH%" (
    echo Error: APM agent JAR not found at %APM_AGENT_PATH%
    echo Please copy your newrelic-agent.jar to the apm\ directory
    exit /b 1
)

REM Check if application JAR exists
IF NOT EXIST "%APP_JAR%" (
    echo Error: Application JAR not found at %APP_JAR%
    echo Please run 'mvn clean package' first
    exit /b 1
)

REM Check if config file exists
IF NOT EXIST "%APM_CONFIG_PATH%" (
    echo Error: APM config file not found at %APM_CONFIG_PATH%
    exit /b 1
)

echo ==========================================
echo Starting Spring Boot App with APM Agent
echo ==========================================
echo APM Agent: %APM_AGENT_PATH%
echo Config File: %APM_CONFIG_PATH%
echo Application JAR: %APP_JAR%
echo ==========================================
echo.

REM Run the application with APM agent
java %JAVA_OPTS% ^
     -javaagent:%APM_AGENT_PATH% ^
     -Dnewrelic.config.file=%APM_CONFIG_PATH% ^
     -Dnewrelic.environment=staging ^
     -jar %APP_JAR%
