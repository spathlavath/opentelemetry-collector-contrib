# Windows Server Deployment Guide

This guide provides step-by-step instructions for deploying the Spring Boot APM demo application on Windows Server.

## Prerequisites on Windows Server

1. **Java 17 or higher** - Download from [Oracle](https://www.oracle.com/java/technologies/downloads/) or [OpenJDK](https://adoptium.net/)
2. **Maven 3.6 or higher** (optional, if building on Windows) - Download from [Apache Maven](https://maven.apache.org/download.cgi)
3. **New Relic Java Agent JAR file** - `newrelic-agent.jar`
4. **curl** (optional, for testing) - Available in Windows 10+ or install from [curl.se](https://curl.se/windows/)

## Files to Transfer to Windows Server

### Option 1: Transfer Pre-Built Application (Recommended)

If you've already built the application on your development machine, transfer these files:

```
sample-spring-boot-app/
├── target/
│   └── demo-0.0.1-SNAPSHOT.jar          # Built application JAR
├── apm/
│   ├── newrelic.yml                      # APM configuration file
│   └── newrelic-agent.jar                # Your APM agent JAR (copy this file)
├── start-with-apm.bat                    # Windows startup script
└── test-api.bat                          # Windows test script (optional)
```

**Minimum files required:**
1. `target/demo-0.0.1-SNAPSHOT.jar` - The application
2. `apm/newrelic.yml` - APM configuration
3. `apm/newrelic-agent.jar` - Your APM agent
4. `start-with-apm.bat` - Startup script

### Option 2: Transfer Source Code and Build on Windows

If you want to build on Windows Server, transfer the entire project:

```
sample-spring-boot-app/
├── src/                                  # Source code directory
├── pom.xml                               # Maven configuration
├── apm/
│   ├── newrelic.yml                      # APM configuration
│   └── newrelic-agent.jar                # Your APM agent JAR
├── start-with-apm.bat                    # Windows startup script
└── test-api.bat                          # Windows test script
```

## Step-by-Step Deployment Instructions

### Step 1: Create Directory on Windows Server

Open PowerShell or Command Prompt and create a deployment directory:

```cmd
mkdir C:\apps\demo-spring-boot-app
cd C:\apps\demo-spring-boot-app
```

### Step 2: Transfer Files Using SCP

From your development machine (Mac/Linux), use SCP to transfer files:

#### Option 1: Transfer Pre-Built Application

```bash
# Navigate to the project directory on your Mac
cd /Users/pkulkarni/workspace/mssql-otel/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/sample-spring-boot-app

# Transfer the built JAR file
scp target/demo-0.0.1-SNAPSHOT.jar username@windows-server:C:/apps/demo-spring-boot-app/

# Transfer APM configuration
scp apm/newrelic.yml username@windows-server:C:/apps/demo-spring-boot-app/apm/

# Transfer your APM agent JAR
scp apm/newrelic-agent.jar username@windows-server:C:/apps/demo-spring-boot-app/apm/

# Transfer Windows startup script
scp start-with-apm.bat username@windows-server:C:/apps/demo-spring-boot-app/

# Optional: Transfer test script
scp test-api.bat username@windows-server:C:/apps/demo-spring-boot-app/
```

#### Option 2: Transfer Entire Project

```bash
# Transfer the entire project directory
scp -r sample-spring-boot-app username@windows-server:C:/apps/
```

### Alternative: Using WinSCP or FileZilla

If you prefer a GUI tool:

1. **Using WinSCP:**
   - Download from https://winscp.net/
   - Connect to your Windows Server
   - Navigate to `C:\apps\demo-spring-boot-app\`
   - Drag and drop the required files

2. **Using FileZilla:**
   - Download from https://filezilla-project.org/
   - Connect using SFTP protocol
   - Transfer the files to the target directory

### Step 3: Configure APM on Windows Server

On the Windows Server, edit the APM configuration file:

```cmd
notepad C:\apps\demo-spring-boot-app\apm\newrelic.yml
```

Update the license key:
- Find the line: `license_key: '<%= license_key %>'`
- Replace with your actual key: `license_key: 'your_actual_license_key_here'`

Or set as environment variable:

```cmd
setx NEW_RELIC_LICENSE_KEY "your_actual_license_key_here"
```

### Step 4: Build Application on Windows (If needed)

If you transferred source code and need to build on Windows:

```cmd
cd C:\apps\demo-spring-boot-app
mvn clean package
```

This will create `target\demo-0.0.1-SNAPSHOT.jar`

### Step 5: Verify Java Installation

```cmd
java -version
```

Expected output should show Java 17 or higher.

### Step 6: Run the Application with APM Agent

#### Using the Batch Script

```cmd
cd C:\apps\demo-spring-boot-app
start-with-apm.bat
```

#### Manual Command (Alternative)

```cmd
cd C:\apps\demo-spring-boot-app

java -Xmx512m -Xms256m ^
     -javaagent:apm\newrelic-agent.jar ^
     -Dnewrelic.config.file=apm\newrelic.yml ^
     -Dnewrelic.environment=production ^
     -jar target\demo-0.0.1-SNAPSHOT.jar
```

### Step 7: Verify Application is Running

Open a new Command Prompt or PowerShell window:

```cmd
curl http://localhost:8080/api/health
```

Or open a web browser and navigate to:
- http://localhost:8080/api/health
- http://localhost:8080/api/hello

### Step 8: Test the API (Optional)

Run the test script:

```cmd
cd C:\apps\demo-spring-boot-app
test-api.bat
```

Or test manually using curl or PowerShell:

```powershell
# Using PowerShell
Invoke-RestMethod -Uri "http://localhost:8080/api/health" -Method Get

# Create a user
$body = @{
    name = "John Doe"
    email = "john@example.com"
    role = "Admin"
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8080/api/users" -Method Post -Body $body -ContentType "application/json"

# Get all users
Invoke-RestMethod -Uri "http://localhost:8080/api/users" -Method Get
```

## Running as a Windows Service (Optional)

To run the application as a Windows service, you can use tools like:

### Option 1: Using NSSM (Non-Sucking Service Manager)

1. Download NSSM from https://nssm.cc/download

2. Install the service:
```cmd
nssm install DemoSpringBootApp "C:\Program Files\Java\jdk-17\bin\java.exe"
nssm set DemoSpringBootApp AppDirectory "C:\apps\demo-spring-boot-app"
nssm set DemoSpringBootApp AppParameters "-Xmx512m -Xms256m -javaagent:apm\newrelic-agent.jar -Dnewrelic.config.file=apm\newrelic.yml -jar target\demo-0.0.1-SNAPSHOT.jar"
nssm set DemoSpringBootApp DisplayName "Demo Spring Boot APM App"
nssm set DemoSpringBootApp Description "Spring Boot application with New Relic APM monitoring"
nssm set DemoSpringBootApp Start SERVICE_AUTO_START
```

3. Start the service:
```cmd
nssm start DemoSpringBootApp
```

4. Check service status:
```cmd
nssm status DemoSpringBootApp
```

### Option 2: Using Windows Service Wrapper (WinSW)

1. Download WinSW from https://github.com/winsw/winsw/releases

2. Create `demo-app-service.xml`:

```xml
<service>
  <id>DemoSpringBootApp</id>
  <name>Demo Spring Boot APM App</name>
  <description>Spring Boot application with New Relic APM monitoring</description>
  <executable>java</executable>
  <arguments>-Xmx512m -Xms256m -javaagent:apm\newrelic-agent.jar -Dnewrelic.config.file=apm\newrelic.yml -jar target\demo-0.0.1-SNAPSHOT.jar</arguments>
  <workingdirectory>C:\apps\demo-spring-boot-app</workingdirectory>
  <logpath>C:\apps\demo-spring-boot-app\logs</logpath>
  <log mode="roll-by-size">
    <sizeThreshold>10240</sizeThreshold>
    <keepFiles>8</keepFiles>
  </log>
  <startmode>Automatic</startmode>
</service>
```

3. Install and start:
```cmd
demo-app-service.exe install
demo-app-service.exe start
```

## Firewall Configuration

If you need to access the application from other machines:

```cmd
netsh advfirewall firewall add rule name="Spring Boot App" dir=in action=allow protocol=TCP localport=8080
```

## Environment Variables (Alternative to Config File)

Instead of editing `newrelic.yml`, you can set environment variables:

```cmd
setx NEW_RELIC_LICENSE_KEY "your_license_key"
setx NEW_RELIC_APP_NAME "Demo Spring Boot APM App"
setx NEW_RELIC_LOG_FILE_PATH "C:\apps\demo-spring-boot-app\logs"
```

## Troubleshooting on Windows

### Issue: Java not found

**Solution:** Add Java to PATH or use full path:
```cmd
"C:\Program Files\Java\jdk-17\bin\java.exe" -javaagent:apm\newrelic-agent.jar -jar target\demo-0.0.1-SNAPSHOT.jar
```

### Issue: Port 8080 already in use

**Solution:** Change the port in `src\main\resources\application.properties`:
```properties
server.port=8081
```

Or use command line argument:
```cmd
java -javaagent:apm\newrelic-agent.jar -jar target\demo-0.0.1-SNAPSHOT.jar --server.port=8081
```

### Issue: APM agent not connecting

**Solution:** Check logs in `C:\apps\demo-spring-boot-app\logs\newrelic\`

Enable debug logging:
```cmd
java -javaagent:apm\newrelic-agent.jar -Dnewrelic.config.log_level=finest -jar target\demo-0.0.1-SNAPSHOT.jar
```

### Issue: Access denied errors

**Solution:** Run Command Prompt as Administrator or adjust file permissions:
```cmd
icacls C:\apps\demo-spring-boot-app /grant Users:(OI)(CI)F /T
```

## Monitoring and Logs

Application logs location:
- Application logs: Console output or configure logging in `application.properties`
- APM agent logs: `C:\apps\demo-spring-boot-app\logs\newrelic\`

View logs:
```cmd
type C:\apps\demo-spring-boot-app\logs\newrelic\newrelic_agent.log
```

## Quick Reference: Essential Files Only

For a minimal deployment, you only need these 4 files on Windows:

```
C:\apps\demo-spring-boot-app\
├── demo-0.0.1-SNAPSHOT.jar       (rename from target/)
├── newrelic-agent.jar            (your APM agent)
├── newrelic.yml                  (APM config)
└── start.bat                     (startup script)
```

Minimal `start.bat`:
```batch
@echo off
java -javaagent:newrelic-agent.jar -Dnewrelic.config.file=newrelic.yml -jar demo-0.0.1-SNAPSHOT.jar
```

## Security Considerations

1. **Don't commit license keys** to version control
2. **Use environment variables** for sensitive configuration
3. **Restrict file permissions** on the APM config file
4. **Use HTTPS** in production (configure SSL in Spring Boot)
5. **Keep Java and dependencies updated**

## Summary of SCP Commands

Here's a quick copy-paste section for all SCP commands:

```bash
# From your Mac, run these commands:
cd /Users/pkulkarni/workspace/mssql-otel/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver

# Create target directory on Windows
ssh username@windows-server "mkdir C:\apps\demo-spring-boot-app\apm"

# Copy files (replace 'username@windows-server' with your actual credentials)
scp -r sample-spring-boot-app/target/demo-0.0.1-SNAPSHOT.jar username@windows-server:C:/apps/demo-spring-boot-app/
scp sample-spring-boot-app/apm/newrelic.yml username@windows-server:C:/apps/demo-spring-boot-app/apm/
scp sample-spring-boot-app/apm/newrelic-agent.jar username@windows-server:C:/apps/demo-spring-boot-app/apm/
scp sample-spring-boot-app/start-with-apm.bat username@windows-server:C:/apps/demo-spring-boot-app/
scp sample-spring-boot-app/test-api.bat username@windows-server:C:/apps/demo-spring-boot-app/
```

## Next Steps

After deployment:
1. Verify the application is running
2. Generate some test traffic
3. Log in to New Relic APM dashboard
4. Confirm telemetry data is being received
5. Monitor application performance metrics
