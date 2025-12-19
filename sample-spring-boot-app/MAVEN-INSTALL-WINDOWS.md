# Maven Installation Guide for Windows

## Prerequisites
- ✅ JDK 25 installed (`jdk-25_windows-x64_bin`)
- Java should be in your PATH

## Step 1: Verify Java Installation

Open Command Prompt or PowerShell and verify Java is installed:

```cmd
java -version
```

You should see output like:
```
java version "25" ...
```

Check JAVA_HOME:
```cmd
echo %JAVA_HOME%
```

If JAVA_HOME is not set, you'll need to set it (see Step 4).

## Step 2: Download Maven

### Option A: Direct Download

1. Go to: https://maven.apache.org/download.cgi
2. Download the **Binary zip archive**: `apache-maven-3.9.6-bin.zip` (or latest version)
3. Or use PowerShell to download directly:

```powershell
# Download Maven 3.9.6
Invoke-WebRequest -Uri "https://dlcdn.apache.org/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.zip" -OutFile "$env:TEMP\apache-maven-3.9.6-bin.zip"
```

### Option B: Using Chocolatey (if installed)

```cmd
choco install maven
```

## Step 3: Extract Maven

1. Extract the downloaded zip file to a location like:
   ```
   C:\Program Files\Apache\maven\
   ```

2. Using PowerShell:
```powershell
# Extract Maven
Expand-Archive -Path "$env:TEMP\apache-maven-3.9.6-bin.zip" -DestinationPath "C:\Program Files\Apache\"

# Rename folder for easier access
Rename-Item "C:\Program Files\Apache\apache-maven-3.9.6" "C:\Program Files\Apache\maven"
```

3. Or manually:
   - Right-click the zip file → Extract All
   - Extract to `C:\Program Files\Apache\`
   - Rename `apache-maven-3.9.6` to `maven`

## Step 4: Set Environment Variables

### Using GUI (Recommended for beginners):

1. **Set JAVA_HOME:**
   - Right-click **This PC** → **Properties**
   - Click **Advanced system settings**
   - Click **Environment Variables**
   - Under **System variables**, click **New**
   - Variable name: `JAVA_HOME`
   - Variable value: `C:\Program Files\Java\jdk-25` (adjust to your JDK installation path)
   - Click **OK**

2. **Set MAVEN_HOME:**
   - In the same **Environment Variables** window
   - Under **System variables**, click **New**
   - Variable name: `MAVEN_HOME`
   - Variable value: `C:\Program Files\Apache\maven`
   - Click **OK**

3. **Add Maven to PATH:**
   - In **System variables**, find and select **Path**
   - Click **Edit**
   - Click **New**
   - Add: `%MAVEN_HOME%\bin`
   - Click **OK**
   - Also add: `%JAVA_HOME%\bin` (if not already there)
   - Click **OK** on all windows

### Using Command Prompt (Admin):

```cmd
:: Set JAVA_HOME
setx JAVA_HOME "C:\Program Files\Java\jdk-25" /M

:: Set MAVEN_HOME
setx MAVEN_HOME "C:\Program Files\Apache\maven" /M

:: Add to PATH (append to existing PATH)
setx PATH "%PATH%;%MAVEN_HOME%\bin;%JAVA_HOME%\bin" /M
```

### Using PowerShell (Admin):

```powershell
# Set JAVA_HOME
[System.Environment]::SetEnvironmentVariable('JAVA_HOME', 'C:\Program Files\Java\jdk-25', 'Machine')

# Set MAVEN_HOME
[System.Environment]::SetEnvironmentVariable('MAVEN_HOME', 'C:\Program Files\Apache\maven', 'Machine')

# Add to PATH
$oldPath = [System.Environment]::GetEnvironmentVariable('Path', 'Machine')
$newPath = $oldPath + ';%MAVEN_HOME%\bin;%JAVA_HOME%\bin'
[System.Environment]::SetEnvironmentVariable('Path', $newPath, 'Machine')
```

## Step 5: Verify Maven Installation

**IMPORTANT:** Close and reopen your Command Prompt/PowerShell to reload environment variables.

```cmd
mvn -version
```

Expected output:
```
Apache Maven 3.9.6 (...)
Maven home: C:\Program Files\Apache\maven
Java version: 25, vendor: Oracle Corporation
Java home: C:\Program Files\Java\jdk-25
Default locale: en_US, platform encoding: UTF-8
OS name: "windows 11", version: "10.0", arch: "amd64", family: "windows"
```

## Step 6: Configure Maven (Optional)

### Set Maven Local Repository Location

Create or edit `C:\Users\YourUsername\.m2\settings.xml`:

```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                              https://maven.apache.org/xsd/settings-1.0.0.xsd">
  <localRepository>C:/maven-repository</localRepository>
</settings>
```

This changes where Maven stores downloaded dependencies.

## Step 7: Build Your Spring Boot Application

Now you can build the application:

```cmd
cd C:\apps\sample-spring-boot-app
mvn clean package
```

You should see:
```
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

The JAR file will be created at:
```
C:\apps\sample-spring-boot-app\target\demo-0.0.1-SNAPSHOT.jar
```

## Troubleshooting

### Issue: "mvn is not recognized"

**Solution:**
- Close and reopen Command Prompt/PowerShell
- Verify PATH contains `%MAVEN_HOME%\bin`
- Check: `echo %PATH%`

### Issue: "JAVA_HOME is not set"

**Solution:**
```cmd
setx JAVA_HOME "C:\Program Files\Java\jdk-25" /M
```

Then close and reopen Command Prompt.

### Issue: "Unable to locate the Javac Compiler"

**Solution:** Maven needs the JDK (not JRE). Verify:
```cmd
"%JAVA_HOME%\bin\javac" -version
```

If javac is not found, ensure JAVA_HOME points to the JDK directory.

### Issue: Maven build fails with SSL/TLS errors

**Solution:** Maven repositories use HTTPS. Ensure your Windows server can access:
- https://repo.maven.apache.org/maven2/

Test connectivity:
```cmd
curl https://repo.maven.apache.org/maven2/
```

### Issue: Build is very slow

**Solution:** First build downloads all dependencies. Subsequent builds will be faster.

You can also configure a mirror in `settings.xml` for faster downloads.

## Quick Install Script (PowerShell - Run as Administrator)

Save this as `install-maven.ps1` and run:

```powershell
# Maven Installation Script for Windows
$mavenVersion = "3.9.6"
$mavenUrl = "https://dlcdn.apache.org/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.zip"
$downloadPath = "$env:TEMP\apache-maven-$mavenVersion-bin.zip"
$installPath = "C:\Program Files\Apache\maven"

# Download Maven
Write-Host "Downloading Maven $mavenVersion..." -ForegroundColor Green
Invoke-WebRequest -Uri $mavenUrl -OutFile $downloadPath

# Create install directory
Write-Host "Creating installation directory..." -ForegroundColor Green
New-Item -ItemType Directory -Force -Path "C:\Program Files\Apache" | Out-Null

# Extract Maven
Write-Host "Extracting Maven..." -ForegroundColor Green
Expand-Archive -Path $downloadPath -DestinationPath "C:\Program Files\Apache\" -Force

# Rename folder
if (Test-Path "C:\Program Files\Apache\apache-maven-$mavenVersion") {
    if (Test-Path $installPath) {
        Remove-Item $installPath -Recurse -Force
    }
    Rename-Item "C:\Program Files\Apache\apache-maven-$mavenVersion" "maven"
}

# Set environment variables
Write-Host "Setting environment variables..." -ForegroundColor Green

# Set MAVEN_HOME
[System.Environment]::SetEnvironmentVariable('MAVEN_HOME', $installPath, 'Machine')

# Add to PATH
$currentPath = [System.Environment]::GetEnvironmentVariable('Path', 'Machine')
if ($currentPath -notlike "*$installPath\bin*") {
    $newPath = $currentPath + ";$installPath\bin"
    [System.Environment]::SetEnvironmentVariable('Path', $newPath, 'Machine')
}

# Clean up
Remove-Item $downloadPath -Force

Write-Host "`nMaven installation complete!" -ForegroundColor Green
Write-Host "IMPORTANT: Close and reopen your terminal to use Maven" -ForegroundColor Yellow
Write-Host "`nVerify installation with: mvn -version" -ForegroundColor Cyan
```

Run it:
```powershell
# Save the script, then run as Administrator:
Set-ExecutionPolicy Bypass -Scope Process -Force
.\install-maven.ps1
```

## Summary - Quick Commands

After Maven is installed, here's what to do:

```cmd
# 1. Navigate to project
cd C:\apps\sample-spring-boot-app

# 2. Build the application
mvn clean package

# 3. Run with APM
start-with-apm.bat

# 4. Test the application
test-api.bat
```

## Next Steps

Once Maven is installed and you've built the application:
1. Copy your `newrelic-agent.jar` to the `apm\` folder
2. Update license key in `apm\newrelic.yml`
3. Run `start-with-apm.bat`
4. Access the application at http://localhost:8080
