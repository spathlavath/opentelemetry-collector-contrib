# Maven Daemon (mvnd) Setup Guide for Windows

You've downloaded Maven Daemon (mvnd) which is a faster alternative to standard Maven. Here's how to complete the setup:

## Step 1: Extract mvnd

You've already downloaded the file. Now extract it:

```powershell
# Extract mvnd
Expand-Archive -Path "$env:TEMP\apache-maven-3.9.6-bin.zip" -DestinationPath "C:\Program Files\Apache\" -Force

# The extracted folder will be named: maven-mvnd-1.0.3-windows-amd64
```

Or manually:
- Navigate to: `C:\Users\YourUsername\AppData\Local\Temp\`
- Find: `apache-maven-3.9.6-bin.zip` (it's actually the mvnd file)
- Right-click → Extract All
- Extract to: `C:\Program Files\Apache\`

## Step 2: Set Environment Variables

### Option A: Using PowerShell (Run as Administrator)

```powershell
# Set JAVA_HOME (if not already set)
[System.Environment]::SetEnvironmentVariable('JAVA_HOME', 'C:\Program Files\Java\jdk-25', 'Machine')

# Set MAVEN_HOME to mvnd location
[System.Environment]::SetEnvironmentVariable('MAVEN_HOME', 'C:\Program Files\Apache\maven-mvnd-1.0.3-windows-amd64', 'Machine')

# Add to PATH
$currentPath = [System.Environment]::GetEnvironmentVariable('Path', 'Machine')
if ($currentPath -notlike "*maven-mvnd*") {
    $newPath = $currentPath + ';C:\Program Files\Apache\maven-mvnd-1.0.3-windows-amd64\bin;%JAVA_HOME%\bin'
    [System.Environment]::SetEnvironmentVariable('Path', $newPath, 'Machine')
}

Write-Host "Environment variables set successfully!" -ForegroundColor Green
Write-Host "Please close and reopen your terminal/PowerShell" -ForegroundColor Yellow
```

### Option B: Using GUI

1. Right-click **This PC** → **Properties**
2. Click **Advanced system settings**
3. Click **Environment Variables**
4. Add/Edit these **System variables**:
   - `JAVA_HOME` = `C:\Program Files\Java\jdk-25`
   - `MAVEN_HOME` = `C:\Program Files\Apache\maven-mvnd-1.0.3-windows-amd64`
5. Edit **Path** variable and add:
   - `C:\Program Files\Apache\maven-mvnd-1.0.3-windows-amd64\bin`
   - `%JAVA_HOME%\bin`
6. Click **OK** on all dialogs

## Step 3: Verify Installation

**IMPORTANT:** Close and reopen your Command Prompt or PowerShell.

```cmd
mvnd --version
```

Expected output:
```
Apache Maven Daemon (mvnd) 1.0.3 windows-amd64 native client
Terminal: ...
Java version: 25, vendor: Oracle Corporation
Java home: C:\Program Files\Java\jdk-25
```

Also verify:
```cmd
java -version
echo %JAVA_HOME%
echo %MAVEN_HOME%
```

## Step 4: Build Your Spring Boot Application

Now you can build using `mvnd` (note: it's `mvnd`, not `mvn`):

```cmd
cd C:\apps\sample-spring-boot-app
mvnd clean package
```

**Note:** The first run will download dependencies and may take a few minutes. Subsequent builds will be much faster due to mvnd's daemon architecture.

## Step 5: Run the Application

After successful build:

```cmd
start-with-apm.bat
```

## Using mvnd vs mvn

Maven Daemon (mvnd) uses the same commands as Maven, just replace `mvn` with `mvnd`:

```cmd
# Standard Maven commands
mvn clean package      →  mvnd clean package
mvn install            →  mvnd install
mvn test               →  mvnd test
mvn spring-boot:run    →  mvnd spring-boot:run
```

## Troubleshooting

### Issue: "mvnd is not recognized"

**Solution:**
1. Close and reopen Command Prompt/PowerShell
2. Verify PATH: `echo %PATH%` (should contain mvnd bin directory)
3. Try full path: `"C:\Program Files\Apache\maven-mvnd-1.0.3-windows-amd64\bin\mvnd.exe" --version`

### Issue: "JAVA_HOME not set" or Java version error

**Solution:**
```cmd
setx JAVA_HOME "C:\Program Files\Java\jdk-25" /M
```
Then restart terminal.

### Issue: mvnd daemon fails to start

**Solution:**
1. Ensure JAVA_HOME points to JDK (not JRE)
2. Check: `"%JAVA_HOME%\bin\java.exe" -version`
3. Kill any existing mvnd daemons: `mvnd --stop`
4. Try again: `mvnd --version`

### Issue: Build fails with "Could not find or load main class"

**Solution:**
Ensure you're in the correct directory:
```cmd
cd C:\apps\sample-spring-boot-app
dir pom.xml
```
You should see `pom.xml` in the current directory.

## mvnd Benefits

Maven Daemon is faster because:
- Keeps JVM warm (no startup time for subsequent builds)
- Parallel builds by default
- Smart caching

First build might be similar speed to regular Maven, but subsequent builds will be significantly faster.

## Complete Setup Verification Checklist

Run these commands to verify everything is set up:

```cmd
# 1. Check Java
java -version
echo %JAVA_HOME%

# 2. Check mvnd
mvnd --version
echo %MAVEN_HOME%

# 3. Check PATH
echo %PATH%

# 4. Navigate to project
cd C:\apps\sample-spring-boot-app

# 5. Verify pom.xml exists
dir pom.xml

# 6. Build the application
mvnd clean package

# 7. Verify JAR was created
dir target\demo-0.0.1-SNAPSHOT.jar
```

If all commands succeed, you're ready to run the application!

## Alternative: If You Need Standard Maven

If you encounter issues with mvnd and need standard Maven instead:

```powershell
# Download standard Maven
Invoke-WebRequest -Uri "https://dlcdn.apache.org/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.zip" -OutFile "$env:TEMP\apache-maven-standard.zip"

# Extract
Expand-Archive -Path "$env:TEMP\apache-maven-standard.zip" -DestinationPath "C:\Program Files\Apache\"

# Update MAVEN_HOME
[System.Environment]::SetEnvironmentVariable('MAVEN_HOME', 'C:\Program Files\Apache\apache-maven-3.9.6', 'Machine')

# Update PATH (remove mvnd, add maven)
# Then use `mvn` instead of `mvnd`
```

## Summary - Next Steps

1. ✅ Downloaded mvnd
2. ⬜ Extract to `C:\Program Files\Apache\`
3. ⬜ Set environment variables (JAVA_HOME, MAVEN_HOME, PATH)
4. ⬜ Close and reopen terminal
5. ⬜ Verify with `mvnd --version`
6. ⬜ Build with `mvnd clean package`
7. ⬜ Run with `start-with-apm.bat`
