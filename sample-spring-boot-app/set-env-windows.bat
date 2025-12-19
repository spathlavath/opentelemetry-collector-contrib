@echo off
echo ==========================================
echo Set Environment Variables for Windows
echo ==========================================
echo.

REM Set New Relic License Key
echo Enter your New Relic License Key:
set /p NEW_RELIC_LICENSE_KEY="License Key: "

REM Set SQL Server connection details
echo.
echo Enter SQL Server Host (or press Enter for default: localhost):
set /p SQLSERVER_HOST="Host: "
if "%SQLSERVER_HOST%"=="" set SQLSERVER_HOST=localhost

echo Enter SQL Server Username (or press Enter for default: sa):
set /p SQLSERVER_USERNAME="Username: "
if "%SQLSERVER_USERNAME%"=="" set SQLSERVER_USERNAME=sa

echo Enter SQL Server Password:
set /p SQLSERVER_PASSWORD="Password: "

echo Enter SQL Server Database (or press Enter for default: master):
set /p SQLSERVER_DATABASE="Database: "
if "%SQLSERVER_DATABASE%"=="" set SQLSERVER_DATABASE=master

echo.
echo ==========================================
echo Environment Variables Set (Current Session)
echo ==========================================
echo NEW_RELIC_LICENSE_KEY=%NEW_RELIC_LICENSE_KEY%
echo SQLSERVER_HOST=%SQLSERVER_HOST%
echo SQLSERVER_USERNAME=%SQLSERVER_USERNAME%
echo SQLSERVER_PASSWORD=********
echo SQLSERVER_DATABASE=%SQLSERVER_DATABASE%
echo.

echo These variables are set for the current session only.
echo.
echo To set permanently, would you like to use setx? (Y/N)
set /p PERMANENT="Choice: "

if /i "%PERMANENT%"=="Y" (
    echo.
    echo Setting permanent environment variables...
    setx NEW_RELIC_LICENSE_KEY "%NEW_RELIC_LICENSE_KEY%"
    setx SQLSERVER_HOST "%SQLSERVER_HOST%"
    setx SQLSERVER_USERNAME "%SQLSERVER_USERNAME%"
    setx SQLSERVER_PASSWORD "%SQLSERVER_PASSWORD%"
    setx SQLSERVER_DATABASE "%SQLSERVER_DATABASE%"
    echo.
    echo Permanent environment variables set successfully!
    echo Please restart your command prompt for changes to take effect.
) else (
    echo.
    echo Variables set for current session only.
    echo Run this script again or use 'setx' manually to set permanently.
)

echo.
echo ==========================================
echo.
pause
