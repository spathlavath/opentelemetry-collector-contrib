@echo off
REM Example script to set environment variables for Windows
REM Copy this file to set-env.bat and update with your actual values

REM ==========================================
REM Environment Variables Configuration
REM ==========================================

REM New Relic License Key
set NEW_RELIC_LICENSE_KEY=your-newrelic-license-key-here

REM SQL Server Connection Details
set SQLSERVER_HOST=localhost
set SQLSERVER_USERNAME=sa
set SQLSERVER_PASSWORD=your-password-here
set SQLSERVER_DATABASE=master

echo ==========================================
echo Environment Variables Set
echo ==========================================
echo NEW_RELIC_LICENSE_KEY=%NEW_RELIC_LICENSE_KEY%
echo SQLSERVER_HOST=%SQLSERVER_HOST%
echo SQLSERVER_USERNAME=%SQLSERVER_USERNAME%
echo SQLSERVER_PASSWORD=********
echo SQLSERVER_DATABASE=%SQLSERVER_DATABASE%
echo ==========================================
echo.
echo Variables set for current session.
echo To make permanent, run:
echo   setx NEW_RELIC_LICENSE_KEY "%NEW_RELIC_LICENSE_KEY%"
echo   setx SQLSERVER_HOST "%SQLSERVER_HOST%"
echo   setx SQLSERVER_USERNAME "%SQLSERVER_USERNAME%"
echo   setx SQLSERVER_PASSWORD "%SQLSERVER_PASSWORD%"
echo   setx SQLSERVER_DATABASE "%SQLSERVER_DATABASE%"
echo.
