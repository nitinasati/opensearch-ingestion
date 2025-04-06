@echo off
echo Running SonarQube Analysis...

REM Configuration
set SONAR_HOST_URL=http://localhost:9000
set SONAR_TOKEN=squ_976e24a0c433eea1fd7ef6b6d0acba4cbc3f4a3d
set PROJECT_KEY=opensearch-ingestion

REM Check if SonarQube scanner is installed
where sonar-scanner >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo SonarQube Scanner is not installed or not in PATH. Please install it first.
    echo Download from: https://docs.sonarqube.org/latest/analyzing-source-code/scanners/sonarscanner/
    goto :end
)

REM Check if SonarQube server is running
echo Checking SonarQube server status...
curl -s "%SONAR_HOST_URL%/api/system/status" | findstr "UP" >nul
if %ERRORLEVEL% NEQ 0 (
    echo SonarQube server is not running. Please start the server first.
    goto :end
)

REM Run the analysis
echo Starting SonarQube analysis...
sonar-scanner -D "sonar.host.url=%SONAR_HOST_URL%" -D "sonar.token=%SONAR_TOKEN%" -D "sonar.projectKey=%PROJECT_KEY%"

if %ERRORLEVEL% NEQ 0 (
    echo SonarQube analysis failed with exit code %ERRORLEVEL%
    goto :end
)

echo SonarQube analysis completed successfully!
echo You can view the results at: %SONAR_HOST_URL%/dashboard?id=%PROJECT_KEY%

:end
pause 