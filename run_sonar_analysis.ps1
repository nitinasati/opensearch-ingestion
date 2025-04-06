# PowerShell script to run SonarQube analysis

# Configuration
$SONAR_HOST_URL = <sonar-url>  # Default SonarQube URL
$SONAR_TOKEN = "<sonar-token>"  # You'll need to add your SonarQube token here
$PROJECT_KEY = "opensearch-ingestion"

# Check if SonarQube scanner is installed
$scannerPath = "sonar-scanner"
$scannerExists = $null -ne (Get-Command $scannerPath -ErrorAction SilentlyContinue)

if (-not $scannerExists) {
    Write-Host "SonarQube Scanner is not installed or not in PATH. Please install it first."
    Write-Host "Download from: https://docs.sonarqube.org/latest/analyzing-source-code/scanners/sonarscanner/"
    exit 1
}

# Check if SonarQube server is running
try {
    $response = Invoke-WebRequest -Uri "$SONAR_HOST_URL/api/system/status" -Method GET -ErrorAction Stop
    if ($response.Content -notlike "*UP*") {
        Write-Host "SonarQube server is not running. Please start the server first."
        exit 1
    }
} catch {
    Write-Host "Could not connect to SonarQube server at $SONAR_HOST_URL"
    Write-Host "Please make sure SonarQube is running and accessible."
    exit 1
}

# Run the analysis
Write-Host "Starting SonarQube analysis..."
try {
    # Run sonar-scanner with the configuration file
    & sonar-scanner -D "sonar.host.url=$SONAR_HOST_URL" -D "sonar.login=$SONAR_TOKEN" -D "sonar.projectKey=$PROJECT_KEY"

    if ($LASTEXITCODE -ne 0) {
        Write-Host "SonarQube analysis failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }

    Write-Host "SonarQube analysis completed successfully!"
    Write-Host "You can view the results at: $SONAR_HOST_URL/dashboard?id=$PROJECT_KEY"
} catch {
    Write-Host "Error running SonarQube analysis: $_"
    exit 1
} 