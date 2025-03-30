# Create log directory if it doesn't exist
$logDir = "log"
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir
}

# Set up logging
$timestamp = Get-Date -Format "yyyyMMdd"
$logFile = Join-Path $logDir "run_ingestion_$timestamp.log"

function Write-Log {
    param($Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "$timestamp - $Message"
    Write-Host $logMessage
    Add-Content -Path $logFile -Value $logMessage
}

function Test-CommandSuccess {
    param($LastExitCode)
    if ($LastExitCode -ne 0) {
        Write-Log "Error: Command failed with exit code $LastExitCode"
        return $false
    }
    return $true
}

# Start timing the entire process
$totalStartTime = Get-Date

# Check if Python is installed
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Log "Error: Python is not installed or not in PATH"
    exit 1
}

# Check if .env file exists
if (-not (Test-Path ".env")) {
    Write-Log "Error: .env file not found"
    exit 1
}

Write-Log "Starting OpenSearch ingestion process..."

# Step 1: Copy Primary Index to Secondary
Write-Log "Step 1: Copying Primary Index to Secondary"
$step1StartTime = Get-Date
python reindex.py --source my_index_primary --target my_index_secondary
if (-not (Test-CommandSuccess $LASTEXITCODE)) {
    Write-Log "Error: Failed to copy primary index to secondary"
    exit 1
}
$step1Duration = (Get-Date) - $step1StartTime
Write-Log "Step 1 completed in $($step1Duration.TotalSeconds) seconds"

# Wait 5 seconds between steps
Start-Sleep -Seconds 5

# Step 2: Switch Alias to Secondary
Write-Log "Step 2: Switching Alias to Secondary"
$step2StartTime = Get-Date
python switch_alias.py --alias my_index_alias --source my_index_primary --target my_index_secondary
if (-not (Test-CommandSuccess $LASTEXITCODE)) {
    Write-Log "Error: Failed to switch alias to secondary"
    exit 1
}
$step2Duration = (Get-Date) - $step2StartTime
Write-Log "Step 2 completed in $($step2Duration.TotalSeconds) seconds"

# Wait 5 seconds between steps
Start-Sleep -Seconds 5

# Step 3: Bulk Update Primary Index
Write-Log "Step 3: Bulk Updating Primary Index"
$step3StartTime = Get-Date
python bulkupdate.py --bucket openlpocbucket --prefix opensearch/ --index my_index_primary --batch-size 1000
if (-not (Test-CommandSuccess $LASTEXITCODE)) {
    Write-Log "Error: Failed to bulk update primary index"
    exit 1
}
$step3Duration = (Get-Date) - $step3StartTime
Write-Log "Step 3 completed in $($step3Duration.TotalSeconds) seconds"

# Wait 5 seconds between steps
Start-Sleep -Seconds 5

# Step 4: Switch Alias back to Primary
Write-Log "Step 4: Switching Alias back to Primary"
$step4StartTime = Get-Date
python switch_alias.py --alias my_index_alias --source my_index_secondary --target my_index_primary
if (-not (Test-CommandSuccess $LASTEXITCODE)) {
    Write-Log "Error: Failed to switch alias back to primary"
    exit 1
}
$step4Duration = (Get-Date) - $step4StartTime
Write-Log "Step 4 completed in $($step4Duration.TotalSeconds) seconds"

# Calculate and display total time
$totalDuration = (Get-Date) - $totalStartTime
Write-Log "----------------------------------------"
Write-Log "Process completed successfully!"
Write-Log "Total time taken: $($totalDuration.TotalSeconds) seconds"
Write-Log "Step 1 (Copy): $($step1Duration.TotalSeconds) seconds"
Write-Log "Step 2 (Switch to Secondary): $($step2Duration.TotalSeconds) seconds"
Write-Log "Step 3 (Bulk Update): $($step3Duration.TotalSeconds) seconds"
Write-Log "Step 4 (Switch to Primary): $($step4Duration.TotalSeconds) seconds"
Write-Log "----------------------------------------"
exit 0 