@echo off
setlocal enabledelayedexpansion

REM Create log directory if it doesn't exist
if not exist "log" mkdir log

REM Set up logging
set YYYYMMDD=%date:~-4,4%%date:~-10,2%%date:~-7,2%
set logFile=log\run_ingestion_%YYYYMMDD%.log

REM Start timing the entire process
set "totalStartTime=%time%"

REM Check if Python is installed
where python >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo %date% %time% - Error: Python is not installed or not in PATH
    echo %date% %time% - Error: Python is not installed or not in PATH >> "%logFile%"
    exit /b 1
)

REM Check if .env file exists
if not exist ".env" (
    echo %date% %time% - Error: .env file not found
    echo %date% %time% - Error: .env file not found >> "%logFile%"
    exit /b 1
)

echo %date% %time% - Starting OpenSearch ingestion process...
echo %date% %time% - Starting OpenSearch ingestion process... >> "%logFile%"

REM Step 1: Copy Primary Index to Secondary
echo %date% %time% - Step 1: Copying Primary Index to Secondary
echo %date% %time% - Step 1: Copying Primary Index to Secondary >> "%logFile%"
set "step1StartTime=%time%"
python reindex.py --source member_index_primary --target member_index_secondary
if %ERRORLEVEL% NEQ 0 (
    echo %date% %time% - Error: Failed to copy primary index to secondary
    echo %date% %time% - Error: Failed to copy primary index to secondary >> "%logFile%"
    exit /b 1
)
set "step1EndTime=%time%"
echo %date% %time% - Step 1 completed
echo %date% %time% - Step 1 completed >> "%logFile%"

REM Wait 5 seconds between steps
timeout /t 5 /nobreak >nul

REM Step 2: Switch Alias to Secondary
echo %date% %time% - Step 2: Switching Alias to Secondary
echo %date% %time% - Step 2: Switching Alias to Secondary >> "%logFile%"
set "step2StartTime=%time%"
python switch_alias.py --alias member_search_alias --source member_index_primary --target member_index_secondary
if %ERRORLEVEL% NEQ 0 (
    echo %date% %time% - Error: Failed to switch alias to secondary
    echo %date% %time% - Error: Failed to switch alias to secondary >> "%logFile%"
    exit /b 1
)
set "step2EndTime=%time%"
echo %date% %time% - Step 2 completed
echo %date% %time% - Step 2 completed >> "%logFile%"

timeout /t 5 /nobreak >nul

REM Step 3: Bulk Update Primary Index with local file
echo %date% %time% - Step 3: Bulk Updating Primary Index with local file
echo %date% %time% - Step 3: Bulk Updating Primary Index with local file >> "%logFile%"
set "step3StartTime=%time%"
python bulkupdate.py --local-files ./testdata/member_data1.json --index member_index_primary --batch-size 1000 --max-workers 4
if %ERRORLEVEL% NEQ 0 (
    echo %date% %time% - Error: Failed to bulk update primary index
    echo %date% %time% - Error: Failed to bulk update primary index >> "%logFile%"
    exit /b 1
)
set "step3EndTime=%time%"
echo %date% %time% - Step 3 (local file) completed
echo %date% %time% - Step 3 (local file) completed >> "%logFile%"

timeout /t 5 /nobreak >nul

REM Step 3: Bulk Update Primary Index with S3 bucket
echo %date% %time% - Step 3: Bulk Updating Primary Index with S3 bucket
echo %date% %time% - Step 3: Bulk Updating Primary Index with S3 bucket >> "%logFile%"
set "step3StartTime=%time%"
python bulkupdate.py --bucket openlpocbucket25 --prefix opensearch/ --index member_index_primary --batch-size 1000 --resume
if %ERRORLEVEL% NEQ 0 (
    echo %date% %time% - Error: Failed to bulk update primary index
    echo %date% %time% - Error: Failed to bulk update primary index >> "%logFile%"
    exit /b 1
)
set "step3EndTime=%time%"
echo %date% %time% - Step 3 (S3 bucket) completed
echo %date% %time% - Step 3 (S3 bucket) completed >> "%logFile%"

timeout /t 5 /nobreak >nul

REM Step 3: Bulk Update Primary Index with local folder
echo %date% %time% - Step 3: Bulk Updating Primary Index with local folder
echo %date% %time% - Step 3: Bulk Updating Primary Index with local folder >> "%logFile%"
set "step3StartTime=%time%"
python bulkupdate.py --local-folder ./testdata --index member_index_primary --batch-size 1000 --max-workers 4
if %ERRORLEVEL% NEQ 0 (
    echo %date% %time% - Error: Failed to bulk update primary index
    echo %date% %time% - Error: Failed to bulk update primary index >> "%logFile%"
    exit /b 1
)
set "step3EndTime=%time%"
echo %date% %time% - Step 3 (local folder) completed
echo %date% %time% - Step 3 (local folder) completed >> "%logFile%"

timeout /t 5 /nobreak >nul

REM Step 3: Bulk Update Primary Index with local folder and resume
echo %date% %time% - Step 3: Bulk Updating Primary Index with local folder and resume
echo %date% %time% - Step 3: Bulk Updating Primary Index with local folder and resume >> "%logFile%"
set "step3StartTime=%time%"
python bulkupdate.py --local-folder ./testdata --index member_index_primary --batch-size 1000 --max-workers 4 --resume
if %ERRORLEVEL% NEQ 0 (
    echo %date% %time% - Error: Failed to bulk update primary index
    echo %date% %time% - Error: Failed to bulk update primary index >> "%logFile%"
    exit /b 1
)
set "step3EndTime=%time%"
echo %date% %time% - Step 3 (local folder with resume) completed
echo %date% %time% - Step 3 (local folder with resume) completed >> "%logFile%"

timeout /t 5 /nobreak >nul

REM Step 4: Switch Alias back to Primary
echo %date% %time% - Step 4: Switching Alias back to Primary
echo %date% %time% - Step 4: Switching Alias back to Primary >> "%logFile%"
set "step4StartTime=%time%"
python switch_alias.py --alias member_search_alias --source member_index_secondary --target member_index_primary
if %ERRORLEVEL% NEQ 0 (
    echo %date% %time% - Error: Failed to switch alias back to primary
    echo %date% %time% - Error: Failed to switch alias back to primary >> "%logFile%"
    exit /b 1
)
set "step4EndTime=%time%"
echo %date% %time% - Step 4 completed
echo %date% %time% - Step 4 completed >> "%logFile%"

REM Calculate and display total time
set "totalEndTime=%time%"
echo %date% %time% - ----------------------------------------
echo %date% %time% - ---------------------------------------- >> "%logFile%"
echo %date% %time% - Process completed successfully!
echo %date% %time% - Process completed successfully! >> "%logFile%"
echo %date% %time% - Total time taken: %totalStartTime% to %totalEndTime%
echo %date% %time% - Total time taken: %totalStartTime% to %totalEndTime% >> "%logFile%"
echo %date% %time% - ----------------------------------------
echo %date% %time% - ---------------------------------------- >> "%logFile%"

exit /b 0 