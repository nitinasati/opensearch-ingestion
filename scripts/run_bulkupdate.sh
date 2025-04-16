#!/bin/bash

# =============================================================================
# OpenSearch Bulk Update Script
# =============================================================================
#
# Purpose:
#   This script automates the process of bulk updating data in OpenSearch.
#   It handles the execution of the bulkupdate.py script with proper error handling,
#   logging, and status reporting.
#
# Features:
#   - Automated bulk update process
#   - Comprehensive error handling and logging
#   - Pre-execution validation checks
#   - Detailed execution status reporting
#   - Configurable parameters
#
# Usage:
#   ./run_bulkupdate.sh --index <index_name> [--bucket <bucket_name> --prefix <prefix>] [--local-files <file1> <file2> ...] [--local-folder <folder_path>] [--batch-size <size>] [--max-workers <workers>] [--resume] [--fresh-load]
#
# Configuration:
#   The script uses the following default parameters:
#   - Batch Size: 1000
#   - Max Workers: 2
#
# Logging:
#   - Creates timestamped log files in the logs directory
#   - Logs both console output and detailed execution information
#   - Maintains separate log files for each execution
#
# Exit Codes:
#   0 - Success
#   1 - Error (Python not installed, missing dependencies, or execution failure)
#
# =============================================================================

# Set default values
BATCH_SIZE=1000
MAX_WORKERS=2
RESUME=false
FRESH_LOAD=true
INDEX="member_index_primary"
BUCKET="openlpocbucket25"
PREFIX="opensearch/"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --index)
            INDEX="$2"
            shift 2
            ;;
        --bucket)
            BUCKET="$2"
            shift 2
            ;;
        --prefix)
            PREFIX="$2"
            shift 2
            ;;
        --local-files)
            shift
            LOCAL_FILES=()
            while [[ $# -gt 0 ]] && [[ $1 != --* ]]; do
                LOCAL_FILES+=("$1")
                shift
            done
            ;;
        --local-folder)
            LOCAL_FOLDER="$2"
            shift 2
            ;;
        --batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        --max-workers)
            MAX_WORKERS="$2"
            shift 2
            ;;
        --resume)
            RESUME=true
            FRESH_LOAD=false
            shift
            ;;
        --fresh-load)
            FRESH_LOAD=true
            RESUME=false
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [ -z "$INDEX" ]; then
    echo "Error: --index is required"
    exit 1
fi

# Validate that at least one data source is provided
if [ -z "$BUCKET" ] && [ -z "$LOCAL_FILES" ] && [ -z "$LOCAL_FOLDER" ]; then
    echo "Error: At least one of --bucket, --local-files, or --local-folder must be provided"
    exit 1
fi

# Set up logging
SCRIPT_NAME=$(basename "$0" .sh)
LOG_DIR="../logs"
LOG_FILE="${LOG_DIR}/${SCRIPT_NAME}_$(date '+%Y%m%d_%H%M%S').log"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Function to log messages
log_message() {
    local level=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[${timestamp}] [${level}] ${message}" | tee -a "$LOG_FILE"
}

# Function to handle errors
handle_error() {
    log_message "ERROR" "$1"
    exit 1
}

# Log script start
log_message "INFO" "Starting ${SCRIPT_NAME} script"
log_message "INFO" "Log file: ${LOG_FILE}"

# Check if Python is installed
if ! command -v python &> /dev/null; then
    handle_error "Python is not installed"
fi
log_message "INFO" "Python installation verified"

# Check if required Python packages are installed
if ! python -c "import boto3, requests, pandas" &> /dev/null; then
    handle_error "Required Python packages are not installed"
fi
log_message "INFO" "Required Python packages verified"

# Log configuration
log_message "INFO" "Configuration:"
log_message "INFO" "  Index: ${INDEX}"
if [ -n "$BUCKET" ]; then
    log_message "INFO" "  Bucket: ${BUCKET}"
    log_message "INFO" "  Prefix: ${PREFIX}"
fi
if [ -n "$LOCAL_FILES" ]; then
    log_message "INFO" "  Local Files: ${LOCAL_FILES[*]}"
fi
if [ -n "$LOCAL_FOLDER" ]; then
    log_message "INFO" "  Local Folder: ${LOCAL_FOLDER}"
fi
log_message "INFO" "  Batch Size: ${BATCH_SIZE}"
log_message "INFO" "  Max Workers: ${MAX_WORKERS}"
log_message "INFO" "  Resume: ${RESUME}"
log_message "INFO" "  Fresh Load: ${FRESH_LOAD}"

# Execute the command and capture output
log_message "INFO" "Starting bulk update process..."
TEMP_OUTPUT=$(mktemp)

# Build the command
CMD="python ../bulkupdate.py --index ${INDEX} --batch-size ${BATCH_SIZE} --max-workers ${MAX_WORKERS}"
if [ -n "$BUCKET" ]; then
    CMD="${CMD} --bucket ${BUCKET} --prefix ${PREFIX}"
fi
if [ -n "$LOCAL_FILES" ]; then
    CMD="${CMD} --local-files ${LOCAL_FILES[*]}"
fi
if [ -n "$LOCAL_FOLDER" ]; then
    CMD="${CMD} --local-folder ${LOCAL_FOLDER}"
fi
if [ "$RESUME" = true ]; then
    CMD="${CMD} --resume"
fi
if [ "$FRESH_LOAD" = true ]; then
    CMD="${CMD} --fresh-load"
fi

# Execute the command and capture output
if ! $CMD 2>&1 | tee "$TEMP_OUTPUT" "$LOG_FILE"; then
    handle_error "Bulk update process failed"
fi

# Check for error messages in the output
if grep -q "ERROR" "$TEMP_OUTPUT" || \
   grep -q "Failed to ingest data" "$TEMP_OUTPUT" || \
   grep -q "Document count mismatch" "$TEMP_OUTPUT" || \
   grep -q "Expected documents: [0-9]*, Actual documents: [0-9]*" "$TEMP_OUTPUT" || \
   grep -q "Failed to process" "$TEMP_OUTPUT"; then
    log_message "ERROR" "Bulk update process failed - check logs for details"
    rm -f "$TEMP_OUTPUT"
    exit 1
fi

# Clean up temporary file
rm -f "$TEMP_OUTPUT"

log_message "INFO" "Bulk update process completed successfully"
exit 0 