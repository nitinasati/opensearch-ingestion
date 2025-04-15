#!/bin/bash

# =============================================================================
# OpenSearch Bulk Update Script
# =============================================================================
#
# Purpose:
#   This script automates the process of bulk updating data in OpenSearch from S3.
#   It handles the execution of the bulkupdate.py script with proper error handling,
#   logging, and status reporting.
#
# Features:
#   - Automated execution of bulk update process
#   - Comprehensive error handling and logging
#   - Pre-execution validation checks
#   - Detailed execution status reporting
#   - Configurable parameters
#
# Usage:
#   ./run_bulkupdate.sh
#
# Configuration:
#   The script uses the following default parameters:
#   - Bucket: openlpocbucket
#   - Prefix: opensearch/
#   - Index: member_index_primary
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
BUCKET="openlpocbucket"
PREFIX="opensearch/"
INDEX="member_index_primary"
BATCH_SIZE=1000
MAX_WORKERS=2

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
if ! python -c "import boto3, pandas, requests" &> /dev/null; then
    handle_error "Required Python packages are not installed"
fi
log_message "INFO" "Required Python packages verified"

# Log configuration
log_message "INFO" "Configuration:"
log_message "INFO" "  Bucket: ${BUCKET}"
log_message "INFO" "  Prefix: ${PREFIX}"
log_message "INFO" "  Index: ${INDEX}"
log_message "INFO" "  Batch Size: ${BATCH_SIZE}"
log_message "INFO" "  Max Workers: ${MAX_WORKERS}"

# Execute the command
log_message "INFO" "Starting bulk update process..."
if ! python ../bulkupdate.py \
    --bucket "$BUCKET" \
    --prefix "$PREFIX" \
    --index "$INDEX" \
    --batch-size "$BATCH_SIZE" \
    --max-workers "$MAX_WORKERS" 2>&1 | tee -a "$LOG_FILE"; then
    handle_error "Bulk update process failed"
fi

log_message "INFO" "Bulk update process completed successfully"
exit 0 