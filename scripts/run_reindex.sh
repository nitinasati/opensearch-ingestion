#!/bin/bash

# =============================================================================
# OpenSearch Reindex Script
# =============================================================================
#
# Purpose:
#   This script automates the process of reindexing data in OpenSearch.
#   It handles the execution of the reindex.py script with proper error handling,
#   logging, and status reporting.
#
# Features:
#   - Automated reindexing process
#   - Comprehensive error handling and logging
#   - Pre-execution validation checks
#   - Detailed execution status reporting
#   - Configurable parameters
#
# Usage:
#   ./run_reindex.sh --source-index <source_index> --target-index <target_index> [--batch-size <size>] [--max-workers <workers>]
#
# Configuration:
#   The script uses the following default parameters:
#   - Source Index: member_index_primary
#   - Target Index: member_index_primary_new
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
SOURCE_INDEX="member_index_primary"
TARGET_INDEX="member_index_primary_new"
BATCH_SIZE=1000
MAX_WORKERS=2

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --source-index)
            SOURCE_INDEX="$2"
            shift 2
            ;;
        --target-index)
            TARGET_INDEX="$2"
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
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

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
if ! python -c "import boto3, requests" &> /dev/null; then
    handle_error "Required Python packages are not installed"
fi
log_message "INFO" "Required Python packages verified"

# Log configuration
log_message "INFO" "Configuration:"
log_message "INFO" "  Source Index: ${SOURCE_INDEX}"
log_message "INFO" "  Target Index: ${TARGET_INDEX}"
log_message "INFO" "  Batch Size: ${BATCH_SIZE}"
log_message "INFO" "  Max Workers: ${MAX_WORKERS}"

# Execute the command
log_message "INFO" "Starting reindex process..."
if ! python ../reindex.py \
    --source-index "$SOURCE_INDEX" \
    --target-index "$TARGET_INDEX" \
    --batch-size "$BATCH_SIZE" \
    --max-workers "$MAX_WORKERS" 2>&1 | tee -a "$LOG_FILE"; then
    handle_error "Reindex process failed"
fi

log_message "INFO" "Reindex process completed successfully"
exit 0 