#!/bin/bash

# =============================================================================
# OpenSearch Index Cleanup Script
# =============================================================================
#
# Purpose:
#   This script automates the process of cleaning up and validating OpenSearch indices.
#   It handles the execution of the index_cleanup.py script with proper error handling,
#   logging, and status reporting.
#
# Features:
#   - Automated index cleanup and validation
#   - Comprehensive error handling and logging
#   - Pre-execution validation checks
#   - Detailed execution status reporting
#   - Configurable parameters
#
# Usage:
#   ./run_index_cleanup.sh --index <index_name> [--force]
#
# Configuration:
#   The script uses the following default parameters:
#   - Index: member_index_primary
#   - Force: false
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
INDEX="member_index_primary"
FORCE="false"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --index)
            INDEX="$2"
            shift 2
            ;;
        --force)
            FORCE="true"
            shift
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
log_message "INFO" "  Index: ${INDEX}"
log_message "INFO" "  Force: ${FORCE}"

# Execute the command
log_message "INFO" "Starting index cleanup process..."
if ! python ../index_cleanup.py \
    --index "$INDEX" \
    $([ "$FORCE" = "true" ] && echo "--force") 2>&1 | tee -a "$LOG_FILE"; then
    handle_error "Index cleanup process failed"
fi

log_message "INFO" "Index cleanup process completed successfully"
exit 0 