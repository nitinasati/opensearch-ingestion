#!/bin/bash

# =============================================================================
# OpenSearch Alias Switch Script
# =============================================================================
#
# Purpose:
#   This script automates the process of switching aliases in OpenSearch.
#   It handles the execution of the switch_alias.py script with proper error handling,
#   logging, and status reporting.
#
# Features:
#   - Automated alias switching process
#   - Comprehensive error handling and logging
#   - Pre-execution validation checks
#   - Detailed execution status reporting
#   - Configurable parameters
#
# Usage:
#   ./run_switch_alias.sh --alias <alias_name> --old-index <old_index> --new-index <new_index>
#
# Configuration:
#   The script uses the following default parameters:
#   - Alias: member_index
#   - Old Index: member_index_primary
#   - New Index: member_index_primary_new
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
ALIAS="member_search_alias"
NEW_INDEX="member_index_secondary"
OLD_INDEX="member_index_primary"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --alias)
            ALIAS="$2"
            shift 2
            ;;
        --old-index)
            OLD_INDEX="$2"
            shift 2
            ;;
        --new-index)
            NEW_INDEX="$2"
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
log_message "INFO" "  Alias: ${ALIAS}"
log_message "INFO" "  Old Index: ${OLD_INDEX}"
log_message "INFO" "  New Index: ${NEW_INDEX}"

# Execute the command and capture output
log_message "INFO" "Starting alias switch process..."
TEMP_OUTPUT=$(mktemp)
if ! python ../switch_alias.py \
    --alias "$ALIAS" \
    --source "$OLD_INDEX" \
    --target "$NEW_INDEX" 2>&1 | tee "$TEMP_OUTPUT" "$LOG_FILE"; then
    handle_error "Alias switch process failed"
fi

# Check for error messages in the output
if grep -q "ERROR" "$TEMP_OUTPUT" || grep -q "Failed to switch alias" "$TEMP_OUTPUT"; then
    log_message "ERROR" "Alias switch process failed - check logs for details"
    rm -f "$TEMP_OUTPUT"
    exit 1
fi

# Clean up temporary file
rm -f "$TEMP_OUTPUT"

log_message "INFO" "Alias switch process completed successfully"
exit 0 