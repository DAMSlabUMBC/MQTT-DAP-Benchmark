#!/bin/bash
# Processes logs in parallel to speed things up
# Analysis takes a while so we do multiple at once

set -e

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <logs_directory> [max_parallel_jobs]"
    echo "Example: $0 logs/unified"
    echo "Example: $0 logs/unified 8  # Run 8 at once"
    exit 1
fi

LOGS_DIR="$1"
MAX_PARALLEL="${2:-4}"  # Default to 4 jobs at once

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

echo "================================================================================"
echo "PARALLEL LOG ANALYSIS"
echo "================================================================================"
echo "Logs directory: $LOGS_DIR"
echo "Max parallel jobs: $MAX_PARALLEL"
echo "Start time: $(date)"
echo ""

LOG_FILES=$(find "$LOGS_DIR" -name "*.log" -type f | sort)

if [ -z "$LOG_FILES" ]; then
    echo "${RED}ERROR: No log files found in $LOGS_DIR${NC}"
    exit 1
fi

TOTAL_LOGS=$(echo "$LOG_FILES" | wc -l | tr -d ' ')
echo "Found $TOTAL_LOGS log file(s) to analyze"
echo "================================================================================"
echo ""

RESULTS_DIR="results/$(basename $LOGS_DIR)"
mkdir -p "$RESULTS_DIR"

# Process a single log file
analyze_log() {
    local LOG_FILE="$1"
    local LOG_NAME=$(basename "$LOG_FILE" .log)
    local RESULTS_CSV="$RESULTS_DIR/${LOG_NAME}_metrics.csv"

    echo "${YELLOW}Analyzing${NC}: $LOG_NAME"

    if python3 benchmark/Benchmark.py analyze "$LOG_FILE" -o "$RESULTS_CSV" > /dev/null 2>&1; then
        echo "${GREEN}✓ Complete${NC}: $LOG_NAME"
        return 0
    else
        echo "${RED}✗ Failed${NC}: $LOG_NAME"
        return 1
    fi
}

export -f analyze_log
export RESULTS_DIR GREEN RED YELLOW NC

echo "Starting parallel analysis..."
echo ""

# Use xargs to run multiple analyses at once
if command -v xargs &> /dev/null; then
    echo "$LOG_FILES" | xargs -P "$MAX_PARALLEL" -I {} bash -c 'analyze_log "$@"' _ {}
else
    # If xargs isn't available, just do them one at a time
    for LOG_FILE in $LOG_FILES; do
        analyze_log "$LOG_FILE"
    done
fi

echo ""
echo "================================================================================"
echo "ANALYSIS COMPLETE"
echo "================================================================================"
echo "End time: $(date)"
echo ""
echo "Output directory: $RESULTS_DIR"
echo "================================================================================"
