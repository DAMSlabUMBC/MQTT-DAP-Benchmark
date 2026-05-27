#!/bin/bash
# Builds the unified DAP broker + runner, runs the full unified test suite,
# then analyzes the logs. Collapsed from the old multi-broker loop now that there
# is a single unified method/target.

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "================================================================================"
echo "COMPLETE BENCHMARK SUITE - UNIFIED DAP METHOD"
echo "================================================================================"
echo "Start time: $(date)"
echo ""

# Build the unified broker + runner
echo "${BLUE}>>> Building unified broker + runner...${NC}"
docker compose -f docker-compose-unified.yml build

# Run the suite
if ./run_pm_tests.sh; then
    RESULT="${GREEN}✓ unified${NC}"
else
    RESULT="${RED}✗ unified${NC}"
fi

# Stop and clean up
docker compose -f docker-compose-unified.yml down
sleep 5

# Analyze the logs
echo ""
echo "================================================================================"
echo "TESTS COMPLETE - STARTING LOG ANALYSIS"
echo "================================================================================"
echo ""

./analyze_logs.sh logs/unified 4

# Print the final summary
echo ""
echo "================================================================================"
echo "COMPLETE BENCHMARK SUITE - FINISHED"
echo "================================================================================"
echo "End time: $(date)"
echo ""
echo "Result: $RESULT"
echo ""
echo "Output locations:"
echo "  Logs:     logs/unified/"
echo "  Results:  results/unified/"
echo "================================================================================"
