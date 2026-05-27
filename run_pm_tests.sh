#!/bin/bash
# Runs the full unified-DAP-method test suite against the DAP broker.
# Collapsed from the old per-method script: there is now a single unified target,
# so this takes no PM-method argument.

set -e

CONFIG_DIR="test-configs"
DOCKER_COMPOSE_FILE="docker-compose-unified.yml"
BROKER_CONTAINER="benchmark-mosquitto-unified"
RUNNER_CONTAINER="benchmark-runner-unified"
RESULTS_DIR="results/unified"
LOGS_DIR="logs/unified"

# Some colors to make output easier to read
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "================================================================================"
echo "UNIFIED DAP METHOD TESTS"
echo "================================================================================"
echo "Start time: $(date)"
echo ""

# Make sure we have the directories
mkdir -p "$RESULTS_DIR" "$LOGS_DIR"

# Fire up the containers
echo "Starting unified broker and runner..."
docker compose -f "$DOCKER_COMPOSE_FILE" up -d

# Give the broker a sec to get ready
echo "Waiting for broker to initialize..."
sleep 10

# Go find all the unified test configs
CONFIG_FILES=$(find $CONFIG_DIR -name "*_unified.cfg" 2>/dev/null | sort)

if [ -z "$CONFIG_FILES" ]; then
    echo "${RED}ERROR: No unified test configs (*_unified.cfg) found in ${CONFIG_DIR}${NC}"
    docker compose -f "$DOCKER_COMPOSE_FILE" down
    exit 1
fi

TOTAL_TESTS=$(echo "$CONFIG_FILES" | wc -l | tr -d ' ')
echo "Found $TOTAL_TESTS unified test(s)"
echo "================================================================================"
echo ""

TEST_NUM=0
PASSED=0
FAILED=0

# Go through each test
for CONFIG in $CONFIG_FILES; do
    TEST_NUM=$((TEST_NUM + 1))
    CONFIG_NAME=$(basename "$CONFIG" .cfg)

    echo ""
    echo "${YELLOW}[$TEST_NUM/$TOTAL_TESTS]${NC} Running test: $CONFIG_NAME"
    echo "--------------------------------------------------------------------------------"

    # Restart the broker between tests to clear out stored purposes/clients
    if [ $TEST_NUM -gt 1 ]; then
        echo "Restarting broker to clean state..."
        docker restart "$BROKER_CONTAINER"
        sleep 5
    fi

    # Run the test (but don't analyze yet, that happens later in parallel)
    if docker exec "$RUNNER_CONTAINER" ./run_test_no_analyze.sh "$CONFIG" mosquitto 1883; then
        echo "${GREEN}✓ PASSED${NC}: $CONFIG_NAME"
        PASSED=$((PASSED + 1))

        # Grab the logs from the container
		docker exec "$RUNNER_CONTAINER" /bin/bash -c "ls /app/logs/${CONFIG_NAME}_*" | while read line; do docker cp "$RUNNER_CONTAINER":/$line "$LOGS_DIR/"; done
    else
        echo "${RED}✗ FAILED${NC}: $CONFIG_NAME"
        FAILED=$((FAILED + 1))
    fi

    sleep 2
done

# Print out the summary
echo ""
echo "================================================================================"
echo "UNIFIED TESTS COMPLETE"
echo "================================================================================"
echo "End time: $(date)"
echo ""
echo "Results:"
echo "  Total tests:   $TOTAL_TESTS"
echo "  Passed:        ${GREEN}$PASSED${NC}"
echo "  Failed:        ${RED}$FAILED${NC}"
echo ""
echo "Logs saved to: $LOGS_DIR/"
echo "================================================================================"
echo ""
echo "Next steps:"
echo "  1. Analyze logs:      ./analyze_logs.sh $LOGS_DIR"
echo "  2. Stop containers:   docker compose -f $DOCKER_COMPOSE_FILE down"
echo ""

if [ $FAILED -gt 0 ]; then
    exit 1
else
    exit 0
fi
