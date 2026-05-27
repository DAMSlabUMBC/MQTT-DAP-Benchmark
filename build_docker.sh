#!/bin/bash
# Builds all the Docker images we need for testing
# Just run this once before running tests

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "================================================================================"
echo "BUILDING ALL DOCKER IMAGES"
echo "================================================================================"
echo ""

# Build the unified MQTT-DAP broker and the test runner
echo "${YELLOW}Building unified MQTT-DAP broker and benchmark runner...${NC}"
echo "This will take 5-10 minutes the first time..."
docker compose -f docker-compose-unified.yml build

echo ""
echo "${GREEN}✓ Unified broker and runner built${NC}"
echo ""

echo "================================================================================"
echo "BUILD COMPLETE"
echo "================================================================================"
echo ""
echo "Next steps:"
echo "  Run the suite:        ./run_all_broker_tests.sh"
echo "  Run tests only:       ./run_pm_tests.sh"
echo ""
