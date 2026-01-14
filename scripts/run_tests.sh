#!/bin/bash
# Script to run backend tests in Docker container

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}Running Backend Tests${NC}"
echo "======================================"

# Detect docker compose command (docker-compose vs docker compose)
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif command -v docker &> /dev/null && docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo -e "${RED}Error: Neither 'docker-compose' nor 'docker compose' command found${NC}"
    exit 1
fi

echo -e "${BLUE}Using: ${DOCKER_COMPOSE}${NC}"

# Check if backend container is running
if ! ${DOCKER_COMPOSE} ps backend | grep -q "Up"; then
    echo -e "${BLUE}Starting backend container...${NC}"
    ${DOCKER_COMPOSE} up -d backend
    echo -e "${GREEN}Waiting for backend to be ready...${NC}"
    sleep 5
fi

# Run tests with custom args if provided, otherwise run all tests
if [ $# -eq 0 ]; then
    echo -e "${BLUE}Executing all pytest tests...${NC}"
    ${DOCKER_COMPOSE} exec backend pytest tests/ -v --tb=short
else
    echo -e "${BLUE}Executing pytest with args: $@${NC}"
    ${DOCKER_COMPOSE} exec backend pytest "$@"
fi

# Check exit code
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
else
    echo -e "${RED}✗ Some tests failed${NC}"
    exit 1
fi
