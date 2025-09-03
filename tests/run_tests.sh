#!/bin/bash
# Script to run brewery pipeline tests

set -e

echo "=== Brewery Pipeline Test Suite ==="
echo

# Setup Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Run different test suites
run_tests() {
    local test_type=$1
    local markers=$2
    
    echo "Running $test_type tests..."
    if [ -z "$markers" ]; then
        pytest tests/
    else
        pytest tests/ -m "$markers"
    fi
    echo
}

# Parse command line arguments
case "${1:-all}" in
    unit)
        run_tests "unit" "unit"
        ;;
    integration)
        run_tests "integration" "integration"
        ;;
    performance)
        run_tests "performance" "performance"
        ;;
    coverage)
        echo "Running tests with coverage report..."
        pytest tests/ --cov=src --cov-report=term-missing --cov-report=html
        echo "Coverage report generated in htmlcov/"
        ;;
    quick)
        echo "Running quick tests (excluding slow)..."
        pytest tests/ -m "not slow"
        ;;
    all)
        run_tests "all" ""
        ;;
    *)
        echo "Usage: $0 [unit|integration|performance|coverage|quick|all]"
        exit 1
        ;;
esac

echo "=== Test run completed ==="