#!/bin/bash
# Quick JMH benchmark status checker
# Usage: ./jmh-status.sh /tmp/jmh_benchmark.log

set -euo pipefail

# Check arguments
if [ $# -ne 1 ]; then
    echo "Usage: $0 <jmh-log-file>"
    echo "Example: $0 /tmp/jmh_scalar.log"
    exit 1
fi

LOG_FILE="$1"

# Check if log file exists
if [ ! -f "$LOG_FILE" ]; then
    echo "Error: Log file not found: $LOG_FILE"
    exit 1
fi

# ANSI color codes
BOLD='\033[1m'
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RESET='\033[0m'

echo -e "${BOLD}JMH Benchmark Status${RESET}"
echo -e "File: ${CYAN}${LOG_FILE}${RESET}"
echo -e "---"

# Read last 100 lines to get recent status
TAIL_OUTPUT=$(tail -n 100 "$LOG_FILE")

# Extract current benchmark name
BENCHMARK=$(echo "$TAIL_OUTPUT" | grep -F "# Benchmark:" | tail -1 | sed 's/.*# Benchmark: //')
if [ -n "$BENCHMARK" ]; then
    SHORT_NAME="${BENCHMARK##*.}"
    echo -e "${BOLD}Benchmark:${RESET} ${SHORT_NAME}"
else
    echo -e "${YELLOW}Benchmark: Starting...${RESET}"
fi

# Extract progress and ETA
PROGRESS_LINE=$(echo "$TAIL_OUTPUT" | grep -F "# Run progress:" | tail -1)
if [ -n "$PROGRESS_LINE" ]; then
    if [[ "$PROGRESS_LINE" =~ ([0-9.]+)%\ complete,\ ETA\ ([0-9:]+) ]]; then
        PROGRESS="${BASH_REMATCH[1]}"
        ETA="${BASH_REMATCH[2]}"
        echo -e "${BOLD}Progress:${RESET}  ${GREEN}${PROGRESS}%${RESET}"
        echo -e "${BOLD}ETA:${RESET}       ${GREEN}${ETA}${RESET}"
    fi
fi

# Extract current fork
FORK_LINE=$(echo "$TAIL_OUTPUT" | grep -F "# Fork:" | tail -1)
if [ -n "$FORK_LINE" ]; then
    if [[ "$FORK_LINE" =~ Fork:\ ([0-9]+)\ of\ ([0-9]+) ]]; then
        FORK_NUM="${BASH_REMATCH[1]}"
        FORK_TOTAL="${BASH_REMATCH[2]}"
        echo -e "${BOLD}Fork:${RESET}      ${FORK_NUM} of ${FORK_TOTAL}"
    fi
fi

# Extract latest iteration (warmup or measurement)
LATEST_WARMUP=$(echo "$TAIL_OUTPUT" | grep -F "# Warmup Iteration" | tail -1)
LATEST_MEASURE=$(echo "$TAIL_OUTPUT" | grep "Iteration" | grep -v "Warmup" | grep -v "^#" | tail -1)

if [ -n "$LATEST_MEASURE" ]; then
    if [[ "$LATEST_MEASURE" =~ Iteration\ +([0-9]+):\ ([0-9.]+)\ ns/op ]]; then
        ITER="${BASH_REMATCH[1]}"
        VALUE="${BASH_REMATCH[2]}"
        echo -e "${BOLD}Phase:${RESET}     ${CYAN}Measurement (iter ${ITER}: ${VALUE} ns/op)${RESET}"
    fi
elif [ -n "$LATEST_WARMUP" ]; then
    if [[ "$LATEST_WARMUP" =~ Warmup\ Iteration\ +([0-9]+):\ ([0-9.]+)\ ns/op ]]; then
        ITER="${BASH_REMATCH[1]}"
        VALUE="${BASH_REMATCH[2]}"
        echo -e "${BOLD}Phase:${RESET}     Warmup (iter ${ITER}: ${VALUE} ns/op)"
    fi
fi

# Check if completed
if echo "$TAIL_OUTPUT" | grep -qF "# Run complete"; then
    echo -e "\n${BOLD}${GREEN}✓ Benchmark completed!${RESET}"
    echo -e "Use log-reader agent to extract results from: ${CYAN}${LOG_FILE}${RESET}"
else
    echo -e "\n${YELLOW}⏳ Benchmark still running...${RESET}"
fi

echo ""
