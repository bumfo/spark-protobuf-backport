#!/bin/bash
# Real-time JMH benchmark progress monitor
# Usage: ./monitor-jmh.sh /tmp/jmh_benchmark.log

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
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RESET='\033[0m'
BOLD='\033[1m'

# Track current benchmark name
CURRENT_BENCHMARK=""

echo -e "${BOLD}JMH Benchmark Progress Monitor${RESET}"
echo -e "Monitoring: ${CYAN}${LOG_FILE}${RESET}"
echo -e "---"

# Follow log file and filter for important lines
tail -f "$LOG_FILE" | while IFS= read -r line; do
    # Extract benchmark name
    if [[ "$line" =~ \#\ Benchmark:\ (.+) ]]; then
        CURRENT_BENCHMARK="${BASH_REMATCH[1]}"
        # Shorten benchmark name (just the last part after last dot)
        SHORT_NAME="${CURRENT_BENCHMARK##*.}"
        echo -e "\n${BOLD}${BLUE}▶ Benchmark: ${SHORT_NAME}${RESET}"
    fi

    # Extract progress and ETA
    if [[ "$line" =~ \#\ Run\ progress:\ ([0-9.]+)%\ complete,\ ETA\ ([0-9:]+) ]]; then
        PROGRESS="${BASH_REMATCH[1]}"
        ETA="${BASH_REMATCH[2]}"
        echo -e "  ${GREEN}Progress: ${PROGRESS}% | ETA: ${ETA}${RESET}"
    fi

    # Extract fork info
    if [[ "$line" =~ \#\ Fork:\ ([0-9]+)\ of\ ([0-9]+) ]]; then
        FORK_NUM="${BASH_REMATCH[1]}"
        FORK_TOTAL="${BASH_REMATCH[2]}"
        echo -e "  ${YELLOW}Fork ${FORK_NUM}/${FORK_TOTAL}${RESET}"
    fi

    # Extract warmup iterations with values
    if [[ "$line" =~ \#\ Warmup\ Iteration\ +([0-9]+):\ ([0-9.]+)\ ns/op ]]; then
        ITER="${BASH_REMATCH[1]}"
        VALUE="${BASH_REMATCH[2]}"
        echo -e "    Warmup ${ITER}: ${VALUE} ns/op"
    fi

    # Extract measurement iterations with values
    if [[ "$line" =~ ^[[:space:]]*\[info\][[:space:]]+Iteration\ +([0-9]+):\ ([0-9.]+)\ ns/op ]]; then
        ITER="${BASH_REMATCH[1]}"
        VALUE="${BASH_REMATCH[2]}"
        echo -e "    ${CYAN}Measure ${ITER}: ${VALUE} ns/op${RESET}"
    fi

    # Detect completion
    if [[ "$line" =~ \#\ Run\ complete ]]; then
        echo -e "\n${BOLD}${GREEN}✓ Benchmark suite completed!${RESET}\n"
        break
    fi

    # Show errors/warnings
    if [[ "$line" =~ \[warn\] ]] || [[ "$line" =~ ERROR ]]; then
        echo -e "  ${YELLOW}⚠ ${line}${RESET}"
    fi
done

echo "Monitor stopped."
