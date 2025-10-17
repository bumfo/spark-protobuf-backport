#!/bin/bash
# Real-time JMH benchmark progress monitor
# Usage: ./monitor-jmh.sh [-t timeout_seconds] <jmh-log-file>

set -euo pipefail

# Default values
TIMEOUT=0  # 0 means no timeout
LOG_FILE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--timeout)
            if [ $# -lt 2 ]; then
                echo "Error: -t requires a timeout value in seconds"
                exit 1
            fi
            TIMEOUT="$2"
            shift 2
            ;;
        *)
            LOG_FILE="$1"
            shift
            ;;
    esac
done

# Check if log file is provided
if [ -z "$LOG_FILE" ]; then
    echo "Usage: $0 [-t timeout_seconds] <jmh-log-file>"
    echo "Example: $0 /tmp/jmh_scalar.log"
    echo "Example: $0 -t 600 /tmp/jmh_scalar.log  # 10 minute timeout"
    exit 1
fi

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
if [ "$TIMEOUT" -gt 0 ]; then
    echo -e "Timeout: ${YELLOW}${TIMEOUT}s${RESET}"
fi
echo -e "---"

# Function to process log lines
process_log_lines() {
    while IFS= read -r line; do
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
        if [[ "$line" =~ Run\ complete ]]; then
            echo -e "\n${BOLD}${GREEN}✓ Benchmark suite completed!${RESET}\n"
            # Kill tail process to allow clean exit
            if [ -n "${TAIL_PID:-}" ]; then
                kill $TAIL_PID 2>/dev/null || true
            fi
            break
        fi

        # Show errors/warnings
        if [[ "$line" =~ \[warn\] ]] || [[ "$line" =~ ERROR ]]; then
            echo -e "  ${YELLOW}⚠ ${line}${RESET}"
        fi
    done
}

# Cleanup function to kill processes on exit
cleanup() {
    # Kill timer if it exists
    if [ -n "${TIMER_PID:-}" ]; then
        kill $TIMER_PID 2>/dev/null || true
    fi
    # Kill tail process if it exists
    if [ -n "${TAIL_PID:-}" ]; then
        kill $TAIL_PID 2>/dev/null || true
    fi
}
trap cleanup EXIT TERM INT

# Run with or without timeout
if [ "$TIMEOUT" -gt 0 ]; then
    # Portable timeout implementation using background timer
    (
        sleep "$TIMEOUT"
        echo -e "\n${YELLOW}⏱ Timeout reached after ${TIMEOUT} seconds${RESET}" >&2
        kill -TERM $$ 2>/dev/null
    ) &
    TIMER_PID=$!
fi

# Start tail in background and use named pipe for clean PID tracking
FIFO=$(mktemp -u)
mkfifo "$FIFO"
trap "rm -f $FIFO; cleanup" EXIT TERM INT

# Start tail in background
tail -n 20 -f "$LOG_FILE" > "$FIFO" &
TAIL_PID=$!

# Process lines from tail via FIFO
process_log_lines < "$FIFO"

# Clean up FIFO
rm -f "$FIFO"

# If we get here, benchmark completed (or timeout triggered)
echo "Monitor stopped."
