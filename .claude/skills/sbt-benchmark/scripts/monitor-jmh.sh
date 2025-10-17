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

# Detect interactive mode (stdout is a terminal)
if [ -t 1 ] && [ -z "${MONITOR_NONINTERACTIVE:-}" ]; then
    INTERACTIVE=true
else
    INTERACTIVE=false
fi

# ANSI color codes (only use in interactive mode)
if [ "$INTERACTIVE" = true ]; then
    GREEN='\033[0;32m'
    BLUE='\033[0;34m'
    YELLOW='\033[1;33m'
    CYAN='\033[0;36m'
    GREY='\033[0;90m'
    RESET='\033[0m'
    BOLD='\033[1m'
else
    GREEN=''
    BLUE=''
    YELLOW=''
    CYAN=''
    GREY=''
    RESET=''
    BOLD=''
fi

# Track state
CURRENT_BENCHMARK=""
LAST_RESULT=""
CURRENT_PROGRESS=""
CURRENT_FORK=""
LAST_ITER_INFO=""
RESULT_BENCHMARK=""

echo -e "${BOLD}JMH Benchmark Progress Monitor${RESET}"
echo -e "Monitoring: ${CYAN}${LOG_FILE}${RESET}"
if [ "$TIMEOUT" -gt 0 ]; then
    echo -e "Timeout: ${YELLOW}${TIMEOUT}s${RESET}"
fi
echo -e "---"

# Function to clear current line and move up
clear_line() {
    echo -ne "\033[2K\r"
}

# Function to display current status
show_status() {
    if [ "$INTERACTIVE" = true ]; then
        # Interactive mode: Update in place
        # Move cursor up 3 lines and clear/rewrite each line
        echo -ne "\033[3A"

        # Line 1: Last result
        clear_line
        if [ -n "$LAST_RESULT" ]; then
            echo -ne "${GREEN}Last: ${LAST_RESULT}${RESET}\n"
        else
            echo -ne "\n"
        fi

        # Line 2: Current benchmark
        clear_line
        if [ -n "$CURRENT_BENCHMARK" ]; then
            local SHORT_NAME="${CURRENT_BENCHMARK##*.}"
            echo -ne "${BOLD}${BLUE}Current: ${SHORT_NAME}${RESET}\n"
        else
            echo -ne "\n"
        fi

        # Line 3: Progress info
        clear_line
        if [ -n "$CURRENT_BENCHMARK" ]; then
            local status_line=""
            if [ -n "$CURRENT_PROGRESS" ]; then
                status_line="${CURRENT_PROGRESS}"
            fi
            if [ -n "$CURRENT_FORK" ]; then
                if [ -n "$status_line" ]; then
                    status_line="${status_line} | "
                fi
                status_line="${status_line}${CURRENT_FORK}"
            fi
            if [ -n "$LAST_ITER_INFO" ]; then
                if [ -n "$status_line" ]; then
                    status_line="${status_line} | "
                fi
                status_line="${status_line}${LAST_ITER_INFO}"
            fi
            if [ -n "$status_line" ]; then
                echo -ne "  ${status_line}\n"
            else
                echo -ne "\n"
            fi
        else
            echo -ne "\n"
        fi
    else
        # Non-interactive mode: Append-only output
        # Only print when there's a significant update
        if [ -n "$LAST_ITER_INFO" ]; then
            local SHORT_NAME="${CURRENT_BENCHMARK##*.}"
            echo "  [${SHORT_NAME}] ${LAST_ITER_INFO}"
        fi
    fi
}

# Initialize status display (3 blank lines for interactive mode only)
if [ "$INTERACTIVE" = true ]; then
    echo -e "\n\n\n"
fi

# Function to process log lines
process_log_lines() {
    while IFS= read -r line; do
        # Extract benchmark name
        if [[ "$line" =~ \#\ Benchmark:\ (.+) ]]; then
            CURRENT_BENCHMARK="${BASH_REMATCH[1]}"
            CURRENT_PROGRESS=""
            CURRENT_FORK=""
            LAST_ITER_INFO=""
            if [ "$INTERACTIVE" = false ]; then
                local SHORT_NAME="${CURRENT_BENCHMARK##*.}"
                echo "Benchmark: ${SHORT_NAME}"
            fi
            show_status
        fi

        # Extract progress and ETA
        if [[ "$line" =~ \#\ Run\ progress:\ ([0-9.]+)%\ complete,\ ETA\ ([0-9:]+) ]]; then
            PROGRESS="${BASH_REMATCH[1]}"
            ETA="${BASH_REMATCH[2]}"
            CURRENT_PROGRESS="${GREEN}${PROGRESS}% | ETA ${ETA}${RESET}"
            if [ "$INTERACTIVE" = false ]; then
                echo "  Progress: ${PROGRESS}% | ETA ${ETA}"
            fi
            show_status
        fi

        # Extract fork info
        if [[ "$line" =~ \#\ Fork:\ ([0-9]+)\ of\ ([0-9]+) ]]; then
            FORK_NUM="${BASH_REMATCH[1]}"
            FORK_TOTAL="${BASH_REMATCH[2]}"
            CURRENT_FORK="${YELLOW}Fork ${FORK_NUM}/${FORK_TOTAL}${RESET}"
            show_status
        fi

        # Extract warmup iterations with values
        if [[ "$line" =~ \#\ Warmup\ Iteration\ +([0-9]+):\ ([0-9.]+)\ ns/op ]]; then
            ITER="${BASH_REMATCH[1]}"
            VALUE="${BASH_REMATCH[2]}"
            LAST_ITER_INFO="${GREY}Warmup ${ITER}: ${VALUE} ns/op${RESET}"
            show_status
        fi

        # Extract measurement iterations with values
        if [[ "$line" =~ ^[[:space:]]*\[info\][[:space:]]+Iteration\ +([0-9]+):\ ([0-9.]+)\ ns/op ]]; then
            ITER="${BASH_REMATCH[1]}"
            VALUE="${BASH_REMATCH[2]}"
            LAST_ITER_INFO="Measure ${ITER}: ${VALUE} ns/op"
            show_status
        fi

        # Extract result
        if [[ "$line" =~ Result\ \"(.+)\":$ ]]; then
            # Next few lines will contain the score
            RESULT_BENCHMARK="${BASH_REMATCH[1]}"
        fi

        # Extract score from result
        if [[ "$line" =~ ^[[:space:]]*\[info\][[:space:]]+([0-9.]+)\ ±\(99\.9%\)\ ([0-9.]+)\ ns/op ]]; then
            SCORE="${BASH_REMATCH[1]}"
            ERROR="${BASH_REMATCH[2]}"
            if [ -n "$RESULT_BENCHMARK" ]; then
                local SHORT_RESULT="${RESULT_BENCHMARK##*.}"
                LAST_RESULT="${SHORT_RESULT}: ${SCORE} ± ${ERROR} ns/op"
                if [ "$INTERACTIVE" = false ]; then
                    echo "Result: ${SHORT_RESULT}: ${SCORE} ± ${ERROR} ns/op"
                fi
                RESULT_BENCHMARK=""
            fi
        fi

        # Detect completion
        if [[ "$line" =~ Run\ complete ]]; then
            if [ "$INTERACTIVE" = true ]; then
                echo -ne "\n\n${BOLD}${GREEN}✓ Benchmark suite completed!${RESET}\n\n"
            else
                echo "✓ Benchmark suite completed!"
            fi
            # Kill tail process to allow clean exit
            if [ -n "${TAIL_PID:-}" ]; then
                kill $TAIL_PID 2>/dev/null || true
            fi
            break
        fi

        # Skip compilation warnings - they're not benchmark errors
        # Benchmark errors would appear in different format
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
