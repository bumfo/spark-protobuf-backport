#!/bin/bash
# Real-time JMH benchmark progress monitor
# Usage: ./monitor-jmh.sh [-t timeout_seconds] <jmh-log-file>

set -euo pipefail

# Default values
TIMEOUT=60  # Default 60s timeout, use 0 for no timeout
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
    echo "Example: $0 /tmp/jmh_scalar.log  # Default 60s timeout"
    echo "Example: $0 -t 0 /tmp/jmh_scalar.log  # No timeout"
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
IN_RESULT_TABLE=false
RUN_COMPLETE=false
TOTAL_TIME=""
COMPILE_SHOWN=false
BUILD_TIME=""
TIMER_PID=0
TAIL_PID=0

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
        # Don't print iteration details - too verbose for logs
        :
    fi
}

# Initialize status display (3 blank lines for interactive mode only)
if [ "$INTERACTIVE" = true ]; then
    printf "\n\n\n"
fi

# Function to process log lines
process_log_lines() {
    while IFS= read -r line; do
        # Detect compilation start (show only once)
        if [[ "$line" =~ ^\[info\]\ compiling\ [0-9]+\ (Scala|Java)\ sources? ]] && [ "$COMPILE_SHOWN" = false ]; then
            COMPILE_SHOWN=true
            echo "Compiling..."
        fi

        # Capture build time (before benchmark run)
        if [[ "$line" =~ ^\[success\]\ Total\ time:\ ([^,]+), ]] && [ "$RUN_COMPLETE" = false ]; then
            BUILD_TIME="${BASH_REMATCH[1]}"
        fi

        # Detect JMH run start - show build complete and clear screen for interactive mode
        if [[ "$line" =~ ^\[info\]\ running\ \(fork\)\ org\.openjdk\.jmh\.Main ]]; then
            if [ -n "$BUILD_TIME" ]; then
                echo "Build successful (${BUILD_TIME})"
            fi

            # In interactive mode, clear and reprint header
            if [ "$INTERACTIVE" = true ]; then
                clear
                echo -e "${BOLD}JMH Benchmark Progress Monitor${RESET}"
                echo -e "Monitoring: ${CYAN}${LOG_FILE}${RESET}"
                if [ "$TIMEOUT" -gt 0 ]; then
                    echo -e "Timeout: ${YELLOW}${TIMEOUT}s${RESET}"
                fi
                echo -e "---"
                printf "\n\n\n"
            fi
        fi

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
                    echo "Partial result: ${SHORT_RESULT}: ${SCORE} ± ${ERROR} ns/op"
                fi
                RESULT_BENCHMARK=""
            fi
        fi

        # Detect result table header
        if [[ "$line" =~ ^[[:space:]]*\[info\][[:space:]]+Benchmark.*Mode.*Cnt.*Score.*Error.*Units ]]; then
            IN_RESULT_TABLE=true
            # Strip [info] prefix and output
            local stripped_line=$(echo "$line" | sed 's/^[[:space:]]*\[info\][[:space:]]*//')
            echo ""
            echo "$stripped_line"
        # Process result table rows
        elif [ "$IN_RESULT_TABLE" = true ]; then
            # Check if this is a table row (starts with [info] followed by non-space)
            if [[ "$line" =~ ^[[:space:]]*\[info\][[:space:]]+[^[:space:]] ]]; then
                # Strip [info] prefix and output
                local stripped_line=$(echo "$line" | sed 's/^[[:space:]]*\[info\][[:space:]]*//')
                echo "$stripped_line"
            # Empty line or non-matching line - end of table
            elif [[ "$line" =~ ^[[:space:]]*$ ]] || [[ ! "$line" =~ ^\[info\] ]]; then
                IN_RESULT_TABLE=false
                echo ""
            fi
        fi

        # Detect completion
        if [[ "$line" =~ Run\ complete ]]; then
            RUN_COMPLETE=true
            # Kill tail process to stop reading new lines
            # But don't break - let the loop finish processing buffered lines
            if [ "$TAIL_PID" -gt 0 ]; then
                kill $TAIL_PID || true
                TAIL_PID=0
            fi
            # Kill timer process to prevent timeout
            if [ "$TIMER_PID" -gt 0 ]; then
                kill $TIMER_PID || true
                TIMER_PID=0
            fi
        fi

        # Extract total time if run completed
        if [ "$RUN_COMPLETE" = true ] && [[ "$line" =~ ^\[success\]\ Total\ time:\ (.+)$ ]]; then
            TOTAL_TIME="${BASH_REMATCH[1]}"
        fi

        # Skip compilation warnings - they're not benchmark errors
        # Benchmark errors would appear in different format
    done
}

# Cleanup function to kill processes on exit
cleanup() {
    # Kill timer if it exists
    if [ "$TIMER_PID" -gt 0 ]; then
        kill $TIMER_PID || true
        TIMER_PID=0
    fi
    # Kill tail process if it exists
    if [ "$TAIL_PID" -gt 0 ]; then
        kill $TAIL_PID || true
        TAIL_PID=0
    fi
    exit 0
}
trap cleanup EXIT

# Run with or without timeout
if [ "$TIMEOUT" -gt 0 ]; then
    # Get the directory where this script is located
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    # Start timeout killer in background (separate process is easier to kill)
    "$SCRIPT_DIR/timeout-killer.sh" "$TIMEOUT" $$ &
    TIMER_PID=$!
fi

# Start tail in background and use named pipe for clean PID tracking
FIFO=$(mktemp -u)
mkfifo "$FIFO"
trap "rm -f $FIFO; cleanup" EXIT TERM INT

# Start tail in background
tail -n 100 -f "$LOG_FILE" > "$FIFO" &
TAIL_PID=$!

# Process lines from tail via FIFO
process_log_lines < "$FIFO"

# Clean up FIFO
rm -f "$FIFO"

# Check if benchmark completed successfully
if [ "$RUN_COMPLETE" = true ]; then
    if [ "$INTERACTIVE" = true ]; then
        echo -ne "\n${BOLD}${GREEN}✓ Benchmark suite completed!${RESET}"
        if [ -n "$TOTAL_TIME" ]; then
            echo -ne " ${CYAN}(${TOTAL_TIME})${RESET}"
        fi
        echo -ne "\n\n"
    else
        echo ""
        if [ -n "$TOTAL_TIME" ]; then
            echo "✓ Benchmark suite completed! (${TOTAL_TIME})"
        else
            echo "✓ Benchmark suite completed!"
        fi
    fi
else
    echo "Monitor stopped."
fi

exit 0
