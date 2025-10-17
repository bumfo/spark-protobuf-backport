#!/bin/bash
# Timeout killer - sends SIGTERM to parent process after timeout
# Usage: timeout-killer.sh <timeout_seconds> <parent_pid>

set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <timeout_seconds> <parent_pid>" >&2
    exit 1
fi

TIMEOUT=$1
PARENT_PID=$2

sleep "$TIMEOUT"
echo -e "\n\033[1;33m⏱ Timeout reached after ${TIMEOUT} seconds\033[0m" >&2
kill -TERM "$PARENT_PID" 2>/dev/null || true
