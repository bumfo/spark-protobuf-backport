#!/bin/bash
#
# PySpark Protobuf Backport Test Runner
# 
# Automatically finds Python virtual environment and runs the functional test.
# If no venv is found, provides instructions to create one.
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 PySpark Protobuf Backport Test Runner${NC}"
echo "============================================================"

# Function to check if a virtual environment exists and is valid
check_venv() {
    local venv_path="$1"
    if [[ -d "$venv_path" && -f "$venv_path/bin/activate" && -f "$venv_path/bin/python" ]]; then
        return 0
    else
        return 1
    fi
}

# Function to check if PySpark is installed in the venv
check_pyspark() {
    local python_path="$1"
    if "$python_path" -c "import pyspark; print(f'PySpark {pyspark.__version__} found')" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Look for virtual environments in common locations
VENV_PATHS=(
    ".venv"
    "venv" 
    ".env"
    "env"
    "../.venv"
    "../venv"
)

FOUND_VENV=""
PYTHON_PATH=""

echo "🔍 Looking for Python virtual environment..."

for venv_path in "${VENV_PATHS[@]}"; do
    if check_venv "$venv_path"; then
        echo -e "   ${GREEN}✓${NC} Found virtual environment: $venv_path"
        FOUND_VENV="$venv_path"
        PYTHON_PATH="$venv_path/bin/python"
        break
    else
        echo -e "   ${YELLOW}✗${NC} Not found: $venv_path"
    fi
done

if [[ -z "$FOUND_VENV" ]]; then
    echo -e "\n${RED}❌ No virtual environment found!${NC}"
    echo
    echo "Please create a virtual environment with PySpark installed:"
    echo
    echo -e "${YELLOW}# Create virtual environment:${NC}"
    echo "python3 -m venv .venv"
    echo
    echo -e "${YELLOW}# Activate it:${NC}"
    echo "source .venv/bin/activate"
    echo
    echo -e "${YELLOW}# Install PySpark:${NC}"
    echo "pip install 'pyspark>=3.2.0,<3.4.0'"
    echo
    echo -e "${YELLOW}# Then run this script again:${NC}"
    echo "./run_test.sh"
    echo
    exit 1
fi

# Check if PySpark is installed
echo
echo "🔍 Checking PySpark installation..."
if check_pyspark "$PYTHON_PATH"; then
    echo -e "   ${GREEN}✓${NC} PySpark is installed and accessible"
else
    echo -e "   ${RED}❌${NC} PySpark not found in virtual environment"
    echo
    echo "Please install PySpark in your virtual environment:"
    echo
    echo -e "${YELLOW}# Activate the virtual environment:${NC}"
    echo "source $FOUND_VENV/bin/activate"
    echo
    echo -e "${YELLOW}# Install PySpark:${NC}"
    echo "pip install 'pyspark>=3.2.0,<3.4.0'"
    echo
    exit 1
fi

# Check if the JAR exists
JAR_PATH="../uber/target/scala-2.12/spark-protobuf-backport-shaded-0.1.0-SNAPSHOT.jar"
echo
echo "🔍 Checking for protobuf backport JAR..."
if [[ -f "$JAR_PATH" ]]; then
    echo -e "   ${GREEN}✓${NC} JAR found: $JAR_PATH"
else
    echo -e "   ${RED}❌${NC} JAR not found: $JAR_PATH"
    echo
    echo "Please build the JAR first:"
    echo
    echo -e "${YELLOW}# From the project root:${NC}"
    echo "cd .."
    echo "sbt uberJar/assembly"
    echo "cd python"
    echo
    exit 1
fi

# All checks passed, run the test
echo
echo -e "${GREEN}✅ All prerequisites met!${NC}"
echo
echo "🧪 Running functional test..."
echo "============================================================"
echo

# Activate the virtual environment and run the test
source "$FOUND_VENV/bin/activate"
python test_pyspark_protobuf.py

# Check the exit code
TEST_EXIT_CODE=$?

echo
echo "============================================================"
if [[ $TEST_EXIT_CODE -eq 0 ]]; then
    echo -e "${GREEN}✅ Test completed successfully!${NC}"
    echo
    echo "💡 Your PySpark protobuf backport is working correctly."
    echo "   You can now use it in your projects:"
    echo
    echo -e "${BLUE}from spark_protobuf.functions import from_protobuf, to_protobuf${NC}"
    echo
else
    echo -e "${RED}❌ Test failed with exit code: $TEST_EXIT_CODE${NC}"
    echo
    echo "Please check the error messages above and:"
    echo "1. Ensure the JAR is built correctly: sbt uberJar/assembly"
    echo "2. Verify your PySpark installation is compatible (3.2.x or 3.3.x)"
    echo "3. Check that the virtual environment has all required dependencies"
fi

exit $TEST_EXIT_CODE