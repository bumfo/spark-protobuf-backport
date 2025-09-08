# PySpark Protobuf Backport - Python Directory

This directory contains the PySpark wrapper for the Spark protobuf backport, providing clean Python imports and functions for protobuf data conversion.

## Key Components

### Test Runner
- **`run_test.sh`** - Automated test script that finds virtual environments, checks prerequisites, and runs functional tests
- Handles venv discovery, PySpark validation, and JAR verification automatically

### Python Package  
- **`spark_protobuf/functions.py`** - Main wrapper functions (`from_protobuf`, `to_protobuf`)
- Wraps Scala implementation via JVM bridge, avoids namespace conflicts with PySpark
- Supports all three usage patterns: compiled class, descriptor file, binary descriptor set

### Functional Testing
- **`test_pyspark_protobuf.py`** - Comprehensive test suite using real protobuf data
- Creates `google.protobuf.Type` messages via shaded protobuf classes through JVM
- Tests all API access patterns: Python functions, SQL registration, DataFrame expr()

## Usage Patterns

### Import Pattern
```python
# Clean import (no namespace conflicts)
from spark_protobuf.functions import from_protobuf, to_protobuf
```

### Function Calls
```python
# Direct function usage
df.select(from_protobuf(col("data"), "MessageType"))

# SQL registration  
spark.sql("SELECT from_protobuf(data, 'MessageType') FROM table")

# DataFrame expr()
df.select(expr("from_protobuf(data, 'MessageType')"))
```

## Testing
```bash
# Automated (recommended)
./run_test.sh

# Manual
source .venv/bin/activate
python test_pyspark_protobuf.py
```

The test suite validates actual protobuf conversion functionality using the shaded protobuf classes (`org.sparkproject.spark_protobuf.protobuf.*`) to ensure compatibility with the backport JAR.