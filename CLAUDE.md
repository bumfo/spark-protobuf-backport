# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Scala/Spark project that backports Spark 3.4's protobuf connector to Spark 3.2.1. It provides `from_protobuf` and `to_protobuf` functions to convert between binary Protobuf data and Spark SQL DataFrames without requiring users to upgrade Spark or patch the runtime.

The project includes two key optimizations:
1. **Compiled message support**: When `messageName` refers to a compiled Java class, uses generated `RowConverter` via Janino for direct conversion to `UnsafeRow`, avoiding `DynamicMessage` overhead
2. **Binary descriptor sets**: Allows passing descriptor bytes directly to avoid file distribution issues on executors

## Project Structure

- **`core/`** - Scala implementation of the protobuf backport (see `core/CLAUDE.md`)
- **`python/`** - PySpark wrapper with comprehensive testing (see `python/CLAUDE.md`) 
- **`shaded/`** - Shaded protobuf dependencies for conflict avoidance
- **`uber/`** - Assembled JAR with all dependencies

## Build Commands

```bash
# Compile the project
sbt compile

# Run tests
sbt --error test

# Run performance benchmarks (excluded from regular tests)
sbt "core/testOnly benchmark.ProtobufConversionBenchmark -- -n benchmark.Benchmark"

# Build shaded JAR with all dependencies
sbt assembly

# Clean build artifacts
sbt clean
```

The assembled JAR includes shaded protobuf dependencies to avoid conflicts with Spark's own protobuf runtime.

## Module Documentation

For detailed implementation and development information:

- **Core Scala Implementation**: See `core/CLAUDE.md` for architecture, performance benchmarks, and development notes
- **PySpark Support**: See `python/CLAUDE.md` for Python wrapper implementation and testing

## Usage Patterns

The backport supports three protobuf usage patterns:

1. **Compiled Java class**: `from_protobuf(col("data"), "com.example.MyMessage")`
2. **Descriptor file**: `from_protobuf(col("data"), "MyMessage", "/path/to/schema.desc")`  
3. **Binary descriptor set**: `from_protobuf(col("data"), "MyMessage", descriptor_bytes)`

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.