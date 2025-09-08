# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Scala/Spark project that backports Spark 3.4's protobuf connector to Spark 3.2.1. It provides `from_protobuf` and `to_protobuf` functions to convert between binary Protobuf data and Spark SQL DataFrames without requiring users to upgrade Spark or patch the runtime.

The project includes two key optimizations:
1. **Compiled message support**: When `messageName` refers to a compiled Java class, uses generated `RowConverter` via Janino for direct conversion to `UnsafeRow`, avoiding `DynamicMessage` overhead
2. **Binary descriptor sets**: Allows passing descriptor bytes directly to avoid file distribution issues on executors

## Build Commands

```bash
# Compile the project
sbt compile

# Run tests
sbt --error test

# Build shaded JAR with all dependencies
sbt assembly

# Clean build artifacts
sbt clean
```

The assembled JAR includes shaded protobuf dependencies to avoid conflicts with Spark's own protobuf runtime.

## Architecture

### Core Components

**Expression Layer** (`src/main/scala/org/apache/spark/sql/protobuf/backport/`):
- `ProtobufDataToCatalyst` - Deserializes protobuf binary → Catalyst rows (`from_protobuf`)
- `CatalystDataToProtobuf` - Serializes Catalyst rows → protobuf binary (`to_protobuf`) 
- `functions` - DataFrame API entry points for both functions
- `ProtobufExtensions` - SQL function registration via SparkSessionExtensions

**Schema & Utilities** (`utils/` subdirectory):
- `SchemaConverters` - Converts protobuf descriptors → Spark SQL schemas
- `ProtobufUtils` - Message descriptor loading (from files, classes, or binary descriptor sets)
- `ProtobufOptions` - Parse mode and recursion depth configuration

**Serialization Logic**:
- `ProtobufSerializer` - Converts Catalyst values to protobuf messages using `DynamicMessage`
- `ProtobufDeserializer` - Converts `DynamicMessage` to Catalyst rows

**Fast Proto Integration** (`src/main/scala/fastproto/`):
- `ProtoToRowGenerator` - Generates optimized `RowConverter` implementations using Janino
- `RowConverter` - Interface for direct protobuf message → `UnsafeRow` conversion

**Spark Compatibility Shims** (`shims/` subdirectory):
- Error handling and query compilation compatibility layer for Spark 3.2.1

### Key Design Patterns

1. **Dual execution paths**: Compiled classes use generated converters; descriptor-only uses `DynamicMessage`
2. **Binary descriptor set support**: Eliminates executor file access requirements
3. **Schema inference caching**: Lazy computation of Spark schemas from protobuf descriptors
4. **Parse mode handling**: Supports both permissive (null on error) and fail-fast modes

## Development Notes

- **Scala version**: 2.12.15 (matches Spark 3.2.1)
- **Spark version**: 3.2.1 (provided dependency)
- **Protobuf version**: 3.11.4 (shaded to avoid conflicts)
- **Test approach**: Single comprehensive test in `TestProtobufBackport.scala` using `google.protobuf.Type`
- **Shading**: All protobuf classes shaded under `org.sparkproject.spark.protobuf311.*`

The test verifies three usage patterns: compiled class, descriptor file, and binary descriptor set approaches.