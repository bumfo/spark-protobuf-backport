# Core - Scala Implementation

This file provides guidance to Claude Code (claude.ai/code) when working with the core Scala implementation of the Spark protobuf backport.

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
5. **Enhanced code generation**: `doGenCode` method generates optimized code when `rowConverterOpt` is available

## Development Notes

- **Scala version**: 2.12.15 (matches Spark 3.2.1)
- **Spark version**: 3.2.1 (provided dependency)
- **Protobuf version**: 3.11.4 (shaded to avoid conflicts)
- **Test approach**: Comprehensive ScalaTest suite in `ProtobufBackportSpec.scala` using `google.protobuf.Type`
- **Performance benchmarks**: Performance comparison tests in `ProtobufConversionBenchmark.scala` comparing codegen vs DynamicMessage paths
- **Shading**: All protobuf classes shaded under `org.sparkproject.spark.protobuf311.*` in uber JAR

The tests verify three usage patterns: compiled class, descriptor file, and binary descriptor set approaches. 

## Performance Benchmarking

The benchmark in `ProtobufConversionBenchmark.scala` compares performance between the three conversion approaches:

- **Compiled class path**: Uses generated `RowConverter` via Janino for direct `UnsafeRow` conversion
- **Descriptor file path**: Uses `DynamicMessage` parsing with descriptor file lookup
- **Binary descriptor set path**: Uses `DynamicMessage` parsing with embedded descriptor bytes

**Current findings**: Benchmark results show that DynamicMessage paths are currently 2-3x faster than the codegen path in DataFrame operations. This reveals optimization opportunities for:
- Janino compilation caching to avoid runtime overhead
- Generated code efficiency improvements  
- Better utilization of JVM optimizations
- Performance gains may be more apparent with larger batch sizes

### Running Benchmarks

```bash
# Run performance benchmarks
sbt "testOnly *ProtobufConversionBenchmark*"
```

The benchmark provides a foundation for measuring and tracking performance improvements as the codebase evolves.

## Code Generation Optimization

The `ProtobufDataToCatalyst.doGenCode` method has been enhanced to generate two distinct code paths:

1. **When `rowConverterOpt` is available**: Generates optimized code that directly calls:
   - `parseCompiled(binary)` to get compiled protobuf message
   - `rowConverter.convert(message)` to convert to InternalRow

2. **When `rowConverterOpt` is `None`**: Falls back to calling `nullSafeEval()`

### Class Name Handling

The generated code uses **runtime-determined class names** instead of hardcoded ones:

- **Primary path**: Uses `messageClassOpt.get.getName` to get the actual protobuf class name
- **Fallback path**: Uses `classOf[PbMessage].getName` which automatically resolves based on runtime classpath:
  - **Non-shaded**: `com.google.protobuf.Message`  
  - **Shaded**: `org.sparkproject.spark_protobuf.protobuf.Message`
- **Generated code**: Casts to specific message type for better JIT optimization potential

**Example generated code**:
```java
// Before: com.google.protobuf.Message parsedMsg = (com.google.protobuf.Message) msg.get();
// After:  com.example.MyMessage parsedMsg = (com.example.MyMessage) msg.get();
```

This approach eliminates hardcoded class assumptions and **automatically supports both shaded and non-shaded protobuf usage** based on the runtime classpath.

**Note**: The fallback case (`None`) should theoretically never occur when `rowConverterOpt` is `Some`, but is included for defensive programming.

This provides better performance potential when Spark actually executes generated code (vs pre-computed paths).