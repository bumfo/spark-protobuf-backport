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
- `RowConverter` - Base interface for simple protobuf binary → `InternalRow` conversion
- `BufferSharingRowConverter` - Base implementation with buffer sharing for nested conversions
- `MessageBasedConverter` - Interface for compiled protobuf message → `InternalRow` conversion
- `WireFormatConverter` - Direct wire format parsing to UnsafeRow without intermediate message objects
- `DynamicMessageConverter` - Fallback converter using traditional DynamicMessage approach

**Spark Compatibility Shims** (`shims/` subdirectory):
- Error handling and query compilation compatibility layer for Spark 3.2.1

### Key Design Patterns

1. **Dual execution paths**: Compiled classes use generated converters; descriptor-only uses `DynamicMessage`
2. **Binary descriptor set support**: Eliminates executor file access requirements
3. **Schema inference caching**: Lazy computation of Spark schemas from protobuf descriptors
4. **Parse mode handling**: Supports both permissive (null on error) and fail-fast modes
5. **Enhanced code generation**: `doGenCode` method generates optimized code when `rowConverterOpt` is available
6. **Wire format optimization**: Direct parsing for binary descriptor sets when possible

## Converter Architecture

The project uses a three-tier converter interface hierarchy for optimal separation of concerns:

### 1. Base Interface: `RowConverter`
- **Purpose**: Simple trait for basic protobuf-to-row conversion
- **Method**: `convert(binary: Array[Byte]): InternalRow`
- **Usage**: Minimal interface implemented by all converters
- **Example**: `DynamicMessageConverter` which doesn't support buffer sharing

### 2. Buffer Sharing Implementation: `BufferSharingRowConverter`
- **Purpose**: Base implementation class with buffer sharing capabilities for nested conversions
- **Key Methods**:
  - `parseAndWriteFields(binary, writer)` - Core parsing logic (abstract)
  - `convertWithSharedBuffer(binary, parentWriter)` - Enables nested buffer sharing
  - `acquireWriter(parentWriter)` - Manages writer acquisition for child structures
- **Usage**: Extended by wire format converters and generated code
- **Examples**: `WireFormatConverter`, generated wire format converters

### 3. Message-Based Interface: `MessageBasedConverter[T]`
- **Purpose**: Interface for compiled protobuf message objects (not binary data)
- **Key Methods**:
  - `convert(message: T): InternalRow` - Basic message conversion
  - `convertWithSharedBuffer(message: T, parentWriter)` - Message conversion with buffer sharing
- **Usage**: Implemented by generated converters for compiled protobuf classes
- **Examples**: Generated converters from `ProtoToRowGenerator`

### Performance Characteristics

- **Generated converters (compiled class)**: ~1,844 ns/op (fastest)
- **Wire format converters**: ~4,399 ns/op (2.4x slower than generated)
- **DynamicMessage converters**: ~24,992 ns/op (13.6x slower than generated)

### Usage Examples

```scala
// Simple conversion (DynamicMessageConverter)
val converter = new DynamicMessageConverter(descriptor, schema)
val row = converter.convert(binaryData)  // No buffer sharing support

// Wire format conversion (BufferSharingRowConverter)
val converter = new WireFormatConverter(descriptor, schema)
val row = converter.convert(binaryData)  // Standalone conversion
converter.convertWithSharedBuffer(binaryData, parentWriter)  // Nested with buffer sharing

// Message-based conversion (Generated code)
val converter = ProtoToRowGenerator.generateConverter(descriptor, messageClass)
val row = converter.convert(message)  // Convert compiled message
converter.convertWithSharedBuffer(message, parentWriter)  // Nested message conversion
```

## Development Notes

- **Scala version**: 2.12.15 (matches Spark 3.2.1)
- **Spark version**: 3.2.1 (provided dependency)
- **Protobuf version**: 3.11.4 (shaded to avoid conflicts)
- **Test approach**: Comprehensive ScalaTest suite in `ProtobufBackportSpec.scala` using `google.protobuf.Type`
- **Performance benchmarks**: Performance comparison tests in `ProtobufConversionBenchmark.scala` comparing codegen vs DynamicMessage paths
- **Shading**: All protobuf classes shaded under `org.sparkproject.spark.protobuf311.*` in uber JAR

The tests verify three usage patterns: compiled class, descriptor file, and binary descriptor set approaches. 

## Performance Benchmarking

The project includes comprehensive benchmarking infrastructure to validate optimization impact:

- **Compiled class path**: Uses generated `RowConverter` via Janino for direct `UnsafeRow` conversion
- **Wire format path**: Uses `WireFormatConverter` for direct binary parsing with binary descriptor sets  
- **Dynamic message path**: Uses `DynamicMessage` parsing for maximum compatibility

### Benchmarking Infrastructure

The project integrates JMH (Java Microbenchmark Harness) for professional performance validation:
- Fork isolation prevents JVM warmup contamination between benchmarks
- Statistical analysis provides confidence intervals
- Enables tracking optimization impact over time

### Running Benchmarks

```bash
# Run traditional ScalaTest benchmarks
sbt "testOnly *ProtobufConversionBenchmark*"

# Run JMH benchmarks (all)
sbt "core/Jmh/run"

# Run specific JMH benchmarks
sbt "core/Jmh/run .*WireFormatConverter.*"

# Quick JMH test with minimal iterations
sbt "core/Jmh/run -wi 2 -i 3 -f 1"
```

The benchmarking infrastructure provides a foundation for measuring and tracking performance improvements as the codebase evolves.

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

## BufferSharingRowConverter Implementation Details

The `BufferSharingRowConverter` provides the foundation for converters that support nested buffer sharing for memory-efficient nested message handling.

### Method Contract

- **`parseAndWriteFields(binary, writer)`**: Abstract method that parses protobuf and writes to absolute ordinals (0, 1, 2)
- **`convertWithSharedBuffer(binary, parentWriter)`**: Handles writer preparation and buffer sharing
- **`acquireWriter(parentWriter)`**: Manages writer acquisition and reuse for child structures

### Usage Pattern

For nested message arrays, always use `convertWithSharedBuffer()` which manages buffer sharing properly. Direct `parseAndWriteFields()` usage in nested contexts can overwrite parent row data.

### Interface Benefits

This design separates parsing logic (`parseAndWriteFields`) from writer management (`convertWithSharedBuffer`), enabling both standalone conversion and efficient nested message handling with shared buffers. The `acquireWriter` method provides a clean abstraction for writer lifecycle management.