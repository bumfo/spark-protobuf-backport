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
- `ProtoToRowGenerator` - Generates optimized `Parser` implementations using Janino
- `Parser` - Base interface for simple protobuf binary → `InternalRow` conversion
- `BufferSharingParser` - Base implementation with buffer sharing for nested conversions
- `MessageParser` - Interface for compiled protobuf message → `InternalRow` conversion
- `WireFormatParser` - Direct wire format parsing to UnsafeRow without intermediate message objects
- `DynamicMessageParser` - Fallback parser using traditional DynamicMessage approach

**Spark Compatibility Shims** (`shims/` subdirectory):
- Error handling and query compilation compatibility layer for Spark 3.2.1

### Key Design Patterns

1. **Dual execution paths**: Compiled classes use generated parsers; descriptor-only uses `DynamicMessage`
2. **Binary descriptor set support**: Eliminates executor file access requirements
3. **Schema inference caching**: Lazy computation of Spark schemas from protobuf descriptors
4. **Parse mode handling**: Supports both permissive (null on error) and fail-fast modes
5. **Enhanced code generation**: `doGenCode` method generates optimized code when `parserOpt` is available
6. **Wire format optimization**: Direct parsing for binary descriptor sets when possible

## Parser Architecture

The project uses a layered parser interface hierarchy optimized for different protobuf conversion scenarios:

### Interface Hierarchy

```
Parser (trait) - Base interface for protobuf → InternalRow
├── BufferSharingParser (abstract - buffer sharing impl)
│   ├── StreamWireParser (abstract - CodedInputStream impl)
│   │   ├── WireFormatParser
│   │   └── (generated wire parsers)
│   └── AbstractMessageParser[T] (message parsing base)
│       └── (generated message parsers)
└── MessageParser[T] (trait - compiled message interface)
    ├── AbstractMessageParser[T] (also)
    └── DynamicMessageParser
```

### Core Interfaces

**`Parser`** - Base trait with `parse(binary: Array[Byte]): InternalRow` for all parsers

**`MessageParser[T]`** - Interface for compiled message objects:
- `parse(message: T): InternalRow` - Direct message conversion

### Implementation Classes

**`BufferSharingParser`** - Abstract base for memory-efficient nested conversions:
- `parseInto(binary, writer)` - Core parsing logic (abstract)
- `parseWithSharedBuffer()` - Enables buffer sharing for nested structures

**`StreamWireParser`** - Abstract base for CodedInputStream-based parsing (extends BufferSharingParser)
**`WireFormatParser`** - Direct wire format parsing for binary descriptor sets
**`AbstractMessageParser[T]`** - Abstract base for generated message parsers with buffer sharing support:
- `parseWithSharedBuffer(message, parentWriter)` - Message conversion with buffer sharing

**`DynamicMessageParser`** - Fallback parser using DynamicMessage (no buffer sharing)

### Performance Characteristics

- **Generated parsers (compiled class)**: ~1,649 ns/op (fastest)
- **Wire format parsers**: ~2,442 ns/op (1.48x slower than generated)
- **DynamicMessage parsers**: ~24,992 ns/op (15.1x slower than generated)

### Usage Examples

```scala
// Simple conversion (DynamicMessageParser)
val parser = new DynamicMessageParser(descriptor, schema)
val row = parser.parse(binaryData)  // No buffer sharing support

// Wire format conversion (BufferSharingParser)
val parser = new WireFormatParser(descriptor, schema)
val row = parser.parse(binaryData)  // Standalone conversion
parser.parseWithSharedBuffer(binaryData, parentWriter)  // Nested with buffer sharing

// Message-based conversion (Generated code)
val parser = ProtoToRowGenerator.generateParser(descriptor, messageClass)
val row = parser.parse(message)  // Parse compiled message
parser.parseWithSharedBuffer(message, parentWriter)  // Nested message conversion
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

- **Compiled class path**: Uses generated `Parser` via Janino for direct `UnsafeRow` conversion
- **Wire format path**: Uses `WireFormatParser` for direct binary parsing with binary descriptor sets  
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
sbt "core/Jmh/run .*WireFormatParser.*"

# Quick JMH test with minimal iterations
sbt "core/Jmh/run -wi 2 -i 3 -f 1"
```

The benchmarking infrastructure provides a foundation for measuring and tracking performance improvements as the codebase evolves.

## Code Generation Optimization

The `ProtobufDataToCatalyst.doGenCode` method has been enhanced to generate two distinct code paths:

1. **When `parserOpt` is available**: Generates optimized code that directly calls:
   - `parseCompiled(binary)` to get compiled protobuf message
   - `parser.parse(message)` to parse to InternalRow

2. **When `parserOpt` is `None`**: Falls back to calling `nullSafeEval()`

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

**Note**: The fallback case (`None`) should theoretically never occur when `parserOpt` is `Some`, but is included for defensive programming.

This provides better performance potential when Spark actually executes generated code (vs pre-computed paths).

## BufferSharingParser Implementation Details

The `BufferSharingParser` provides the foundation for parsers that support nested buffer sharing for memory-efficient nested message handling.

### Method Contract

- **`parseInto(binary, writer)`**: Abstract method that parses protobuf and writes to absolute ordinals (0, 1, 2)
- **`parseWithSharedBuffer(binary, parentWriter)`**: Handles writer preparation and buffer sharing
- **`acquireWriter(parentWriter)`**: Manages writer acquisition and reuse for child structures

### Usage Pattern

For nested message arrays, always use `parseWithSharedBuffer()` which manages buffer sharing properly. Direct `parseInto()` usage in nested contexts can overwrite parent row data.

### Interface Benefits

This design separates parsing logic (`parseInto`) from writer management (`parseWithSharedBuffer`), enabling both standalone conversion and efficient nested message handling with shared buffers. The `acquireWriter` method provides a clean abstraction for writer lifecycle management.

## Wire Format Optimization Tips

**Raw method usage**: Replace wrapper methods with direct raw methods to avoid indirection:
- `readInt32/readUInt32` → `readRawVarint32`
- `readInt64/readUInt64` → `readRawVarint64`
- `readFixed32/readSFixed32` → `readRawLittleEndian32`
- `readFixed64/readSFixed64` → `readRawLittleEndian64`

**Byte handling optimizations**:
- UnsafeRow strings can be written from bytes directly without `UTF8String.fromBytes` intermediate
- Use `readByteArray()` instead of `readBytes().toByteArray()` for cleaner code