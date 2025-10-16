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
- `Parser` - Base interface for protobuf binary → `InternalRow` conversion
- `BufferSharingParser` - Abstract base with buffer sharing for nested conversions
- `MessageParser[T]` - Interface for compiled protobuf message → `InternalRow` conversion
- `ProtoToRowGenerator` - Generates message parsers using Janino
- `WireFormatToRowGenerator` - Generates wire format parsers using Janino
- `StreamWireParser` - Java base class for CodedInputStream-based parsing
- `WireFormatParser` - Direct wire format parsing for binary descriptor sets
- `AbstractMessageParser[T]` - Base for generated message parsers with buffer sharing
- `DynamicMessageParser` - Fallback parser using DynamicMessage approach

**Spark Compatibility Shims** (`shims/` subdirectory):
- Error handling and query compilation compatibility layer for Spark 3.2.1

### Key Design Patterns

1. **Three-tier execution paths**: Generated message parsers (fastest) → Wire format parsers → DynamicMessage (fallback)
2. **Binary descriptor set support**: Eliminates executor file access requirements
3. **Buffer sharing optimization**: Memory-efficient nested conversions via shared UnsafeRowWriter
4. **Parse mode handling**: Supports both permissive (null on error) and fail-fast modes
5. **Code generation**: Janino-based parser generation for optimal performance

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
- **Protobuf version**: 3.21.7 (shaded to avoid conflicts)
- **Shading**: All protobuf classes shaded under `org.sparkproject.spark.protobuf311.*` in uber JAR

## Testing

**`tests/` module**: Systematic three-tier testing of protobuf parsing (all primitive types, packed/unpacked, nested messages, edge cases). See `tests/CLAUDE.md` for details.

**`core/` and `bench/` tests**: Implementation-specific debugging tests for individual components.

```bash
# Three-tier test commands (tests module)
sbt unitTests           # Tier 1: <5s
sbt propertyTests       # Tier 2: <30s
sbt integrationTests    # Tier 3: <60s
sbt allTestTiers        # All tiers sequentially

# Run specific test suite or spec by pattern (any module)
sbt 'testOnly unit.InlineParserSpec'
sbt 'testOnly unit.InlineParserSpec -- -z "packed repeated"'

# View generated InlineParser code for tests module proto (saves to /tmp/generated_inline_parser_*.txt)
sbt 'showGeneratedCode AllPrimitiveTypes'
```

## Nested Schema Pruning

The connector includes an optimizer rule (`ProtobufSchemaPruning`) that prunes unused nested fields during query optimization.

**Implementation**:
- **Optimizer rule**: `org.apache.spark.sql.protobuf.backport.optimizer.ProtobufSchemaPruning`
- **Configuration**: `ProtobufConfig.nestedSchemaPruningEnabled` (default: true)
- **Schema utilities**: `SchemaUtils.pruneSchema()` builds minimal required schemas

**How it works**:
1. Identifies `Project` operations accessing protobuf columns
2. Analyzes `GetStructField` expressions to determine accessed fields
3. Builds minimal schema containing only required field paths
4. Rewrites `ProtobufDataToCatalyst` with pruned schema
5. `WireFormatParser` skips fields not in schema (via `rowOrdinals == -1`)

**Scope**:
- Applies only to `WireFormat` parser (binary descriptor set usage)
- Generated message parsers and DynamicMessage parsers unchanged for simplicity
- Tests in `integration/SchemaPruningSpec.scala` verify correctness

**Testing**:
```bash
# Verify optimizer actually prunes schemas (not just correctness)
sbt 'testOnly integration.SchemaPruningSpec -- -z "actually prune schema"'

# Run all schema pruning tests
sbt 'testOnly integration.SchemaPruningSpec'
``` 

## Performance Benchmarking

```bash
# Run JMH benchmarks (in bench module)
sbt jmh

# Run specific benchmarks
sbt 'bench/Jmh/run .*WireFormatParser.*'
```

## Implementation Notes

### Code Generation
- **ProtobufDataToCatalyst**: Generates optimized code paths when parsers are available
- **Runtime class resolution**: Supports both shaded and non-shaded protobuf usage
- **Parser priority**: Generated message parsers → Wire format parsers → DynamicMessage fallback

### Buffer Sharing Pattern
- **`parseInto(binary, writer)`**: Core parsing logic that writes to UnsafeRowWriter
- **`parseWithSharedBuffer()`**: Manages writer lifecycle and buffer sharing for nested structures
- **Memory efficiency**: Shared buffers reduce allocations in nested message conversions

### NullDefaultRowWriter Optimization

**`NullDefaultRowWriter`** - Specialized UnsafeRowWriter for sparse protobuf data:
- **Default-null semantics**: All fields initialize as null via `setAllNullBytes()` (vs standard UnsafeRowWriter's default-nonnull)
- **Automatic null bit management**: Write methods automatically clear null bits, eliminating manual `clearNullAt()` calls
- **Safer for protobuf**: Unwritten fields remain properly null instead of containing uninitialized data
- **Performance optimizations**:
  - `writeBytes()` for UTF8 strings avoids `UTF8String.fromBytes()` intermediate objects
  - `writeVariableField()` combines offset/size writing with null bit clearing
- **Usage pattern**: Call `resetRowWriter()` → write field data → null bits handled automatically

### Wire Format Optimization Tips

**Raw method usage**: Replace wrapper methods with direct raw methods to avoid indirection:
- `readInt32/readUInt32` → `readRawVarint32`
- `readInt64/readUInt64` → `readRawVarint64`
- `readFixed32/readSFixed32` → `readRawLittleEndian32`
- `readFixed64/readSFixed64` → `readRawLittleEndian64`

**Byte handling optimizations**:
- UnsafeRow strings can be written from bytes directly without `UTF8String.fromBytes` intermediate
- Use `readByteArray()` instead of `readBytes().toByteArray()` for cleaner code

## Referencing Source Dependencies

```bash
COURSIER_CACHE=~/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2

# Protobuf Java 3.21.7 sources
PROTOBUF_JAR=$COURSIER_CACHE/com/google/protobuf/protobuf-java/3.21.7/protobuf-java-3.21.7-sources.jar

# Spark 3.2.1 sources
SPARK_CATALYST_JAR=$COURSIER_CACHE/org/apache/spark/spark-catalyst_2.12/3.2.1/spark-catalyst_2.12-3.2.1-sources.jar
SPARK_SQL_JAR=$COURSIER_CACHE/org/apache/spark/spark-sql_2.12/3.2.1/spark-sql_2.12-3.2.1-sources.jar
SPARK_CORE_JAR=$COURSIER_CACHE/org/apache/spark/spark-core_2.12/3.2.1/spark-core_2.12-3.2.1-sources.jar

# Search for files (use rg if available, otherwise grep)
jar tf $PROTOBUF_JAR | rg CodedInputStream
jar tf $SPARK_CATALYST_JAR | rg UnsafeWriter
jar tf $SPARK_SQL_JAR | rg FileScan.scala

# Read source files - inline jar paths for bash commands
unzip -p $PROTOBUF_JAR com/google/protobuf/CodedInputStream.java | sed -n '1,200p'
unzip -p $SPARK_CATALYST_JAR org/apache/spark/sql/catalyst/expressions/codegen/UnsafeWriter.java | sed -n '113,120p'
```

**Note**: When using bash commands, inline the jar path variables for direct execution.