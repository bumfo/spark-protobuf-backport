package fastproto

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import org.apache.spark.sql.types._
import org.codehaus.janino.SimpleCompiler

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Configuration for inline parser generation.
 *
 * @param canonicalKeyDepth Depth of MESSAGE field expansion in canonical keys.
 *                          Controls parser class specialization vs reuse trade-off.
 *                          - 0: Maximum class reuse (megamorphic call sites)
 *                          - 1: Balanced approach (default, monomorphic for immediate children)
 *                          - N: More specialization (monomorphic for N levels)
 */
case class InlineParserConfig(
  canonicalKeyDepth: Int = 1
) {
  require(canonicalKeyDepth >= 0, s"canonicalKeyDepth must be >= 0, got $canonicalKeyDepth")
}

object InlineParserConfig {
  val default: InlineParserConfig = InlineParserConfig()
}

/**
 * Factory object for generating optimized inline parsers using InlineParserGenerator.
 *
 * This generator uses InlineParserGenerator to produce compact, switch-based parsers
 * that leverage ProtoRuntime for boilerplate code. The generated parsers are optimized
 * for JIT jump table compilation and minimal code size (~50-100 lines vs 300+ lines).
 *
 * Uses Janino for runtime Java compilation and caches generated parsers to avoid
 * redundant compilation.
 *
 * Smart class specialization strategy:
 * - Uses unified canonical key with configurable depth expansion
 * - Canonical key captures immediate structure + nested MESSAGE field structure
 * - Field order implicit in concatenation (iterates over schema.fields)
 * - Always generates specialized classes for dispatch optimization
 *
 * Cache key format: "FQN|canonical(depth=N)"
 * Example (depth=1): "testproto.Node|1:INT32:integer,2:MESSAGE:struct:{1:INT32:integer}"
 */
object InlineParserToRowGenerator {

  /**
   * Convert a fully qualified protobuf name to a valid Java identifier.
   * Must match InlineParserGenerator.sanitizeFullName.
   */
  private def sanitizeFullName(fullName: String): String = {
    fullName.replace('.', '_')
  }

  /**
   * Convert a hash code to an unsigned string representation.
   * Preserves all 32 bits by converting to unsigned long (0 to 4294967295).
   * Avoids Math.abs edge case where Integer.MIN_VALUE stays negative.
   * Public for reuse in ShowGeneratedCode.
   */
  def unsignedHashString(hash: Int): String = {
    (hash.toLong & 0xFFFFFFFFL).toString
  }

  /**
   * Generate cache key for a given descriptor and schema.
   * Format: "FQN|canonical(depth=N)"
   * Public for reuse in ShowGeneratedCode.
   */
  def generateCacheKey(descriptor: Descriptor, schema: StructType, config: InlineParserConfig = InlineParserConfig.default): String = {
    val canonicalKey = generateCanonicalKey(descriptor, schema, depth = config.canonicalKeyDepth)
    s"${descriptor.getFullName}|$canonicalKey"
  }

  /**
   * Generate class name for a given descriptor and schema.
   * Format: "GeneratedInlineParser_<SimpleName>_<CacheKeyHash>"
   * Public for reuse in ShowGeneratedCode.
   */
  def generateClassName(descriptor: Descriptor, schema: StructType, config: InlineParserConfig = InlineParserConfig.default): String = {
    val cacheKey = generateCacheKey(descriptor, schema, config)
    s"GeneratedInlineParser_${descriptor.getName}_${unsignedHashString(cacheKey.hashCode)}"
  }

  /**
   * Detect the actual protobuf package at runtime.
   * Returns "com.google.protobuf" in unshaded environments,
   * or "org.sparkproject.spark_protobuf.protobuf" in shaded environments.
   * This enables InlineParser to work correctly regardless of shading.
   */
  private def detectProtobufPackage(): String = {
    classOf[com.google.protobuf.CodedInputStream].getPackage.getName
  }

  // Global cache for compiled classes: "FQN|canonical(depth=1)" -> Class
  private val classCache: ConcurrentHashMap[String, Class[_ <: StreamWireParser]] =
    new ConcurrentHashMap()

  // Thread-local cache for parser instances (instances have mutable state)
  private val instanceCache: ThreadLocal[scala.collection.mutable.Map[String, StreamWireParser]] =
    ThreadLocal.withInitial(() => scala.collection.mutable.Map.empty[String, StreamWireParser])

  /**
   * Generate or retrieve a cached parser for the given descriptor and schema using default config.
   * Uses two-tier caching: globally cached compiled classes + thread-local instances.
   * This avoids redundant compilation while ensuring thread safety.
   *
   * @param descriptor the protobuf message descriptor
   * @param schema     the target Spark SQL schema
   * @return an optimized inline parser
   */
  def generateParser(descriptor: Descriptor, schema: StructType): StreamWireParser = {
    generateParser(descriptor, schema, InlineParserConfig.default)
  }

  /**
   * Generate or retrieve a cached parser for the given descriptor and schema with custom config.
   * Uses two-tier caching: globally cached compiled classes + thread-local instances.
   * This avoids redundant compilation while ensuring thread safety.
   *
   * @param descriptor the protobuf message descriptor
   * @param schema     the target Spark SQL schema
   * @param config     parser generation configuration
   * @return an optimized inline parser
   */
  def generateParser(descriptor: Descriptor, schema: StructType, config: InlineParserConfig): StreamWireParser = {
    val key = s"${descriptor.getFullName}_${schema.hashCode()}_${config.hashCode()}"
    val threadInstances = instanceCache.get()

    // Check thread-local instance cache first
    threadInstances.get(key) match {
      case Some(parser) => parser
      case None =>
        // Create new parser instance for this thread (handles compilation and dependencies)
        val parser = createParserGraph(descriptor, schema, config)
        threadInstances(key) = parser
        parser
    }
  }

  /**
   * Create a parser with all its nested dependencies.
   */
  private def createParserGraph(descriptor: Descriptor, schema: StructType, config: InlineParserConfig): StreamWireParser = {
    // Create local parser map for this generation cycle
    val localParsers = scala.collection.mutable.Map[String, StreamWireParser]()

    // Generate parsers for nested types
    val rootParser = generateParserInternal(descriptor, schema, localParsers, config)

    // Wire up nested parser dependencies
    wireDependencies(localParsers, descriptor, schema, config)

    rootParser
  }

  /**
   * Generate a single parser and recursively create nested parsers.
   */
  private def generateParserInternal(
      descriptor: Descriptor,
      schema: StructType,
      localParsers: scala.collection.mutable.Map[String, StreamWireParser],
      config: InlineParserConfig
  ): StreamWireParser = {
    val key = s"${descriptor.getFullName}_${schema.hashCode()}"

    // Check if already being generated
    if (localParsers.contains(key)) {
      return localParsers(key)
    }

    // Generate the parser
    val parser = compileParser(descriptor, schema, config)
    localParsers(key) = parser

    // Generate nested parsers - only for fields that exist in both descriptor and schema
    val messageFields = descriptor.getFields.asScala.filter { field =>
      field.getType == FieldDescriptor.Type.MESSAGE && schema.fieldNames.contains(field.getName)
    }
    messageFields.foreach { field =>
      val fieldIndex = schema.fieldIndex(field.getName)
      val sparkField = schema.fields(fieldIndex)

      val nestedSchema = sparkField.dataType match {
        case struct: StructType => struct
        case ArrayType(struct: StructType, _) => struct
        case other =>
          throw new IllegalArgumentException(s"Expected StructType or ArrayType[StructType] for message field ${field.getName}, got $other")
      }

      generateParserInternal(field.getMessageType, nestedSchema, localParsers, config)
    }

    parser
  }

  /**
   * Wire up nested parser dependencies after all parsers are created.
   */
  private def wireDependencies(
      localParsers: scala.collection.mutable.Map[String, StreamWireParser],
      descriptor: Descriptor,
      schema: StructType,
      config: InlineParserConfig,
      visited: scala.collection.mutable.Set[String] = scala.collection.mutable.Set()
  ): Unit = {
    // Use descriptor name + schema hash for visited tracking
    // This is necessary for pruned schemas where the same descriptor
    // may appear multiple times with different schemas
    val key = s"${descriptor.getFullName}_${schema.hashCode()}"

    // Skip if already wired
    if (visited.contains(key)) {
      return
    }
    visited.add(key)

    // Get the parser for this descriptor+schema combination
    val parser = localParsers(key)

    // Set nested parsers - only for fields that exist in both descriptor and schema
    // Wire each field individually to support differential pruning
    val messageFields = descriptor.getFields.asScala.filter { field =>
      field.getType == FieldDescriptor.Type.MESSAGE && schema.fieldNames.contains(field.getName)
    }

    messageFields.foreach { field =>
      val fieldIndex = schema.fieldIndex(field.getName)
      val sparkField = schema.fields(fieldIndex)

      val nestedSchema = sparkField.dataType match {
        case struct: StructType => struct
        case ArrayType(struct: StructType, _) => struct
        case other =>
          throw new IllegalArgumentException(s"Expected StructType or ArrayType[StructType] for message field ${field.getName}, got $other")
      }

      val nestedKey = s"${field.getMessageType.getFullName}_${nestedSchema.hashCode()}"
      val threadInstances = instanceCache.get()
      val nestedParser = localParsers.get(nestedKey).orElse(
        threadInstances.get(nestedKey)
      ).getOrElse(
        throw new IllegalStateException(s"Nested parser not found: $nestedKey")
      )

      // Setter name format: setParser<fieldNumber> using proto field number (e.g., setParser1, setParser5)
      val setterName = s"setParser${field.getNumber}"
      val setterMethod = parser.getClass.getMethod(setterName, classOf[StreamWireParser])
      setterMethod.invoke(parser, nestedParser)
    }

    // Recursively wire nested dependencies
    messageFields.foreach { field =>
      val fieldIndex = schema.fieldIndex(field.getName)
      val sparkField = schema.fields(fieldIndex)

      val nestedSchema = sparkField.dataType match {
        case struct: StructType => struct
        case ArrayType(struct: StructType, _) => struct
        case _ => throw new IllegalArgumentException(s"Expected StructType or ArrayType[StructType] for message field ${field.getName}")
      }

      wireDependencies(localParsers, field.getMessageType, nestedSchema, config, visited)
    }
  }

  /**
   * Generate a canonical key that captures parser structure up to specified depth.
   *
   * The canonical key encodes:
   * - Proto field numbers (wire format tag identification)
   * - Proto types (wire format parsing logic)
   * - Schema types (Spark SQL output types)
   * - Field order (implicit in concatenation order, iterates over schema.fields)
   *
   * Depth parameter controls MESSAGE field expansion:
   * - depth=0: MESSAGE fields use generic markers (no nested structure)
   * - depth=1: MESSAGE fields expanded to show immediate children's structure
   * - depth=N: MESSAGE fields expanded N levels deep
   *
   * Standard usage: depth=1 for optimal monomorphic dispatch.
   * This captures immediate structure + one level of nesting, enabling class reuse
   * for parsers with identical immediate+child structure but different grandchildren.
   *
   * Field signature format:
   * - Scalar: "proto_num:proto_type:schema_type"
   * - Message (depth=0): "proto_num:MESSAGE:schema_type"
   * - Message (depth>0): "proto_num:MESSAGE:schema_type:{nested_canonical}"
   *
   * Examples:
   * - depth=0: "1:INT32:integer,2:MESSAGE:struct"
   * - depth=1: "1:INT32:integer,2:MESSAGE:struct:{1:INT32:integer}"
   */
  private def generateCanonicalKey(descriptor: Descriptor, schema: StructType, depth: Int): String = {
    schema.fields.map { sparkField =>
      val fieldName = sparkField.name
      val protoField = Option(descriptor.findFieldByName(fieldName))
        .getOrElse(throw new IllegalArgumentException(
          s"Schema field '$fieldName' not found in proto descriptor ${descriptor.getFullName}"))

      val protoType = if (protoField.isRepeated) {
        s"${protoField.getType}[]"
      } else {
        protoField.getType.toString
      }

      val schemaType = sparkField.dataType.typeName

      // Expand MESSAGE fields if depth > 0
      if (protoField.getType == FieldDescriptor.Type.MESSAGE && depth > 0) {
        val nestedSchema = sparkField.dataType match {
          case struct: StructType => struct
          case ArrayType(struct: StructType, _) => struct
          case other =>
            throw new IllegalArgumentException(
              s"Expected StructType or ArrayType[StructType] for message field $fieldName, got $other")
        }

        val nestedCanonical = generateCanonicalKey(protoField.getMessageType, nestedSchema, depth - 1)
        s"${protoField.getNumber}:$protoType:$schemaType:{$nestedCanonical}"
      } else {
        s"${protoField.getNumber}:$protoType:$schemaType"
      }
    }.mkString(",")
  }


  /**
   * Get or compile parser class using unified canonical key with configurable depth.
   * Cache key format: "FQN|canonical(depth=N)"
   */
  private def getOrCompileClass(descriptor: Descriptor, schema: StructType, config: InlineParserConfig): Class[_ <: StreamWireParser] = {
    val cacheKey = generateCacheKey(descriptor, schema, config)

    // Check if class already exists
    Option(classCache.get(cacheKey)) match {
      case Some(clazz) => clazz
      case None =>
        // Compile new class
        val className = generateClassName(descriptor, schema, config)
        val protobufPackage = detectProtobufPackage()
        val sourceCode = InlineParserGenerator.generateParser(className, descriptor, schema, protobufPackage)

        // Compile using Janino
        val compiler = new SimpleCompiler()
        compiler.setParentClassLoader(this.getClass.getClassLoader)
        compiler.cook(sourceCode)
        val generatedClass = compiler.getClassLoader.loadClass(s"fastproto.generated.$className").asInstanceOf[Class[_ <: StreamWireParser]]

        // Cache the compiled class and return
        Option(classCache.putIfAbsent(cacheKey, generatedClass)).getOrElse(generatedClass)
    }
  }

  /**
   * Compile and instantiate a single parser.
   * Uses unified canonical key with configurable depth for class lookup.
   */
  private def compileParser(descriptor: Descriptor, schema: StructType, config: InlineParserConfig): StreamWireParser = {
    val parserClass = getOrCompileClass(descriptor, schema, config)

    // Instantiate parser with full schema
    val constructor = parserClass.getConstructor(classOf[StructType])
    constructor.newInstance(schema)
  }
}
