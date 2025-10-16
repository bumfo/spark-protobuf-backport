package fastproto

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import org.apache.spark.sql.types._
import org.codehaus.janino.SimpleCompiler

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

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
 * - Uses two-level cache key: {canonical_hash}-{nested_hash}
 * - Leaf parsers always use shared class (nested_hash = 0)
 * - Non-leaf parsers: generate specialized classes if ≤ threshold configs, else share
 * - Threshold prevents code explosion while preserving monomorphic dispatch
 */
object InlineParserToRowGenerator {

  /**
   * Threshold for generating specialized classes vs shared classes.
   * If a canonical key has ≤ SPECIALIZATION_THRESHOLD nested configurations,
   * generate specialized classes for each (monomorphic dispatch).
   * If > threshold, use shared class (polymorphic dispatch, save code size).
   */
  private val SPECIALIZATION_THRESHOLD = 10

  /**
   * Convert a fully qualified protobuf name to a valid Java identifier.
   * Must match InlineParserGenerator.sanitizeFullName.
   */
  private def sanitizeFullName(fullName: String): String = {
    fullName.replace('.', '_')
  }

  // Global cache for compiled classes: canonical_key -> (nested_hash -> Class)
  // nested_hash = "0" for shared version, actual hash for specialized versions
  private val classCache: ConcurrentHashMap[String, ConcurrentHashMap[String, Class[_ <: StreamWireParser]]] =
    new ConcurrentHashMap()

  // Track nested configuration counts per canonical key for threshold decision
  private val nestedConfigCounts: ConcurrentHashMap[String, java.util.concurrent.atomic.AtomicInteger] =
    new ConcurrentHashMap()

  // Thread-local cache for parser instances (instances have mutable state)
  private val instanceCache: ThreadLocal[scala.collection.mutable.Map[String, StreamWireParser]] =
    ThreadLocal.withInitial(() => scala.collection.mutable.Map.empty[String, StreamWireParser])

  /**
   * Generate or retrieve a cached parser for the given descriptor and schema.
   * Uses two-tier caching: globally cached compiled classes + thread-local instances.
   * This avoids redundant compilation while ensuring thread safety.
   *
   * @param descriptor the protobuf message descriptor
   * @param schema     the target Spark SQL schema
   * @return an optimized inline parser
   */
  def generateParser(descriptor: Descriptor, schema: StructType): StreamWireParser = {
    val key = s"${descriptor.getFullName}_${schema.hashCode()}"
    val threadInstances = instanceCache.get()

    // Check thread-local instance cache first
    threadInstances.get(key) match {
      case Some(parser) => parser
      case None =>
        // Create new parser instance for this thread (handles compilation and dependencies)
        val parser = createParserGraph(descriptor, schema)
        threadInstances(key) = parser
        parser
    }
  }

  /**
   * Create a parser with all its nested dependencies.
   */
  private def createParserGraph(descriptor: Descriptor, schema: StructType): StreamWireParser = {
    // Create local parser map for this generation cycle
    val localParsers = scala.collection.mutable.Map[String, StreamWireParser]()

    // Generate parsers for nested types
    val rootParser = generateParserInternal(descriptor, schema, localParsers)

    // Wire up nested parser dependencies
    wireDependencies(localParsers, descriptor, schema)

    rootParser
  }

  /**
   * Generate a single parser and recursively create nested parsers.
   */
  private def generateParserInternal(
      descriptor: Descriptor,
      schema: StructType,
      localParsers: scala.collection.mutable.Map[String, StreamWireParser]
  ): StreamWireParser = {
    val key = s"${descriptor.getFullName}_${schema.hashCode()}"

    // Check if already being generated
    if (localParsers.contains(key)) {
      return localParsers(key)
    }

    // Generate the parser
    val parser = compileParser(descriptor, schema)
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

      generateParserInternal(field.getMessageType, nestedSchema, localParsers)
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

      // Setter name format: setParser_<fieldName>_<MessageTypeName>
      val setterName = s"setParser_${field.getName}_${field.getMessageType.getName}"
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

      wireDependencies(localParsers, field.getMessageType, nestedSchema, visited)
    }
  }

  /**
   * Generate a canonical key based only on the field structure at the current level.
   * This enables class reuse for parsers with identical field structures but different
   * nested schema depths (e.g., recursive types with pruning).
   *
   * The canonical key includes:
   * - Descriptor full name
   * - Field names and ordinals (sorted for determinism)
   * - Field types (scalar, string, bytes, message, repeated)
   * - For message fields: nested descriptor name only (not schema hash)
   */
  private def generateCanonicalKey(descriptor: Descriptor, schema: StructType): String = {
    val fields = descriptor.getFields.asScala
      .filter(f => schema.fieldNames.contains(f.getName))
      .map { field =>
        val fieldIndex = schema.fieldIndex(field.getName)
        val sparkField = schema.fields(fieldIndex)

        val typeSignature = (field.getType, sparkField.dataType) match {
          case (FieldDescriptor.Type.MESSAGE, _: StructType) =>
            s"msg:${field.getMessageType.getFullName}"
          case (FieldDescriptor.Type.MESSAGE, ArrayType(_: StructType, _)) =>
            s"msg[]:${field.getMessageType.getFullName}"
          case (_, ArrayType(elementType, _)) =>
            s"${elementType.typeName}[]"
          case (_, dataType) =>
            dataType.typeName
        }

        s"${field.getNumber}:${field.getName}:$typeSignature"
      }
      .toSeq
      .sorted
      .mkString(",")

    s"${descriptor.getFullName}|$fields"
  }

  /**
   * Compute nested key from all nested parser canonical keys.
   * Returns "0" for leaf parsers (no nested message fields).
   * For non-leaf parsers, returns hash of all nested canonical keys.
   */
  private def computeNestedKey(descriptor: Descriptor, schema: StructType): String = {
    val messageFields = descriptor.getFields.asScala.filter { field =>
      field.getType == FieldDescriptor.Type.MESSAGE && schema.fieldNames.contains(field.getName)
    }

    // Leaf parser: no nested message fields
    if (messageFields.isEmpty) {
      return "0"
    }

    // Compute nested key from all nested parser canonical keys
    val nestedKeys = messageFields
      .sortBy(_.getNumber)
      .map { field =>
        val fieldIndex = schema.fieldIndex(field.getName)
        val sparkField = schema.fields(fieldIndex)

        val nestedSchema = sparkField.dataType match {
          case struct: StructType => struct
          case ArrayType(struct: StructType, _) => struct
          case other =>
            throw new IllegalArgumentException(s"Expected StructType or ArrayType[StructType] for message field ${field.getName}, got $other")
        }

        val nestedCanonicalKey = generateCanonicalKey(field.getMessageType, nestedSchema)
        s"${field.getName}:$nestedCanonicalKey"
      }
      .mkString("|")

    Math.abs(nestedKeys.hashCode).toString
  }

  /**
   * Get or compile parser class using two-level cache with smart specialization.
   * Uses canonical key + nested key for class generation.
   * Applies threshold logic to decide between shared vs specialized classes.
   */
  private def getOrCompileClass(descriptor: Descriptor, schema: StructType): Class[_ <: StreamWireParser] = {
    val canonicalKey = generateCanonicalKey(descriptor, schema)
    val nestedKey = computeNestedKey(descriptor, schema)

    // Decide whether to use shared (0) or specialized (actual hash) version
    val effectiveNestedKey = if (nestedKey == "0") {
      // Leaf parser: always use shared
      "0"
    } else {
      // Non-leaf parser: apply threshold logic
      val counter = nestedConfigCounts.computeIfAbsent(
        canonicalKey,
        _ => new java.util.concurrent.atomic.AtomicInteger(0)
      )
      val configCount = counter.incrementAndGet()

      if (configCount <= SPECIALIZATION_THRESHOLD) {
        // Under threshold: use specialized class for monomorphic dispatch
        nestedKey
      } else {
        // Over threshold: use shared class to prevent code explosion
        "0"
      }
    }

    // Get or create nested cache map for this canonical key
    val nestedCache = classCache.computeIfAbsent(
      canonicalKey,
      _ => new ConcurrentHashMap[String, Class[_ <: StreamWireParser]]()
    )

    // Check if class already exists for this nested key
    Option(nestedCache.get(effectiveNestedKey)) match {
      case Some(clazz) => clazz
      case None =>
        // Compile new class
        val className = s"GeneratedInlineParser_${descriptor.getName}_${Math.abs(canonicalKey.hashCode)}_$effectiveNestedKey"
        val sourceCode = InlineParserGenerator.generateParser(className, descriptor, schema)

        // Compile using Janino
        val compiler = new SimpleCompiler()
        compiler.setParentClassLoader(this.getClass.getClassLoader)
        compiler.cook(sourceCode)
        val generatedClass = compiler.getClassLoader.loadClass(s"fastproto.generated.$className").asInstanceOf[Class[_ <: StreamWireParser]]

        // Cache the compiled class and return
        Option(nestedCache.putIfAbsent(effectiveNestedKey, generatedClass)).getOrElse(generatedClass)
    }
  }

  /**
   * Compile and instantiate a single parser.
   * Uses canonical key for class lookup (enables class reuse),
   * but creates instance with full schema (preserves per-instance state).
   */
  private def compileParser(descriptor: Descriptor, schema: StructType): StreamWireParser = {
    val parserClass = getOrCompileClass(descriptor, schema)

    // Instantiate parser with full schema
    val constructor = parserClass.getConstructor(classOf[StructType])
    constructor.newInstance(schema)
  }
}
