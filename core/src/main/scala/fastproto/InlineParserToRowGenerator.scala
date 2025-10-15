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
 */
object InlineParserToRowGenerator {

  // Global cache for compiled classes (classes are immutable and thread-safe)
  private val classCache: ConcurrentHashMap[String, Class[_ <: StreamWireParser]] =
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
    // Use only descriptor name for visited tracking
    val descriptorName = descriptor.getFullName

    // Skip if already wired
    if (visited.contains(descriptorName)) {
      return
    }
    visited.add(descriptorName)

    // Parser lookup still uses the full key with schema hash
    val key = s"${descriptor.getFullName}_${schema.hashCode()}"
    val parser = localParsers(key)

    // Set nested parsers - only for fields that exist in both descriptor and schema
    // Group by message type to avoid setting the same parser multiple times
    val messageFields = descriptor.getFields.asScala.filter { field =>
      field.getType == FieldDescriptor.Type.MESSAGE && schema.fieldNames.contains(field.getName)
    }

    val uniqueMessageTypes = messageFields.groupBy(_.getMessageType.getName).map(_._2.head)

    uniqueMessageTypes.foreach { field =>
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

      val setterName = s"setParser_${field.getMessageType.getName}"
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
   * Get or compile parser class using global class cache.
   */
  private def getOrCompileClass(descriptor: Descriptor, schema: StructType, key: String): Class[_ <: StreamWireParser] = {
    // Check global class cache first
    Option(classCache.get(key)) match {
      case Some(clazz) => clazz
      case None =>
        // Compile new class
        val className = s"GeneratedInlineParser_${descriptor.getName}_${Math.abs(key.hashCode)}"
        val sourceCode = InlineParserGenerator.generateParser(className, descriptor, schema)

        // Compile using Janino
        val compiler = new SimpleCompiler()
        compiler.setParentClassLoader(this.getClass.getClassLoader)
        compiler.cook(sourceCode)
        val generatedClass = compiler.getClassLoader.loadClass(s"fastproto.generated.$className").asInstanceOf[Class[_ <: StreamWireParser]]

        // Cache the compiled class globally and return
        Option(classCache.putIfAbsent(key, generatedClass)).getOrElse(generatedClass)
    }
  }

  /**
   * Compile and instantiate a single parser.
   */
  private def compileParser(descriptor: Descriptor, schema: StructType): StreamWireParser = {
    val key = s"${descriptor.getFullName}_${schema.hashCode()}"
    val parserClass = getOrCompileClass(descriptor, schema, key)

    // Instantiate parser
    val constructor = parserClass.getConstructor(classOf[StructType])
    constructor.newInstance(schema)
  }
}
