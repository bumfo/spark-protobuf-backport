package fastproto

// Use JavaConverters for Scala 2.12 compatibility

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.{ByteString, Message, ProtocolMessageEnum}
import org.apache.spark.sql.types._
import org.codehaus.janino.SimpleCompiler

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Factory object for generating [[Parser]] instances on the fly.  Given a
 * Protobuf [[Descriptor]] and the corresponding compiled message class, this
 * object synthesises a small Java class that extracts each field from the
 * message and writes it into a new array.  The generated class implements
 * [[Parser]] for the provided message type.  Janino is used to
 * compile the generated Java code at runtime.  Only a subset of types is
 * currently supported: primitive numeric types, booleans, strings, byte
 * strings and enums.  Nested messages and repeated fields are emitted as
 * `null` placeholders and can be extended with recursive conversion logic.
 */
object ProtoToRowGenerator {

  // Global cache for compiled classes (classes are immutable and thread-safe)
  private val classCache: ConcurrentHashMap[String, Class[_ <: Parser]] =
    new ConcurrentHashMap()

  // Thread-local cache for parser instances (instances have mutable state)
  private val instanceCache: ThreadLocal[scala.collection.mutable.Map[String, Parser]] =
    ThreadLocal.withInitial(() => scala.collection.mutable.Map.empty[String, Parser])


  /**
   * Compute the accessor method suffix for a given field descriptor.  This
   * converts snake_case names into CamelCase, capitalising each segment.
   * For example, a field named "source_context" yields "SourceContext".
   */
  private def accessorName(fd: FieldDescriptor): String = {
    val name = fd.getName
    name.split("_").iterator.map { part =>
      if (part.isEmpty) "" else part.substring(0, 1).toUpperCase + part.substring(1)
    }.mkString("")
  }

  /**
   * Generate a concrete [[AbstractMessageParser]] for the given Protobuf message type.
   * Uses internal caching to avoid redundant Janino compilation for the same
   * descriptor and message class combinations. Handles recursive types by
   * creating parser graphs locally before updating the global cache.
   *
   * @param descriptor   the Protobuf descriptor describing the message schema
   * @param messageClass the compiled Protobuf Java class
   * @param schema       the Spark SQL schema for the message
   * @tparam T the concrete type of the message
   * @return a [[AbstractMessageParser]] capable of converting the message into an
   *         [[org.apache.spark.sql.catalyst.InternalRow]]
   */
  def generateParser[T <: Message](
      descriptor: Descriptor,
      messageClass: Class[T],
      schema: StructType): AbstractMessageParser[T] = {
    val key = s"${messageClass.getName}_${descriptor.getFullName}"
    val threadInstances = instanceCache.get()

    // Check thread-local instance cache first
    threadInstances.get(key) match {
      case Some(parser) => return parser.asInstanceOf[AbstractMessageParser[T]]
      case None => // Continue with generation
    }

    // Local map to track parsers being built (excludes already cached ones)
    // Map key -> (parser, neededTypeKeys)
    val localParsers = scala.collection.mutable.Map[String, (Parser, Set[String])]()

    // Phase 1: Create parser graph with null dependencies
    val rootParser = createParserGraph(descriptor, messageClass, schema, localParsers)

    // Phase 2: Wire up dependencies
    wireDependencies(localParsers)

    // Phase 3: Update thread-local cache
    localParsers.foreach { case (k, (conv, _)) =>
      threadInstances(k) = conv
    }

    rootParser.asInstanceOf[AbstractMessageParser[T]]
  }

  /**
   * Create a parser graph for the given descriptor and class, recursively
   * building parsers for nested types. Returns existing parsers from
   * global cache when available to avoid duplication.
   */
  private def createParserGraph(
      descriptor: Descriptor,
      messageClass: Class[_ <: Message],
      schema: StructType,
      localParsers: scala.collection.mutable.Map[String, (Parser, Set[String])]): Parser = {
    val key = s"${messageClass.getName}_${descriptor.getFullName}"

    // Check thread-local cache FIRST (important optimization!)
    val threadInstances = instanceCache.get()
    threadInstances.get(key) match {
      case Some(parser) => return parser
      case None => // Not cached in this thread
    }

    // Check local map
    if (localParsers.contains(key)) return localParsers(key)._1

    // Collect all nested message fields (per-field approach for now)
    // Exclude map fields since they don't need nested parsers - they're handled as Maps
    val nestedFields = descriptor.getFields.asScala.filter { fd =>
      fd.getJavaType == FieldDescriptor.JavaType.MESSAGE &&
      !(fd.isRepeated && fd.getMessageType.getOptions.hasMapEntry)
    }.toList
    val nestedTypes: Map[String, (Descriptor, Class[_ <: Message])] = nestedFields.map { fd =>
      val nestedClass = getNestedMessageClass(fd, messageClass)
      val typeKey = s"${nestedClass.getName}_${fd.getMessageType.getFullName}"
      typeKey -> (fd.getMessageType, nestedClass)
    }.toMap

    // Create parser with null dependencies initially
    val parser = generateParserWithNullDeps(descriptor, messageClass, schema, nestedTypes.size)
    val neededTypeKeys = nestedTypes.map { case (typeKey, (desc, clazz)) =>
      s"${clazz.getName}_${desc.getFullName}"
    }.toSet
    localParsers(key) = (parser, neededTypeKeys)

    // Recursively create parsers for unique nested types
    nestedTypes.values.foreach { case (desc, clazz) =>
      // For nested types, we'll use a simple fallback - in practice the schema
      // passed to the root parser should handle all nested structures
      // FIXME: Pass nested schemas instead of root schema, see https://github.com/bumfo/spark-protobuf-backport/pull/19/files/0953545dc109e448842a6a706054d4a65f7ea15a#r2366204259
      createParserGraph(desc, clazz, schema, localParsers)
    }

    parser
  }

  /**
   * Wire dependencies between parsers after all have been created.
   */
  private def wireDependencies(localParsers: scala.collection.mutable.Map[String, (Parser, Set[String])]): Unit = {
    val threadInstances = instanceCache.get()
    localParsers.foreach { case (parserKey, (parser, neededTypes)) =>
      // Set dependencies on this parser
      val dependencies = neededTypes.toSeq.map { typeKey =>
        localParsers.get(typeKey) match {
          case Some((nestedParser, _)) => nestedParser
          case None => threadInstances.getOrElse(typeKey, null)
        }
      }
      setParserDependencies(parser, dependencies)
    }
  }

  /**
   * Extract nested message class from field descriptor using reflection.
   */
  private def getNestedMessageClass(fd: FieldDescriptor, messageClass: Class[_ <: Message]): Class[_ <: Message] = {
    val accessor = accessorName(fd)
    if (fd.isRepeated) {
      val m = messageClass.getMethod(s"get${accessor}", classOf[Int])
      m.getReturnType.asInstanceOf[Class[_ <: Message]]
    } else {
      val m = messageClass.getMethod(s"get${accessor}")
      m.getReturnType.asInstanceOf[Class[_ <: Message]]
    }
  }

  /**
   * Set dependencies on a parser using reflection (temporary - will be replaced
   * by proper generated setter methods).
   */
  private def setParserDependencies(parser: Parser, dependencies: Seq[Parser]): Unit = {
    // For now, use reflection to set dependencies
    // This will be replaced by proper generated setter methods
    val parserClass = parser.getClass
    dependencies.zipWithIndex.foreach { case (dep, idx) =>
      try {
        val setterMethod = parserClass.getMethod(s"setNestedParser${idx}", classOf[AbstractMessageParser[_]])
        setterMethod.invoke(parser, dep)
      } catch {
        case _: NoSuchMethodException => // Parser has no dependencies, ignore
      }
    }
  }


  /**
   * Internal method that performs the actual parser generation and compilation
   * with null dependencies initially. This allows recursive types to be handled
   * by deferring dependency injection until after all parsers are created.
   *
   * @param descriptor     the Protobuf descriptor describing the message schema
   * @param messageClass   the compiled Protobuf Java class
   * @param numNestedTypes the number of unique nested message types
   * @return a [[Parser]] capable of converting the message into an
   *         [[org.apache.spark.sql.catalyst.InternalRow]]
   */
  private def generateParserWithNullDeps(
      descriptor: Descriptor,
      messageClass: Class[_ <: Message],
      schema: StructType,
      numNestedTypes: Int): Parser = {
    // Create a unique class name to avoid collisions when multiple parsers are generated
    val className = s"GeneratedParser_${descriptor.getName}_${System.nanoTime()}"

    val sourceCode = generateParserSourceCode(className, descriptor, messageClass, numNestedTypes)
    compileAndInstantiate(sourceCode, className, schema)
  }

  /**
   * Generate Java source code for a AbstractMessageParser implementation.
   * This method creates the complete Java class source code including imports,
   * class declaration, fields, constructor, and conversion methods.
   *
   * @param className      the name of the generated Java class
   * @param descriptor     the Protobuf descriptor for the message
   * @param messageClass   the compiled Java class for the message
   * @param numNestedTypes the number of nested message types
   * @return StringBuilder containing the complete Java source code
   */
  def generateParserSourceCode(
      className: String,
      descriptor: Descriptor,
      messageClass: Class[_ <: Message],
      numNestedTypes: Int): StringBuilder = {
    // Create per-field parser indices for message fields (excluding maps)
    val messageFields = descriptor.getFields.asScala.filter { fd =>
      fd.getJavaType == FieldDescriptor.JavaType.MESSAGE &&
      !(fd.isRepeated && fd.getMessageType.getOptions.hasMapEntry)
    }.toList

    // Verify we have the expected number of nested types
    assert(messageFields.size == numNestedTypes,
      s"Expected $numNestedTypes nested types but found ${messageFields.size}")

    // Create field-to-parser-index mapping for code generation
    val fieldToParserIndex: Map[FieldDescriptor, Int] = messageFields.zipWithIndex.toMap

    val code = new StringBuilder
    // Imports required by the generated Java source
    code ++= "import org.apache.spark.sql.catalyst.expressions.UnsafeRow;\n"
    code ++= "import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;\n"
    code ++= "import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;\n"
    code ++= "import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter;\n"
    code ++= "import org.apache.spark.sql.catalyst.InternalRow;\n"
    code ++= "import org.apache.spark.sql.types.StructType;\n"
    code ++= "import org.apache.spark.unsafe.types.UTF8String;\n"
    code ++= "import fastproto.AbstractMessageParser;\n"
    code ++= "import fastproto.UnsafeRowWriterHelper;\n"
    code ++= "import java.util.Map;\n"

    // Begin class declaration
    code ++= s"public final class ${className} extends fastproto.AbstractMessageParser<${messageClass.getName}> {\n"

    // Declare fields: nested parsers (non-final for setter injection)
    (0 until numNestedTypes).foreach { idx =>
      code ++= s"  private fastproto.AbstractMessageParser nestedConv${idx}; // Non-final for dependency injection\n"
    }

    // Constructor with null nested parsers initially
    code ++= s"  public ${className}(StructType schema) {\n"
    code ++= "    super(schema);\n"
    (0 until numNestedTypes).foreach { idx =>
      code ++= s"    this.nestedConv${idx} = null; // Will be set via setter\n"
    }
    code ++= "  }\n"

    // Generate setter methods for dependency injection
    (0 until numNestedTypes).foreach { idx =>
      code ++= s"  public void setNestedParser${idx}(fastproto.AbstractMessageParser conv) {\n"
      code ++= "    if (this.nestedConv" + idx + " != null) throw new IllegalStateException(\"Parser " + idx + " already set\");\n"
      code ++= s"    this.nestedConv${idx} = conv;\n"
      code ++= s"  }\n"
    }

    // Generate field-specific methods for complex fields
    descriptor.getFields.asScala.zipWithIndex.foreach { case (fd, idx) =>
      val fieldName = fd.getName
      val accessor = accessorName(fd)
      val countMethodName = s"get${accessor}Count"
      val indexGetterName = s"get${accessor}"
      val getBytesMethodName = s"get${accessor}Bytes"

      // Generate methods for fields that have substantial code (8+ lines)
      fd.getJavaType match {
        case FieldDescriptor.JavaType.STRING if fd.isRepeated =>
          // Generate method for repeated string field
          code ++= s"  private void write${accessor}Field(UnsafeRowWriter writer, ${messageClass.getName} msg) {\n"
          code ++= s"    int count = msg.${countMethodName}();\n"
          code ++= s"    int offset = writer.cursor();\n"
          code ++= s"    UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 8);\n"
          code ++= s"    arrayWriter.initialize(count);\n"
          code ++= s"    for (int i = 0; i < count; i++) {\n"
          code ++= s"      ${classOf[ByteString].getName} bs = msg.${getBytesMethodName}(i);\n"
          code ++= s"      byte[] bytes = bs.toByteArray();\n"
          code ++= s"      arrayWriter.write(i, bytes);\n"
          code ++= s"    }\n"
          code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
          code ++= s"  }\n\n"

        case FieldDescriptor.JavaType.BYTE_STRING if fd.isRepeated =>
          // Generate method for repeated ByteString field
          code ++= s"  private void write${accessor}Field(UnsafeRowWriter writer, ${messageClass.getName} msg) {\n"
          code ++= s"    int count = msg.${countMethodName}();\n"
          code ++= s"    int offset = writer.cursor();\n"
          code ++= s"    UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 8);\n"
          code ++= s"    arrayWriter.initialize(count);\n"
          code ++= s"    for (int i = 0; i < count; i++) {\n"
          code ++= s"      " + classOf[ByteString].getName + s" bs = msg.${indexGetterName}(i);\n"
          code ++= s"      if (bs == null) { arrayWriter.setNull(i); } else { arrayWriter.write(i, bs.toByteArray()); }\n"
          code ++= s"    }\n"
          code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
          code ++= s"  }\n\n"

        case FieldDescriptor.JavaType.INT if fd.isRepeated =>
          // Generate method for repeated int field
          code ++= s"  private void write${accessor}Field(UnsafeRowWriter writer, ${messageClass.getName} msg) {\n"
          code ++= s"    int count = msg.${countMethodName}();\n"
          code ++= s"    int offset = writer.cursor();\n"
          code ++= s"    UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 4);\n"
          code ++= s"    arrayWriter.initialize(count);\n"
          code ++= s"    for (int i = 0; i < count; i++) {\n"
          code ++= s"      arrayWriter.write(i, msg.${indexGetterName}(i));\n"
          code ++= s"    }\n"
          code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
          code ++= s"  }\n\n"

        case FieldDescriptor.JavaType.LONG if fd.isRepeated =>
          // Generate method for repeated long field
          code ++= s"  private void write${accessor}Field(UnsafeRowWriter writer, ${messageClass.getName} msg) {\n"
          code ++= s"    int count = msg.${countMethodName}();\n"
          code ++= s"    int offset = writer.cursor();\n"
          code ++= s"    UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 8);\n"
          code ++= s"    arrayWriter.initialize(count);\n"
          code ++= s"    for (int i = 0; i < count; i++) {\n"
          code ++= s"      arrayWriter.write(i, msg.${indexGetterName}(i));\n"
          code ++= s"    }\n"
          code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
          code ++= s"  }\n\n"

        case FieldDescriptor.JavaType.FLOAT if fd.isRepeated =>
          // Generate method for repeated float field
          code ++= s"  private void write${accessor}Field(UnsafeRowWriter writer, ${messageClass.getName} msg) {\n"
          code ++= s"    int count = msg.${countMethodName}();\n"
          code ++= s"    int offset = writer.cursor();\n"
          code ++= s"    UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 4);\n"
          code ++= s"    arrayWriter.initialize(count);\n"
          code ++= s"    for (int i = 0; i < count; i++) {\n"
          code ++= s"      arrayWriter.write(i, msg.${indexGetterName}(i));\n"
          code ++= s"    }\n"
          code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
          code ++= s"  }\n\n"

        case FieldDescriptor.JavaType.DOUBLE if fd.isRepeated =>
          // Generate method for repeated double field
          code ++= s"  private void write${accessor}Field(UnsafeRowWriter writer, ${messageClass.getName} msg) {\n"
          code ++= s"    int count = msg.${countMethodName}();\n"
          code ++= s"    int offset = writer.cursor();\n"
          code ++= s"    UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 8);\n"
          code ++= s"    arrayWriter.initialize(count);\n"
          code ++= s"    for (int i = 0; i < count; i++) {\n"
          code ++= s"      arrayWriter.write(i, msg.${indexGetterName}(i));\n"
          code ++= s"    }\n"
          code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
          code ++= s"  }\n\n"

        case FieldDescriptor.JavaType.BOOLEAN if fd.isRepeated =>
          // Generate method for repeated boolean field
          code ++= s"  private void write${accessor}Field(UnsafeRowWriter writer, ${messageClass.getName} msg) {\n"
          code ++= s"    int count = msg.${countMethodName}();\n"
          code ++= s"    int offset = writer.cursor();\n"
          code ++= s"    UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 1);\n"
          code ++= s"    arrayWriter.initialize(count);\n"
          code ++= s"    for (int i = 0; i < count; i++) {\n"
          code ++= s"      arrayWriter.write(i, msg.${indexGetterName}(i));\n"
          code ++= s"    }\n"
          code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
          code ++= s"  }\n\n"

        case FieldDescriptor.JavaType.ENUM if fd.isRepeated =>
          // Generate method for repeated enum field
          code ++= s"  private void write${accessor}Field(UnsafeRowWriter writer, ${messageClass.getName} msg) {\n"
          code ++= s"    int count = msg.${countMethodName}();\n"
          code ++= s"    int offset = writer.cursor();\n"
          code ++= s"    UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 8);\n"
          code ++= s"    arrayWriter.initialize(count);\n"
          code ++= s"    for (int i = 0; i < count; i++) {\n"
          code ++= s"      " + classOf[ProtocolMessageEnum].getName + s" e = msg.${indexGetterName}(i);\n"
          code ++= s"      if (e == null) { arrayWriter.setNull(i); } else { arrayWriter.write(i, UTF8String.fromString(e.toString())); }\n"
          code ++= s"    }\n"
          code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
          code ++= s"  }\n\n"

        case FieldDescriptor.JavaType.MESSAGE if fd.isRepeated && fd.getMessageType.getOptions.hasMapEntry =>
          // Generate method for map field - extract key/value types dynamically
          val mapFields = fd.getMessageType.getFields.asScala.toList
          val keyField = mapFields.find(_.getName == "key").get
          val valueField = mapFields.find(_.getName == "value").get
          val keyJavaType = getJavaTypeString(keyField)
          val valueJavaType = getJavaTypeString(valueField)

          code ++= s"  private void write${accessor}Field(UnsafeRowWriter writer, ${messageClass.getName} msg) {\n"
          code ++= s"    java.util.Map mapField = msg.get${accessor}Map();\n"
          code ++= s"    int count = mapField.size();\n"
          code ++= s"    int offset = writer.cursor();\n"
          code ++= s"    UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 8);\n"
          code ++= s"    arrayWriter.initialize(count);\n"
          code ++= s"    int entryIndex = 0;\n"
          code ++= s"    for (java.util.Map.Entry entry : (java.util.Set<java.util.Map.Entry>) mapField.entrySet()) {\n"
          code ++= s"      int elemOffset = arrayWriter.cursor();\n"
          code ++= s"      UnsafeRowWriter structWriter = new UnsafeRowWriter(arrayWriter, 2);\n"
          code ++= s"      structWriter.resetRowWriter();\n"
          code ++= s"      " + classOf[UnsafeRowWriterHelper].getName + ".setAllFieldsNull(structWriter);\n"
          code ++= s"      // Write key (field 0)\n"
          code ++= s"      ${keyJavaType} key = (${keyJavaType}) entry.getKey();\n"
          code ++= s"      ${generateFieldWriteCode(keyField, "key", "structWriter", 0)}\n"
          code ++= s"      // Write value (field 1)\n"
          code ++= s"      ${valueJavaType} value = (${valueJavaType}) entry.getValue();\n"
          code ++= s"      ${generateFieldWriteCode(valueField, "value", "structWriter", 1)}\n"
          code ++= s"      arrayWriter.setOffsetAndSizeFromPreviousCursor(entryIndex, elemOffset);\n"
          code ++= s"      entryIndex++;\n"
          code ++= s"    }\n"
          code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
          code ++= s"  }\n\n"

        case FieldDescriptor.JavaType.MESSAGE if fd.isRepeated =>
          // Generate method for repeated message field
          val parserIndex = fieldToParserIndex(fd)
          val nestedParserName = s"nestedConv${parserIndex}"
          code ++= s"  private void write${accessor}Field(UnsafeRowWriter writer, ${messageClass.getName} msg) {\n"
          code ++= s"    int count = msg.${countMethodName}();\n"
          code ++= s"    int offset = writer.cursor();\n"
          code ++= s"    UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 8);\n"
          code ++= s"    arrayWriter.initialize(count);\n"
          // Lift writer acquisition outside loop for O(1) allocations instead of O(n)
          code ++= s"    UnsafeRowWriter nestedWriter = null;\n"
          code ++= s"    if (${nestedParserName} != null) {\n"
          code ++= s"      nestedWriter = ${nestedParserName}.acquireNestedWriter(writer);\n"
          code ++= s"    }\n"
          code ++= s"    for (int i = 0; i < count; i++) {\n"
          code ++= s"      " + classOf[Message].getName + s" element = (" + classOf[Message].getName + s") msg.${indexGetterName}(i);\n"
          code ++= s"      if (element == null || nestedWriter == null) {\n"
          code ++= s"        arrayWriter.setNull(i);\n"
          code ++= s"      } else {\n"
          code ++= s"        int elemOffset = arrayWriter.cursor();\n"
          code ++= s"        nestedWriter.resetRowWriter();\n"
          code ++= s"        " + classOf[UnsafeRowWriterHelper].getName + ".setAllFieldsNull(nestedWriter);\n"
          code ++= s"        ${nestedParserName}.parseInto(element, nestedWriter);\n"
          code ++= s"        arrayWriter.setOffsetAndSizeFromPreviousCursor(i, elemOffset);\n"
          code ++= s"      }\n"
          code ++= s"    }\n"
          code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
          code ++= s"  }\n\n"

        case FieldDescriptor.JavaType.MESSAGE if !fd.isRepeated =>
          // Generate method for singular message field
          val parserIndex = fieldToParserIndex(fd)
          val nestedParserName = s"nestedConv${parserIndex}"
          val getterName = indexGetterName
          val hasMethodName = if (!(fd.getType == FieldDescriptor.Type.MESSAGE && fd.getMessageType.getOptions.hasMapEntry)) {
            Some(s"has${accessor}")
          } else {
            None
          }
          code ++= s"  private void write${accessor}Field(UnsafeRowWriter writer, ${messageClass.getName} msg) {\n"
          hasMethodName match {
            case Some(method) =>
              code ++= s"    if (!msg.${method}()) {\n"
              code ++= s"      writer.setNullAt($idx);\n"
              code ++= s"    } else {\n"
              code ++= s"      " + classOf[Message].getName + s" v = (" + classOf[Message].getName + s") msg.${getterName}();\n"
              code ++= s"      if (${nestedParserName} == null) {\n"
              code ++= s"        writer.setNullAt($idx);\n"
              code ++= s"      } else {\n"
              code ++= s"        int offset = writer.cursor();\n"
              // Inline parseWithSharedBuffer for potential optimizations
              code ++= s"        UnsafeRowWriter nestedWriter = ${nestedParserName}.acquireWriter(writer);\n"
              code ++= s"        ${nestedParserName}.parseInto(v, nestedWriter);\n"
              code ++= s"        writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
              code ++= s"        UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
              code ++= s"      }\n"
              code ++= s"    }\n"
            case None =>
              code ++= s"    " + classOf[Message].getName + s" v = (" + classOf[Message].getName + s") msg.${getterName}();\n"
              code ++= s"    if (v == null || ${nestedParserName} == null) {\n"
              code ++= s"      writer.setNullAt($idx);\n"
              code ++= s"    } else {\n"
              code ++= s"      int offset = writer.cursor();\n"
              // Inline parseWithSharedBuffer for potential optimizations
              code ++= s"      UnsafeRowWriter nestedWriter = ${nestedParserName}.acquireWriter(writer);\n"
              code ++= s"      ${nestedParserName}.parseInto(v, nestedWriter);\n"
              code ++= s"      writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
              code ++= s"      UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
              code ++= s"    }\n"
          }
          code ++= s"  }\n\n"

        case _ => // No method generated for simple singular fields
      }
    }

    // Override parseInto to implement binary conversion with inlined parseFrom
    code ++= "  @Override\n"
    code ++= "  protected void parseInto(byte[] binary, UnsafeRowWriter writer) {\n"
    code ++= "    try {\n"
    code ++= "      // Direct parseFrom call - no reflection needed\n"
    code ++= s"      ${messageClass.getName} message = ${messageClass.getName}.parseFrom(binary);\n"
    code ++= "      parseInto(message, writer);\n"
    code ++= "    } catch (Exception e) {\n"
    code ++= "      throw new RuntimeException(\"Failed to parse protobuf binary\", e);\n"
    code ++= "    }\n"
    code ++= "  }\n"
    code ++= "\n"

    // Typed parseInto implementation (called by bridge method)
    code ++= "  private void parseInto(" + messageClass.getName + " msg, UnsafeRowWriter writer) {\n"

    // Generate per‑field extraction and writing logic using writer
    descriptor.getFields.asScala.zipWithIndex.foreach { case (fd, idx) =>
      // Insert a comment into the generated Java code to aid debugging.  This
      // comment identifies the Protobuf field being processed along with
      // whether it is repeated.  Because Janino reports compilation errors
      // relative to the generated source, adding field names to the
      // generated code makes it easier to trace errors back to the original
      // descriptor.
      code ++= s"    // Field '${fd.getName}', JavaType=${fd.getJavaType}, repeated=${fd.isRepeated}\n"
      val accessor = accessorName(fd)
      val getBytesMethodName = s"get${accessor}Bytes"
      val getterName = s"get${accessor}"
      fd.getJavaType match {
        // For complex fields (repeated and nested messages), call the generated methods
        case FieldDescriptor.JavaType.STRING if fd.isRepeated =>
          code ++= s"    write${accessor}Field(writer, msg);\n"
        case FieldDescriptor.JavaType.BYTE_STRING if fd.isRepeated =>
          code ++= s"    write${accessor}Field(writer, msg);\n"
        case FieldDescriptor.JavaType.INT if fd.isRepeated =>
          code ++= s"    write${accessor}Field(writer, msg);\n"
        case FieldDescriptor.JavaType.LONG if fd.isRepeated =>
          code ++= s"    write${accessor}Field(writer, msg);\n"
        case FieldDescriptor.JavaType.FLOAT if fd.isRepeated =>
          code ++= s"    write${accessor}Field(writer, msg);\n"
        case FieldDescriptor.JavaType.DOUBLE if fd.isRepeated =>
          code ++= s"    write${accessor}Field(writer, msg);\n"
        case FieldDescriptor.JavaType.BOOLEAN if fd.isRepeated =>
          code ++= s"    write${accessor}Field(writer, msg);\n"
        case FieldDescriptor.JavaType.ENUM if fd.isRepeated =>
          code ++= s"    write${accessor}Field(writer, msg);\n"
        case FieldDescriptor.JavaType.MESSAGE if fd.isRepeated && fd.getMessageType.getOptions.hasMapEntry =>
          code ++= s"    write${accessor}Field(writer, msg);\n"
        case FieldDescriptor.JavaType.MESSAGE if fd.isRepeated =>
          code ++= s"    write${accessor}Field(writer, msg);\n"
        case FieldDescriptor.JavaType.MESSAGE if !fd.isRepeated =>
          code ++= s"    write${accessor}Field(writer, msg);\n"

        // For simple singular fields, keep inline with optimized string handling
        case FieldDescriptor.JavaType.INT =>
          code ++= s"    writer.write($idx, msg.${getterName}());\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
        case FieldDescriptor.JavaType.LONG =>
          code ++= s"    writer.write($idx, msg.${getterName}());\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
        case FieldDescriptor.JavaType.FLOAT =>
          code ++= s"    writer.write($idx, msg.${getterName}());\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
        case FieldDescriptor.JavaType.DOUBLE =>
          code ++= s"    writer.write($idx, msg.${getterName}());\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
        case FieldDescriptor.JavaType.BOOLEAN =>
          code ++= s"    writer.write($idx, msg.${getterName}());\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
        case FieldDescriptor.JavaType.STRING =>
          // Optimized singular string: use direct bytes
          code ++= s"    byte[] bytes${idx} = msg.${getBytesMethodName}().toByteArray();\n"
          code ++= s"    writer.write($idx, bytes${idx});\n"
          code ++= s"    UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
        case FieldDescriptor.JavaType.BYTE_STRING =>
          // Singular ByteString: already optimized in original code
          code ++= s"    " + classOf[ByteString].getName + s" b${idx} = msg.${getterName}();\n"
          code ++= s"    if (b${idx} == null) {\n"
          code ++= s"      writer.setNullAt($idx);\n"
          code ++= s"    } else {\n"
          code ++= s"      writer.write($idx, b${idx}.toByteArray());\n"
          code ++= s"      UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
          code ++= s"    }\n"
        case FieldDescriptor.JavaType.ENUM =>
          // Singular enum: convert to string (keep UTF8String for now, could be optimized later)
          code ++= s"    " + classOf[ProtocolMessageEnum].getName + s" e${idx} = msg.${getterName}();\n"
          code ++= s"    if (e${idx} == null) {\n"
          code ++= s"      writer.setNullAt($idx);\n"
          code ++= s"    } else {\n"
          code ++= s"      writer.write($idx, UTF8String.fromString(e${idx}.toString()));\n"
          code ++= s"      UnsafeRowWriterHelper.clearNullAt(writer, $idx);\n"
          code ++= s"    }\n"
      }
    }

    code ++= "  }\n" // End of parseInto method

    // Bridge method for type erasure compatibility (implements abstract method)
    code ++= "  public void parseInto(Object msg, UnsafeRowWriter writer) {\n"
    code ++= s"    parseInto((${messageClass.getName}) msg, writer);\n"
    code ++= "  }\n"

    code ++= "}\n" // End of class

    code
  }

  /**
   * Get or compile parser class using global class cache.
   */
  private def getOrCompileClass(sourceCode: StringBuilder, className: String, key: String): Class[_ <: Parser] = {
    // Check global class cache first
    Option(classCache.get(key)) match {
      case Some(clazz) => clazz
      case None =>
        // Compile the generated Java code using Janino
        val compiler = new SimpleCompiler()
        compiler.setParentClassLoader(this.getClass.getClassLoader)
        compiler.cook(sourceCode.toString)
        val generatedClass = compiler.getClassLoader.loadClass(className).asInstanceOf[Class[_ <: Parser]]

        // Cache the compiled class globally and return
        Option(classCache.putIfAbsent(key, generatedClass)).getOrElse(generatedClass)
    }
  }

  /**
   * Compile the generated Java source code using Janino and instantiate the parser.
   *
   * @param sourceCode the complete Java source code
   * @param className  the name of the generated class
   * @param schema     the Spark SQL schema for this message
   * @return a compiled and instantiated Parser
   */
  private def compileAndInstantiate(sourceCode: StringBuilder, className: String, schema: StructType): Parser = {
    // Get or compile the class globally
    val key = s"${className}_${schema.hashCode}"
    val parserClass = getOrCompileClass(sourceCode, className, key)

    // Create parser with schema - dependencies will be set via setters
    val constructor = parserClass.getConstructor(classOf[StructType])
    constructor.newInstance(schema).asInstanceOf[Parser]
  }

  /**
   * Get the Java type string for a protobuf field descriptor.
   */
  private def getJavaTypeString(fd: FieldDescriptor): String = {
    fd.getJavaType match {
      case FieldDescriptor.JavaType.INT => "Integer"
      case FieldDescriptor.JavaType.LONG => "Long"
      case FieldDescriptor.JavaType.FLOAT => "Float"
      case FieldDescriptor.JavaType.DOUBLE => "Double"
      case FieldDescriptor.JavaType.BOOLEAN => "Boolean"
      case FieldDescriptor.JavaType.STRING => "String"
      case FieldDescriptor.JavaType.BYTE_STRING => classOf[ByteString].getName
      case FieldDescriptor.JavaType.ENUM => fd.getEnumType.getName
      case FieldDescriptor.JavaType.MESSAGE => fd.getMessageType.getName
    }
  }

  /**
   * Generate code to write a field value to an UnsafeRowWriter.
   */
  private def generateFieldWriteCode(fd: FieldDescriptor, varName: String, writerName: String, fieldIndex: Int): String = {
    fd.getJavaType match {
      case FieldDescriptor.JavaType.INT =>
        s"if ($varName == null) { $writerName.setNullAt($fieldIndex); } else { $writerName.write($fieldIndex, $varName.intValue()); }"
      case FieldDescriptor.JavaType.LONG =>
        s"if ($varName == null) { $writerName.setNullAt($fieldIndex); } else { $writerName.write($fieldIndex, $varName.longValue()); }"
      case FieldDescriptor.JavaType.FLOAT =>
        s"if ($varName == null) { $writerName.setNullAt($fieldIndex); } else { $writerName.write($fieldIndex, $varName.floatValue()); }"
      case FieldDescriptor.JavaType.DOUBLE =>
        s"if ($varName == null) { $writerName.setNullAt($fieldIndex); } else { $writerName.write($fieldIndex, $varName.doubleValue()); }"
      case FieldDescriptor.JavaType.BOOLEAN =>
        s"if ($varName == null) { $writerName.setNullAt($fieldIndex); } else { $writerName.write($fieldIndex, $varName.booleanValue()); }"
      case FieldDescriptor.JavaType.STRING =>
        s"if ($varName == null) { $writerName.setNullAt($fieldIndex); } else { $writerName.write($fieldIndex, UTF8String.fromString($varName)); }"
      case FieldDescriptor.JavaType.BYTE_STRING =>
        s"if ($varName == null) { $writerName.setNullAt($fieldIndex); } else { $writerName.write($fieldIndex, $varName.toByteArray()); }"
      case FieldDescriptor.JavaType.ENUM =>
        s"if ($varName == null) { $writerName.setNullAt($fieldIndex); } else { $writerName.write($fieldIndex, UTF8String.fromString($varName.toString())); }"
      case FieldDescriptor.JavaType.MESSAGE =>
        // For map nested messages, this shouldn't be called, but provide a fallback
        s"$writerName.setNullAt($fieldIndex); // Nested message not supported in map values"
    }
  }
}