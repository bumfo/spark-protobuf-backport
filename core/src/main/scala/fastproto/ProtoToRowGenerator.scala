package fastproto

// Use JavaConverters for Scala 2.12 compatibility

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.{ByteString, Message, ProtocolMessageEnum}
import org.apache.spark.sql.types._
import org.codehaus.janino.SimpleCompiler

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Factory object for generating [[RowConverter]] instances on the fly.  Given a
 * Protobuf [[Descriptor]] and the corresponding compiled message class, this
 * object synthesises a small Java class that extracts each field from the
 * message and writes it into a new array.  The generated class implements
 * [[RowConverter]] for the provided message type.  Janino is used to
 * compile the generated Java code at runtime.  Only a subset of types is
 * currently supported: primitive numeric types, booleans, strings, byte
 * strings and enums.  Nested messages and repeated fields are emitted as
 * `null` placeholders and can be extended with recursive conversion logic.
 */
object ProtoToRowGenerator {

  // Thread-safe cache for generated converters to avoid redundant compilation
  private val converterCache: ConcurrentHashMap[String, RowConverter] =
    new ConcurrentHashMap()

  // Thread-safe cache for computed schemas to avoid redundant schema generation
  private val schemaCache: ConcurrentHashMap[String, StructType] =
    new ConcurrentHashMap()

  /**
   * Compute the accessor method suffix for a given field descriptor.  This
   * converts snake_case names into CamelCase, capitalising each segment.
   * For example, a field named "source_context" yields "SourceContext".
   * This helper is defined at the object level so it is visible to both
   * schema generation and nested converter resolution.
   */
  private def accessorName(fd: FieldDescriptor): String = {
    val name = fd.getName
    name.split("_").iterator.map { part =>
      if (part.isEmpty) "" else part.substring(0, 1).toUpperCase + part.substring(1)
    }.mkString("")
  }

  /**
   * Get a cached schema for the given descriptor, computing it if necessary.
   * This avoids redundant schema generation for the same descriptors.
   *
   * @param descriptor the Protobuf descriptor
   * @return a cached [[StructType]] representing the schema
   */
  private def getCachedSchema(descriptor: Descriptor): StructType = {
    val key = descriptor.getFullName
    schemaCache.computeIfAbsent(key, _ => buildStructTypeInternal(descriptor))
  }

  /**
   * Recursively build a Spark SQL [[StructType]] corresponding to the
   * structure of a Protobuf message.  Primitive fields are mapped to
   * appropriate Catalyst types; repeated fields become [[ArrayType]] and
   * Protobuf map entries become [[MapType]].  Nested message types are
   * converted into nested [[StructType]]s.
   *
   * This is the internal implementation that should be called through
   * getCachedSchema for better performance.
   *
   * @param descriptor the root Protobuf descriptor
   * @return a [[StructType]] representing the schema
   */
  private def buildStructTypeInternal(descriptor: Descriptor): StructType = {
    val fields = descriptor.getFields.asScala.map { fd =>
      val dt = fieldToDataType(fd)
      // Proto3 fields are optional by default; mark field nullable unless explicitly required
      val nullable = !fd.isRequired
      StructField(fd.getName, dt, nullable)
    }
    StructType(fields.toArray)
  }

  /**
   * Convert a Protobuf field descriptor into a Spark SQL [[DataType]].
   * Nested messages are handled recursively.  Repeated fields become
   * [[ArrayType]] and Protobuf map entry types are translated into
   * [[MapType]].  Primitive wrapper types (e.g. IntValue) are treated
   * according to their contained primitive.
   */
  private def fieldToDataType(fd: FieldDescriptor): DataType = {
    import FieldDescriptor.JavaType._
    // Handle Protobuf map entries: repeated message types with mapEntry option.  In this
    // implementation we do not emit a Spark MapType because writing MapData into
    // an UnsafeRow requires more complex handling.  Instead, treat map entries
    // as an array of structs with two fields (key and value).  This approach
    // still captures all information from the map and avoids the need to build
    // MapData at runtime.
    if (fd.isRepeated && fd.getType == FieldDescriptor.Type.MESSAGE && fd.getMessageType.getOptions.hasMapEntry) {
      // Build a StructType for the map entry (with key and value fields) and wrap it in an ArrayType.
      val entryType = getCachedSchema(fd.getMessageType)
      ArrayType(entryType, containsNull = false)
    } else if (fd.isRepeated) {
      // Repeated (array) field
      val elementType = fd.getJavaType match {
        case INT => IntegerType
        case LONG => LongType
        case FLOAT => FloatType
        case DOUBLE => DoubleType
        case BOOLEAN => BooleanType
        case STRING => StringType
        case BYTE_STRING => BinaryType
        case ENUM => StringType
        case MESSAGE => getCachedSchema(fd.getMessageType)
      }
      ArrayType(elementType, containsNull = false)
    } else {
      fd.getJavaType match {
        case INT => IntegerType
        case LONG => LongType
        case FLOAT => FloatType
        case DOUBLE => DoubleType
        case BOOLEAN => BooleanType
        case STRING => StringType
        case BYTE_STRING => BinaryType
        case ENUM => StringType
        case MESSAGE => getCachedSchema(fd.getMessageType)
      }
    }
  }

  /**
   * Generate a concrete [[MessageBasedConverter]] for the given Protobuf message type.
   * Uses internal caching to avoid redundant Janino compilation for the same 
   * descriptor and message class combinations. Handles recursive types by
   * creating converter graphs locally before updating the global cache.
   *
   * @param descriptor   the Protobuf descriptor describing the message schema
   * @param messageClass the compiled Protobuf Java class
   * @tparam T the concrete type of the message
   * @return a [[MessageBasedConverter]] capable of converting the message into an
   *         [[org.apache.spark.sql.catalyst.InternalRow]]
   */
  def generateConverter[T <: Message](
      descriptor: Descriptor,
      messageClass: Class[T]): MessageBasedConverter[T] = {
    val key = s"${messageClass.getName}_${descriptor.getFullName}"

    // Check global cache first
    converterCache.get(key) match {
      case converter if converter != null => return converter.asInstanceOf[MessageBasedConverter[T]]
      case _ => // Continue with generation
    }

    // Local map to track converters being built (excludes already cached ones)
    // Map key -> (converter, neededTypeKeys)
    val localConverters = scala.collection.mutable.Map[String, (RowConverter, Set[String])]()

    // Phase 1: Create converter graph with null dependencies
    val rootConverter = createConverterGraph(descriptor, messageClass, localConverters)

    // Phase 2: Wire up dependencies
    wireDependencies(localConverters)

    // Phase 3: Atomically update global cache
    localConverters.foreach { case (k, (conv, _)) =>
      converterCache.putIfAbsent(k, conv)
    }

    rootConverter.asInstanceOf[MessageBasedConverter[T]]
  }

  /**
   * Create a converter graph for the given descriptor and class, recursively
   * building converters for nested types. Returns existing converters from
   * global cache when available to avoid duplication.
   */
  private def createConverterGraph(
      descriptor: Descriptor,
      messageClass: Class[_ <: Message],
      localConverters: scala.collection.mutable.Map[String, (RowConverter, Set[String])]): RowConverter = {
    val key = s"${messageClass.getName}_${descriptor.getFullName}"

    // Check global cache FIRST (important optimization!)
    converterCache.get(key) match {
      case converter if converter != null => return converter
      case _ => // Not cached globally
    }

    // Check local map
    if (localConverters.contains(key)) return localConverters(key)._1

    // Collect all nested message fields (per-field approach for now)
    val nestedFields = descriptor.getFields.asScala.filter(_.getJavaType == FieldDescriptor.JavaType.MESSAGE).toList
    val nestedTypes: Map[String, (Descriptor, Class[_ <: Message])] = nestedFields.map { fd =>
      val nestedClass = getNestedMessageClass(fd, messageClass)
      val typeKey = s"${nestedClass.getName}_${fd.getMessageType.getFullName}"
      typeKey -> (fd.getMessageType, nestedClass)
    }.toMap

    // Create converter with null dependencies initially
    val converter = generateConverterWithNullDeps(descriptor, messageClass, nestedTypes.size)
    val neededTypeKeys = nestedTypes.map { case (typeKey, (desc, clazz)) =>
      s"${clazz.getName}_${desc.getFullName}"
    }.toSet
    localConverters(key) = (converter, neededTypeKeys)

    // Recursively create converters for unique nested types
    nestedTypes.values.foreach { case (desc, clazz) =>
      createConverterGraph(desc, clazz, localConverters)
    }

    converter
  }

  /**
   * Wire dependencies between converters after all have been created.
   */
  private def wireDependencies(localConverters: scala.collection.mutable.Map[String, (RowConverter, Set[String])]): Unit = {
    localConverters.foreach { case (converterKey, (converter, neededTypes)) =>
      // Set dependencies on this converter
      val dependencies = neededTypes.toSeq.map { typeKey =>
        localConverters.get(typeKey) match {
          case Some((nestedConverter, _)) => nestedConverter
          case None => converterCache.get(typeKey)
        }
      }
      setConverterDependencies(converter, dependencies)
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
   * Set dependencies on a converter using reflection (temporary - will be replaced
   * by proper generated setter methods).
   */
  private def setConverterDependencies(converter: RowConverter, dependencies: Seq[RowConverter]): Unit = {
    // For now, use reflection to set dependencies
    // This will be replaced by proper generated setter methods
    val converterClass = converter.getClass
    dependencies.zipWithIndex.foreach { case (dep, idx) =>
      try {
        val setterMethod = converterClass.getMethod(s"setNestedConverter${idx}", classOf[MessageBasedConverter[_]])
        setterMethod.invoke(converter, dep)
      } catch {
        case _: NoSuchMethodException => // Converter has no dependencies, ignore
      }
    }
  }


  /**
   * Internal method that performs the actual converter generation and compilation
   * with null dependencies initially. This allows recursive types to be handled
   * by deferring dependency injection until after all converters are created.
   *
   * @param descriptor     the Protobuf descriptor describing the message schema
   * @param messageClass   the compiled Protobuf Java class
   * @param numNestedTypes the number of unique nested message types
   * @return a [[RowConverter]] capable of converting the message into an
   *         [[org.apache.spark.sql.catalyst.InternalRow]]
   */
  private def generateConverterWithNullDeps(
      descriptor: Descriptor,
      messageClass: Class[_ <: Message],
      numNestedTypes: Int): RowConverter = {
    // Create a unique class name to avoid collisions when multiple converters are generated
    val className = s"GeneratedConverter_${descriptor.getName}_${System.nanoTime()}"

    val sourceCode = generateConverterSourceCode(className, descriptor, messageClass, numNestedTypes)
    compileAndInstantiate(sourceCode, className, descriptor)
  }

  /**
   * Generate Java source code for a MessageBasedConverter implementation.
   * This method creates the complete Java class source code including imports,
   * class declaration, fields, constructor, and conversion methods.
   *
   * @param className the name of the generated Java class
   * @param descriptor the Protobuf descriptor for the message
   * @param messageClass the compiled Java class for the message
   * @param numNestedTypes the number of nested message types
   * @return StringBuilder containing the complete Java source code
   */
  def generateConverterSourceCode(
      className: String,
      descriptor: Descriptor,
      messageClass: Class[_ <: Message],
      numNestedTypes: Int): StringBuilder = {
    // Create per-field converter indices for message fields
    val messageFields = descriptor.getFields.asScala.filter(_.getJavaType == FieldDescriptor.JavaType.MESSAGE).toList

    // Verify we have the expected number of nested types
    assert(messageFields.size == numNestedTypes,
      s"Expected $numNestedTypes nested types but found ${messageFields.size}")

    // Create field-to-converter-index mapping for code generation
    val fieldToConverterIndex: Map[FieldDescriptor, Int] = messageFields.zipWithIndex.toMap

    val code = new StringBuilder
    // Imports required by the generated Java source
    code ++= "import org.apache.spark.sql.catalyst.expressions.UnsafeRow;\n"
    code ++= "import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;\n"
    code ++= "import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;\n"
    code ++= "import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter;\n"
    code ++= "import org.apache.spark.sql.catalyst.InternalRow;\n"
    code ++= "import org.apache.spark.sql.types.StructType;\n"
    code ++= "import org.apache.spark.unsafe.types.UTF8String;\n"
    code ++= "import fastproto.AbstractMessageBasedConverter;\n"

    // Begin class declaration
    code ++= s"public final class ${className} extends fastproto.AbstractMessageBasedConverter<${messageClass.getName}> {\n"

    // Declare fields: nested converters (non-final for setter injection)
    (0 until numNestedTypes).foreach { idx =>
      code ++= s"  private fastproto.MessageBasedConverter nestedConv${idx}; // Non-final for dependency injection\n"
    }

    // Constructor with null nested converters initially
    code ++= s"  public ${className}(StructType schema) {\n"
    code ++= "    super(schema);\n"
    (0 until numNestedTypes).foreach { idx =>
      code ++= s"    this.nestedConv${idx} = null; // Will be set via setter\n"
    }
    code ++= "  }\n"

    // Generate setter methods for dependency injection
    (0 until numNestedTypes).foreach { idx =>
      code ++= s"  public void setNestedConverter${idx}(fastproto.MessageBasedConverter conv) {\n"
      code ++= "    if (this.nestedConv" + idx + " != null) throw new IllegalStateException(\"Converter " + idx + " already set\");\n"
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
          code ++= s"      com.google.protobuf.ByteString bs = msg.${getBytesMethodName}(i);\n"
          code ++= s"      byte[] bytes = bs.toByteArray();\n"
          code ++= s"      arrayWriter.write(i, bytes);\n"
          code ++= s"    }\n"
          code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
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
          code ++= s"  }\n\n"

        case FieldDescriptor.JavaType.MESSAGE if fd.isRepeated =>
          // Generate method for repeated message field
          val converterIndex = fieldToConverterIndex(fd)
          val nestedConverterName = s"nestedConv${converterIndex}"
          code ++= s"  private void write${accessor}Field(UnsafeRowWriter writer, ${messageClass.getName} msg) {\n"
          code ++= s"    int count = msg.${countMethodName}();\n"
          code ++= s"    int offset = writer.cursor();\n"
          code ++= s"    UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 8);\n"
          code ++= s"    arrayWriter.initialize(count);\n"
          code ++= s"    for (int i = 0; i < count; i++) {\n"
          code ++= s"      " + classOf[Message].getName + s" element = (" + classOf[Message].getName + s") msg.${indexGetterName}(i);\n"
          code ++= s"      if (element == null || ${nestedConverterName} == null) {\n"
          code ++= s"        arrayWriter.setNull(i);\n"
          code ++= s"      } else {\n"
          code ++= s"        int elemOffset = arrayWriter.cursor();\n"
          code ++= s"        ${nestedConverterName}.convert(element, writer);\n"
          code ++= s"        arrayWriter.setOffsetAndSizeFromPreviousCursor(i, elemOffset);\n"
          code ++= s"      }\n"
          code ++= s"    }\n"
          code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
          code ++= s"  }\n\n"

        case FieldDescriptor.JavaType.MESSAGE if !fd.isRepeated =>
          // Generate method for singular message field
          val converterIndex = fieldToConverterIndex(fd)
          val nestedConverterName = s"nestedConv${converterIndex}"
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
              code ++= s"      if (${nestedConverterName} == null) {\n"
              code ++= s"        writer.setNullAt($idx);\n"
              code ++= s"      } else {\n"
              code ++= s"        int offset = writer.cursor();\n"
              code ++= s"        ${nestedConverterName}.convert(v, writer);\n"
              code ++= s"        writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
              code ++= s"      }\n"
              code ++= s"    }\n"
            case None =>
              code ++= s"    " + classOf[Message].getName + s" v = (" + classOf[Message].getName + s") msg.${getterName}();\n"
              code ++= s"    if (v == null || ${nestedConverterName} == null) {\n"
              code ++= s"      writer.setNullAt($idx);\n"
              code ++= s"    } else {\n"
              code ++= s"      int offset = writer.cursor();\n"
              code ++= s"      ${nestedConverterName}.convert(v, writer);\n"
              code ++= s"      writer.setOffsetAndSizeFromPreviousCursor($idx, offset);\n"
              code ++= s"    }\n"
          }
          code ++= s"  }\n\n"

        case _ => // No method generated for simple singular fields
      }
    }

    // Override writeData to implement binary conversion with inlined parseFrom
    code ++= "  @Override\n"
    code ++= "  protected void writeData(byte[] binary, UnsafeRowWriter writer) {\n"
    code ++= "    try {\n"
    code ++= "      // Direct parseFrom call - no reflection needed\n"
    code ++= s"      ${messageClass.getName} message = ${messageClass.getName}.parseFrom(binary);\n"
    code ++= "      writeMessage(message, writer);\n"
    code ++= "    } catch (Exception e) {\n"
    code ++= "      throw new RuntimeException(\"Failed to parse protobuf binary\", e);\n"
    code ++= "    }\n"
    code ++= "  }\n"
    code ++= "\n"

    // Typed writeMessage implementation (called by bridge method)
    code ++= "  private void writeMessage(" + messageClass.getName + " msg, UnsafeRowWriter writer) {\n"

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
        case FieldDescriptor.JavaType.MESSAGE if fd.isRepeated =>
          code ++= s"    write${accessor}Field(writer, msg);\n"
        case FieldDescriptor.JavaType.MESSAGE if !fd.isRepeated =>
          code ++= s"    write${accessor}Field(writer, msg);\n"

        // For simple singular fields, keep inline with optimized string handling
        case FieldDescriptor.JavaType.INT =>
          code ++= s"    writer.write($idx, msg.${getterName}());\n"
        case FieldDescriptor.JavaType.LONG =>
          code ++= s"    writer.write($idx, msg.${getterName}());\n"
        case FieldDescriptor.JavaType.FLOAT =>
          code ++= s"    writer.write($idx, msg.${getterName}());\n"
        case FieldDescriptor.JavaType.DOUBLE =>
          code ++= s"    writer.write($idx, msg.${getterName}());\n"
        case FieldDescriptor.JavaType.BOOLEAN =>
          code ++= s"    writer.write($idx, msg.${getterName}());\n"
        case FieldDescriptor.JavaType.STRING =>
          // Optimized singular string: use direct bytes
          code ++= s"    byte[] bytes${idx} = msg.${getBytesMethodName}().toByteArray();\n"
          code ++= s"    writer.write($idx, bytes${idx});\n"
        case FieldDescriptor.JavaType.BYTE_STRING =>
          // Singular ByteString: already optimized in original code
          code ++= s"    " + classOf[ByteString].getName + s" b${idx} = msg.${getterName}();\n"
          code ++= s"    if (b${idx} == null) {\n"
          code ++= s"      writer.setNullAt($idx);\n"
          code ++= s"    } else {\n"
          code ++= s"      writer.write($idx, b${idx}.toByteArray());\n"
          code ++= s"    }\n"
        case FieldDescriptor.JavaType.ENUM =>
          // Singular enum: convert to string (keep UTF8String for now, could be optimized later)
          code ++= s"    " + classOf[ProtocolMessageEnum].getName + s" e${idx} = msg.${getterName}();\n"
          code ++= s"    if (e${idx} == null) {\n"
          code ++= s"      writer.setNullAt($idx);\n"
          code ++= s"    } else {\n"
          code ++= s"      writer.write($idx, UTF8String.fromString(e${idx}.toString()));\n"
          code ++= s"    }\n"
      }
    }

    code ++= "  }\n" // End of writeMessage method

    // Bridge method for type erasure compatibility (implements abstract method)
    code ++= "  public void writeMessage(Object msg, UnsafeRowWriter writer) {\n"
    code ++= s"    writeMessage((${messageClass.getName}) msg, writer);\n"
    code ++= "  }\n"

    code ++= "}\n" // End of class

    code
  }

  /**
   * Compile the generated Java source code using Janino and instantiate the converter.
   *
   * @param sourceCode the complete Java source code
   * @param className the name of the generated class
   * @param descriptor the Protobuf descriptor (used for schema generation)
   * @return a compiled and instantiated RowConverter
   */
  private def compileAndInstantiate(sourceCode: StringBuilder, className: String, descriptor: Descriptor): RowConverter = {
    // Build the Spark SQL schema corresponding to this descriptor
    val schema: StructType = getCachedSchema(descriptor)

    // Compile the generated Java code using Janino
    val compiler = new SimpleCompiler()
    compiler.setParentClassLoader(this.getClass.getClassLoader)
    compiler.cook(sourceCode.toString)
    val generatedClass = compiler.getClassLoader.loadClass(className)

    // Create converter with just schema - dependencies will be set via setters
    val constructor = generatedClass.getConstructor(classOf[StructType])
    constructor.newInstance(schema).asInstanceOf[RowConverter]
  }
}