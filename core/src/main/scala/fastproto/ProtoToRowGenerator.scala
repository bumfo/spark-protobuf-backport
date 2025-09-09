package fastproto

// Use JavaConverters for Scala 2.12 compatibility

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.{ByteString, Message, ProtocolMessageEnum}
import org.apache.spark.sql.types._
import org.codehaus.janino.SimpleCompiler

import scala.collection.JavaConverters._
import java.util.concurrent.ConcurrentHashMap

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
  private val converterCache: ConcurrentHashMap[String, RowConverter[_ <: Message]] = 
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
   * Generate a concrete [[RowConverter]] for the given Protobuf message type.
   * Uses internal caching to avoid redundant Janino compilation for the same 
   * descriptor and message class combinations. Handles recursive types by
   * creating converter graphs locally before updating the global cache.
   *
   * @param descriptor   the Protobuf descriptor describing the message schema
   * @param messageClass the compiled Protobuf Java class
   * @tparam T the concrete type of the message
   * @return a [[RowConverter]] capable of converting the message into an
   *         [[org.apache.spark.sql.catalyst.InternalRow]]
   */
  def generateConverter[T <: Message](descriptor: Descriptor,
                                      messageClass: Class[T]): RowConverter[T] = {
    val key = s"${messageClass.getName}_${descriptor.getFullName}"
    
    // Check global cache first
    converterCache.get(key) match {
      case converter if converter != null => return converter.asInstanceOf[RowConverter[T]]
      case _ => // Continue with generation
    }
    
    // Local map to track converters being built (excludes already cached ones)
    // Map key -> (converter, neededTypeKeys)
    val localConverters = scala.collection.mutable.Map[String, (RowConverter[_ <: Message], Set[String])]()
    
    // Phase 1: Create converter graph with null dependencies
    val rootConverter = createConverterGraph(descriptor, messageClass, localConverters)
    
    // Phase 2: Wire up dependencies
    wireDependencies(localConverters)
    
    // Phase 3: Atomically update global cache
    localConverters.foreach { case (k, (conv, _)) => 
      converterCache.putIfAbsent(k, conv)
    }
    
    rootConverter.asInstanceOf[RowConverter[T]]
  }

  /**
   * Create a converter graph for the given descriptor and class, recursively
   * building converters for nested types. Returns existing converters from
   * global cache when available to avoid duplication.
   */
  private def createConverterGraph(descriptor: Descriptor, 
                                   messageClass: Class[_ <: Message],
                                   localConverters: scala.collection.mutable.Map[String, (RowConverter[_ <: Message], Set[String])]): RowConverter[_ <: Message] = {
    val key = s"${messageClass.getName}_${descriptor.getFullName}"
    
    // Check global cache FIRST (important optimization!)
    converterCache.get(key) match {
      case converter if converter != null => return converter.asInstanceOf[RowConverter[_ <: Message]]
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
  private def wireDependencies(localConverters: scala.collection.mutable.Map[String, (RowConverter[_ <: Message], Set[String])]): Unit = {
    localConverters.foreach { case (converterKey, (converter, neededTypes)) =>
      // Set dependencies on this converter
      val dependencies = neededTypes.toSeq.map { typeKey =>
        localConverters.get(typeKey) match {
          case Some((nestedConverter, _)) => nestedConverter
          case None => converterCache.get(typeKey).asInstanceOf[RowConverter[_ <: Message]]
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
  private def setConverterDependencies(converter: RowConverter[_ <: Message], dependencies: Seq[RowConverter[_ <: Message]]): Unit = {
    // For now, use reflection to set dependencies
    // This will be replaced by proper generated setter methods
    val converterClass = converter.getClass
    dependencies.zipWithIndex.foreach { case (dep, idx) =>
      try {
        val setterMethod = converterClass.getMethod(s"setNestedConverter${idx}", classOf[RowConverter[_]])
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
   * @param descriptor   the Protobuf descriptor describing the message schema
   * @param messageClass the compiled Protobuf Java class
   * @param numNestedTypes the number of unique nested message types
   * @return a [[RowConverter]] capable of converting the message into an
   *         [[org.apache.spark.sql.catalyst.InternalRow]]
   */
  private def generateConverterWithNullDeps(descriptor: Descriptor,
                                            messageClass: Class[_ <: Message], 
                                            numNestedTypes: Int): RowConverter[_ <: Message] = {
    // Build the Spark SQL schema corresponding to this descriptor
    val schema: StructType = getCachedSchema(descriptor)

    // Create per-field converter indices for message fields
    val messageFields = descriptor.getFields.asScala.filter(_.getJavaType == FieldDescriptor.JavaType.MESSAGE).toList
    
    // Verify we have the expected number of nested types
    assert(messageFields.size == numNestedTypes, 
      s"Expected $numNestedTypes nested types but found ${messageFields.size}")

    // Create field-to-converter-index mapping for code generation
    val fieldToConverterIndex: Map[FieldDescriptor, Int] = messageFields.zipWithIndex.toMap

    // Create a unique class name to avoid collisions when multiple converters are generated
    val className = s"GeneratedConverter_${descriptor.getName}_${System.nanoTime()}"
    val code = new StringBuilder
    // Imports required by the generated Java source
    code ++= "import org.apache.spark.sql.catalyst.expressions.UnsafeRow;\n"
    code ++= "import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;\n"
    code ++= "import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;\n"
    code ++= "import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter;\n"
    code ++= "import org.apache.spark.sql.catalyst.InternalRow;\n"
    code ++= "import org.apache.spark.sql.types.StructType;\n"
    code ++= "import org.apache.spark.unsafe.types.UTF8String;\n"
    code ++= "import fastproto.RowConverter;\n"
    
    // Begin class declaration
    code ++= s"public final class ${className} implements fastproto.RowConverter<${messageClass.getName}> {\n"
    
    // Declare fields: schema, writer, and nested converters (non-final for setter injection)
    code ++= "  private final StructType schema;\n"
    code ++= "  private final UnsafeRowWriter writer;\n"
    (0 until numNestedTypes).foreach { idx =>
      code ++= s"  private fastproto.RowConverter nestedConv${idx}; // Non-final for dependency injection\n"
    }
    
    // Constructor with null nested converters initially
    code ++= s"  public ${className}(StructType schema) {\n"
    code ++= "    this.schema = schema;\n"
    code ++= "    int numFields = schema.length();\n"
    code ++= "    this.writer = new UnsafeRowWriter(numFields);\n"
    (0 until numNestedTypes).foreach { idx =>
      code ++= s"    this.nestedConv${idx} = null; // Will be set via setter\n"
    }
    code ++= "  }\n"
    
    // Generate setter methods for dependency injection
    (0 until numNestedTypes).foreach { idx =>
      code ++= s"  public void setNestedConverter${idx}(fastproto.RowConverter conv) {\n"
      code ++= "    if (this.nestedConv" + idx + " != null) throw new IllegalStateException(\"Converter " + idx + " already set\");\n"
      code ++= s"    this.nestedConv${idx} = conv;\n"
      code ++= s"  }\n"
    }
    // Generate the primary convert method - delegates to two-parameter version
    code ++= "  public UnsafeRow convert(" + messageClass.getName + " msg) {\n"
    code ++= "    return convert(msg, null);\n"
    code ++= "  }\n"
    code ++= "\n"
    // Generate the convert method with parentWriter parameter for BufferHolder sharing  
    code ++= "  public UnsafeRow convert(" + messageClass.getName + " msg, UnsafeWriter parentWriter) {\n"
    code ++= "    UnsafeRowWriter writer;\n"
    code ++= "    if (parentWriter == null) {\n"
    code ++= "      // Use instance writer and reset it\n"
    code ++= "      writer = this.writer;\n"
    code ++= "      writer.reset();\n"
    code ++= "      writer.zeroOutNullBytes();\n"
    code ++= "    } else {\n"
    code ++= "      // Create new writer that shares BufferHolder with parent\n"
    code ++= "      writer = new UnsafeRowWriter(parentWriter, schema.length());\n"
    code ++= "      writer.resetRowWriter(); // Initialize null bytes but don't reset buffer\n"
    code ++= "    }\n"
    
    // Generate per‑field extraction and writing logic using writer
    descriptor.getFields.asScala.zipWithIndex.foreach { case (fd, idx) =>
      // Insert a comment into the generated Java code to aid debugging.  This
      // comment identifies the Protobuf field being processed along with
      // whether it is repeated.  Because Janino reports compilation errors
      // relative to the generated source, adding field names to the
      // generated code makes it easier to trace errors back to the original
      // descriptor.
      code ++= s"    // Field '${fd.getName}', JavaType=${fd.getJavaType}, repeated=${fd.isRepeated}\n"
      // Build accessor method names.  For repeated fields, use getXCount() and getX(index)
      val listGetterName = s"get${accessorName(fd)}List"
      val countMethodName = s"get${accessorName(fd)}Count"
      val indexGetterName = s"get${accessorName(fd)}"
      val getterName = if (fd.isRepeated) listGetterName else indexGetterName
      val hasMethodName = if (fd.getJavaType == FieldDescriptor.JavaType.MESSAGE && !fd.isRepeated && !(fd.getType == FieldDescriptor.Type.MESSAGE && fd.getMessageType.getOptions.hasMapEntry)) {
        // For singular message fields there is a hasX() method
        Some(s"has${accessorName(fd)}")
      } else {
        None
      }
      fd.getJavaType match {
        case FieldDescriptor.JavaType.INT =>
          if (fd.isRepeated) {
            // Repeated int32: write using UnsafeArrayWriter (element size = 4 bytes)
            code ++= s"    int size${idx} = msg.${countMethodName}();\n"
            code ++= s"    int offset${idx} = writer.cursor();\n"
            code ++= s"    org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter${idx} = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(writer, 4);\n"
            code ++= s"    arrayWriter${idx}.initialize(size${idx});\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) { arrayWriter${idx}.write(i, msg.${indexGetterName}(i)); }\n"
            code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset${idx});\n"
          } else {
            code ++= s"    writer.write($idx, msg.${getterName}());\n"
          }
        case FieldDescriptor.JavaType.LONG =>
          if (fd.isRepeated) {
            // Repeated int64: element size = 8 bytes
            code ++= s"    int size${idx} = msg.${countMethodName}();\n"
            code ++= s"    int offset${idx} = writer.cursor();\n"
            code ++= s"    org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter${idx} = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(writer, 8);\n"
            code ++= s"    arrayWriter${idx}.initialize(size${idx});\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) { arrayWriter${idx}.write(i, msg.${indexGetterName}(i)); }\n"
            code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset${idx});\n"
          } else {
            code ++= s"    writer.write($idx, msg.${getterName}());\n"
          }
        case FieldDescriptor.JavaType.FLOAT =>
          if (fd.isRepeated) {
            // Repeated float: element size = 4 bytes
            code ++= s"    int size${idx} = msg.${countMethodName}();\n"
            code ++= s"    int offset${idx} = writer.cursor();\n"
            code ++= s"    org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter${idx} = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(writer, 4);\n"
            code ++= s"    arrayWriter${idx}.initialize(size${idx});\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) { arrayWriter${idx}.write(i, msg.${indexGetterName}(i)); }\n"
            code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset${idx});\n"
          } else {
            code ++= s"    writer.write($idx, msg.${getterName}());\n"
          }
        case FieldDescriptor.JavaType.DOUBLE =>
          if (fd.isRepeated) {
            // Repeated double: element size = 8 bytes
            code ++= s"    int size${idx} = msg.${countMethodName}();\n"
            code ++= s"    int offset${idx} = writer.cursor();\n"
            code ++= s"    org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter${idx} = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(writer, 8);\n"
            code ++= s"    arrayWriter${idx}.initialize(size${idx});\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) { arrayWriter${idx}.write(i, msg.${indexGetterName}(i)); }\n"
            code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset${idx});\n"
          } else {
            code ++= s"    writer.write($idx, msg.${getterName}());\n"
          }
        case FieldDescriptor.JavaType.BOOLEAN =>
          if (fd.isRepeated) {
            // Repeated boolean: element size = 1 byte (boolean is stored as byte)
            code ++= s"    int size${idx} = msg.${countMethodName}();\n"
            code ++= s"    int offset${idx} = writer.cursor();\n"
            code ++= s"    org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter${idx} = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(writer, 1);\n"
            code ++= s"    arrayWriter${idx}.initialize(size${idx});\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) { arrayWriter${idx}.write(i, msg.${indexGetterName}(i)); }\n"
            code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset${idx});\n"
          } else {
            code ++= s"    writer.write($idx, msg.${getterName}());\n"
          }
        case FieldDescriptor.JavaType.STRING =>
          if (fd.isRepeated) {
            // Repeated string: need to handle variable-length data
            val elemSize = 8 // strings are variable-length; we store offset & length
            code ++= s"    int size${idx} = msg.${countMethodName}();\n"
            code ++= s"    int offset${idx} = writer.cursor();\n"
            code ++= s"    org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter${idx} = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(writer, ${elemSize});\n"
            code ++= s"    arrayWriter${idx}.initialize(size${idx});\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) {\n"
            code ++= s"      String s = msg.${indexGetterName}(i);\n"
            code ++= s"      if (s == null) { arrayWriter${idx}.setNull(i); } else { arrayWriter${idx}.write(i, UTF8String.fromString(s)); }\n"
            code ++= s"    }\n"
            code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset${idx});\n"
          } else {
            code ++= s"    String v${idx} = msg.${getterName}();\n"
            code ++= s"    if (v${idx} == null) {\n"
            code ++= s"      writer.setNullAt($idx);\n"
            code ++= s"    } else {\n"
            code ++= s"      writer.write($idx, UTF8String.fromString(v${idx}));\n"
            code ++= s"    }\n"
          }
        case FieldDescriptor.JavaType.BYTE_STRING =>
          if (fd.isRepeated) {
            // Repeated ByteString: variable-length data
            val elemSize = 8 // byte strings are variable-length; we store offset & length
            code ++= s"    int size${idx} = msg.${countMethodName}();\n"
            code ++= s"    int offset${idx} = writer.cursor();\n"
            code ++= s"    org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter${idx} = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(writer, ${elemSize});\n"
            code ++= s"    arrayWriter${idx}.initialize(size${idx});\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) {\n"
            code ++= s"      " + classOf[ByteString].getName + s" bs = msg.${indexGetterName}(i);\n"
            code ++= s"      if (bs == null) { arrayWriter${idx}.setNull(i); } else { arrayWriter${idx}.write(i, bs.toByteArray()); }\n"
            code ++= s"    }\n"
            code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset${idx});\n"
          } else {
            code ++= s"    " + classOf[ByteString].getName + s" b${idx} = msg.${getterName}();\n"
            code ++= s"    if (b${idx} == null) {\n"
            code ++= s"      writer.setNullAt($idx);\n"
            code ++= s"    } else {\n"
            code ++= s"      writer.write($idx, b${idx}.toByteArray());\n"
            code ++= s"    }\n"
          }
        case FieldDescriptor.JavaType.ENUM =>
          if (fd.isRepeated) {
            // Repeated enum: convert to strings (variable-length data)
            val elemSize = 8 // enums converted to strings are variable-length
            code ++= s"    int size${idx} = msg.${countMethodName}();\n"
            code ++= s"    int offset${idx} = writer.cursor();\n"
            code ++= s"    org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter${idx} = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(writer, ${elemSize});\n"
            code ++= s"    arrayWriter${idx}.initialize(size${idx});\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) {\n"
            code ++= s"      " + classOf[ProtocolMessageEnum].getName + s" e = msg.${indexGetterName}(i);\n"
            code ++= s"      if (e == null) { arrayWriter${idx}.setNull(i); } else { arrayWriter${idx}.write(i, UTF8String.fromString(e.toString())); }\n"
            code ++= s"    }\n"
            code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset${idx});\n"
          } else {
            code ++= s"    " + classOf[ProtocolMessageEnum].getName + s" e${idx} = msg.${getterName}();\n"
            code ++= s"    if (e${idx} == null) {\n"
            code ++= s"      writer.setNullAt($idx);\n"
            code ++= s"    } else {\n"
            code ++= s"      writer.write($idx, UTF8String.fromString(e${idx}.toString()));\n"
            code ++= s"    }\n"
          }
        case FieldDescriptor.JavaType.MESSAGE =>
          val converterIndex = fieldToConverterIndex(fd)
          val nestedConverterName = s"nestedConv${converterIndex}"
          if (fd.isRepeated) {
            // Repeated message: use nested converter for each element
            val elemSize = 8 // nested structs are variable-length; we store offset & length (8 bytes)
            code ++= s"    int size${idx} = msg.${countMethodName}();\n"
            code ++= s"    int offset${idx} = writer.cursor();\n"
            code ++= s"    org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter${idx} = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(writer, ${elemSize});\n"
            code ++= s"    arrayWriter${idx}.initialize(size${idx});\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) {\n"
            code ++= s"      " + classOf[Message].getName + s" element = (" + classOf[Message].getName + s") msg.${indexGetterName}(i);\n"
            code ++= s"      if (element == null || ${nestedConverterName} == null) { \n"
            code ++= s"        arrayWriter${idx}.setNull(i); \n"
            code ++= s"      } else { \n"
            code ++= s"        int elemOffset = arrayWriter${idx}.cursor();\n"
            code ++= s"        ${nestedConverterName}.convert(element, writer);\n"
            code ++= s"        arrayWriter${idx}.setOffsetAndSizeFromPreviousCursor(i, elemOffset);\n"
            code ++= s"      }\n"
            code ++= s"    }\n"
            code ++= s"    writer.setOffsetAndSizeFromPreviousCursor($idx, offset${idx});\n"
          } else {
            // Singular message: use nested converter, handle nullability
            hasMethodName match {
              case Some(method) =>
                code ++= s"    if (!msg.${method}()) {\n"
                code ++= s"      writer.setNullAt($idx);\n"
                code ++= s"    } else {\n"
                code ++= s"      " + classOf[Message].getName + s" v${idx} = (" + classOf[Message].getName + s") msg.${getterName}();\n"
                code ++= s"      if (${nestedConverterName} == null) {\n"
                code ++= s"        writer.setNullAt($idx);\n"
                code ++= s"      } else {\n"
                code ++= s"        int offset${idx} = writer.cursor();\n"
                code ++= s"        ${nestedConverterName}.convert(v${idx}, writer);\n"
                code ++= s"        writer.setOffsetAndSizeFromPreviousCursor($idx, offset${idx});\n"
                code ++= s"      }\n"
                code ++= s"    }\n"
              case None =>
                code ++= s"    " + classOf[Message].getName + s" v${idx} = (" + classOf[Message].getName + s") msg.${getterName}();\n"
                code ++= s"    if (v${idx} == null || ${nestedConverterName} == null) {\n"
                code ++= s"      writer.setNullAt($idx);\n"
                code ++= s"    } else {\n"
                code ++= s"      int offset${idx} = writer.cursor();\n"
                code ++= s"      ${nestedConverterName}.convert(v${idx}, writer);\n"
                code ++= s"      writer.setOffsetAndSizeFromPreviousCursor($idx, offset${idx});\n"
                code ++= s"    }\n"
            }
          }
      }
    }
    
    code ++= "    if (parentWriter != null) {\n"
    code ++= "      // When using shared writer, data is written to parent buffer; return null\n"
    code ++= "      return null;\n"
    code ++= "    } else {\n"
    code ++= "      // Root conversion - return the constructed row\n"
    code ++= "      return writer.getRow();\n"
    code ++= "    }\n"
    code ++= "  }\n" // End of convert(T, UnsafeWriter) method
    
    // Bridge method with @Override for Object signature
    code ++= "  @Override\n"
    code ++= "  public org.apache.spark.sql.catalyst.InternalRow convert(Object obj) {\n"
    code ++= s"    return convert((${messageClass.getName}) obj);\n"
    code ++= "  }\n"
    
    // Bridge method with @Override for Object, UnsafeWriter signature
    code ++= "  @Override\n"
    code ++= "  public org.apache.spark.sql.catalyst.InternalRow convert(Object obj, org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter parentWriter) {\n"
    code ++= s"    return convert((${messageClass.getName}) obj, parentWriter);\n"
    code ++= "  }\n"
    // Implement the schema() accessor defined on RowConverter.  Returning the
    // stored StructType allows callers to inspect the Catalyst schema used
    // during conversion.
    code ++= "  @Override\n"
    code ++= "  public org.apache.spark.sql.types.StructType schema() {\n"
    code ++= "    return this.schema;\n"
    code ++= "  }\n"
    code ++= "}\n" // End of class

    // Compile the generated Java code using Janino
    val compiler = new SimpleCompiler()
    compiler.setParentClassLoader(this.getClass.getClassLoader)
    compiler.cook(code.toString)
    val generatedClass = compiler.getClassLoader.loadClass(className)

    // Create converter with just schema - dependencies will be set via setters
    val constructor = generatedClass.getConstructor(classOf[StructType])
    constructor.newInstance(schema).asInstanceOf[RowConverter[_ <: Message]]
  }
}