package fastproto

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.WireFormat
import org.apache.spark.sql.types._
import org.codehaus.janino.SimpleCompiler

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Factory object for generating optimized [[StreamWireParser]] instances.
 *
 * Given a protobuf [[Descriptor]] and corresponding Spark SQL schema, this object
 * generates specialized Java code that directly parses wire format data into
 * UnsafeRow structures. The generated code is optimized for JIT compilation with:
 *
 * - Inlined field parsing (no runtime type checking)
 * - Branch prediction friendly control flow
 * - Primitive array usage to avoid boxing
 * - Monomorphic call sites for better inlining
 *
 * Uses Janino for runtime Java compilation and caches generated parsers to
 * avoid redundant compilation.
 */
object WireFormatToRowGenerator {

  // Global cache for compiled classes (classes are immutable and thread-safe)
  private val classCache: ConcurrentHashMap[String, Class[_ <: StreamWireParser]] =
    new ConcurrentHashMap()

  // Thread-local cache for parser instances (instances have mutable state)
  private val instanceCache: ThreadLocal[scala.collection.mutable.Map[String, StreamWireParser]] =
    ThreadLocal.withInitial(() => scala.collection.mutable.Map.empty[String, StreamWireParser])

  // ========== Wire Format Type Categorization ==========

  /**
   * Wire format-based type categorization methods.
   * These group types by their semantic wire format characteristics.
   */
  private def isVarint32(fieldType: FieldDescriptor.Type): Boolean = {
    import FieldDescriptor.Type._
    fieldType match {
      case INT32 | SINT32 | UINT32 | ENUM => true
      case _ => false
    }
  }

  private def isVarint64(fieldType: FieldDescriptor.Type): Boolean = {
    import FieldDescriptor.Type._
    fieldType match {
      case INT64 | SINT64 | UINT64 => true
      case _ => false
    }
  }

  private def isFixedInt32(fieldType: FieldDescriptor.Type): Boolean = {
    import FieldDescriptor.Type._
    fieldType match {
      case FIXED32 | SFIXED32 => true
      case _ => false
    }
  }

  private def isFixedInt64(fieldType: FieldDescriptor.Type): Boolean = {
    import FieldDescriptor.Type._
    fieldType match {
      case FIXED64 | SFIXED64 => true
      case _ => false
    }
  }

  private def isLengthDelimited(fieldType: FieldDescriptor.Type): Boolean = {
    import FieldDescriptor.Type._
    fieldType match {
      case STRING | BYTES | MESSAGE => true
      case _ => false
    }
  }

  // ========== Method Name Mappings ==========

  /**
   * Get the CodedInputStream read method name for a field type.
   */
  private def getReadMethod(fieldType: FieldDescriptor.Type): String = {
    import FieldDescriptor.Type._
    fieldType match {
      case INT32 => "readInt32"
      case SINT32 => "readSInt32"
      case UINT32 => "readUInt32"
      case INT64 => "readInt64"
      case SINT64 => "readSInt64"
      case UINT64 => "readUInt64"
      case FIXED32 => "readFixed32"
      case SFIXED32 => "readSFixed32"
      case FIXED64 => "readFixed64"
      case SFIXED64 => "readSFixed64"
      case FLOAT => "readFloat"
      case DOUBLE => "readDouble"
      case BOOL => "readBool"
      case STRING | BYTES | MESSAGE => "readByteArray"
      case ENUM => "readEnum"
    }
  }

  /**
   * Get the packed parsing method name for a field type.
   */
  private def getPackedParseMethod(fieldType: FieldDescriptor.Type): String = {
    import FieldDescriptor.Type._
    fieldType match {
      case INT32 | UINT32 | ENUM => "parsePackedVarint32s"
      case SINT32 => "parsePackedSInt32s"
      case FIXED32 | SFIXED32 => "parsePackedFixed32s"
      case INT64 | UINT64 => "parsePackedVarint64s"
      case SINT64 => "parsePackedSInt64s"
      case FIXED64 | SFIXED64 => "parsePackedFixed64s"
      case FLOAT => "parsePackedFloats"
      case DOUBLE => "parsePackedDoubles"
      case BOOL => "parsePackedBooleans"
      case _ => throw new IllegalArgumentException(s"Type $fieldType cannot be packed")
    }
  }

  /**
   * Get the bytes per element for fixed-size types, None for variable-length types.
   */
  private def getBytesPerElement(fieldType: FieldDescriptor.Type): Option[Int] = {
    import FieldDescriptor.Type._
    fieldType match {
      case t if isFixedInt32(t) => Some(4)
      case t if isFixedInt64(t) => Some(8)
      case FLOAT => Some(4)
      case DOUBLE => Some(8)
      case BOOL => Some(1)
      case _ => None  // Variable-length types
    }
  }

  /**
   * Get the array resize method name for a field type.
   */
  private def getResizeMethod(fieldType: FieldDescriptor.Type): String = {
    import FieldDescriptor.Type._
    fieldType match {
      case t if isVarint32(t) || isFixedInt32(t) => "resizeIntArray"
      case t if isVarint64(t) || isFixedInt64(t) => "resizeLongArray"
      case FLOAT => "resizeFloatArray"
      case DOUBLE => "resizeDoubleArray"
      case BOOL => "resizeBooleanArray"
      case t if isLengthDelimited(t) => "resizeByteArrayArray"
      case ENUM => "resizeIntArray"
      case _ => throw new IllegalArgumentException(s"Unknown type: $fieldType")
    }
  }

  /**
   * Get the array write method name for a field type.
   */
  private def getWriteArrayMethod(fieldType: FieldDescriptor.Type): String = {
    import FieldDescriptor.Type._
    fieldType match {
      case t if isVarint32(t) || isFixedInt32(t) => "writeIntArray"
      case t if isVarint64(t) || isFixedInt64(t) => "writeLongArray"
      case FLOAT => "writeFloatArray"
      case DOUBLE => "writeDoubleArray"
      case BOOL => "writeBooleanArray"
      case STRING => "writeStringArray"
      case BYTES => "writeBytesArray"
      case MESSAGE => "writeMessageArray"
      case ENUM => "writeIntArray"
      case _ => throw new IllegalArgumentException(s"Unsupported array type: $fieldType")
    }
  }

  /**
   * Generate or retrieve a cached parser for the given descriptor and schema.
   * Uses two-tier caching: globally cached compiled classes + thread-local instances.
   * This avoids redundant compilation while ensuring thread safety.
   *
   * @param descriptor the protobuf message descriptor
   * @param schema     the target Spark SQL schema
   * @return an optimized parser for wire format parsing
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
      schema: StructType
  ): Unit = {
    val key = s"${descriptor.getFullName}_${schema.hashCode()}"
    val parser = localParsers(key)

    // Set nested parsers - only for fields that exist in both descriptor and schema
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

      val setterMethod = parser.getClass.getMethod(s"setNestedParser${field.getNumber}", classOf[StreamWireParser])
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

      wireDependencies(localParsers, field.getMessageType, nestedSchema)
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
        val className = s"GeneratedWireParser_${descriptor.getName}_${Math.abs(key.hashCode)}"
        val sourceCode = generateSourceCode(className, descriptor, schema)

        // Compile using Janino
        val compiler = new SimpleCompiler()
        compiler.setParentClassLoader(this.getClass.getClassLoader)
        compiler.cook(sourceCode.toString)
        val generatedClass = compiler.getClassLoader.loadClass(className).asInstanceOf[Class[_ <: StreamWireParser]]

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

  /**
   * Generate Java source code for an optimized wire format parser.
   */
  private def generateSourceCode(className: String, descriptor: Descriptor, schema: StructType): StringBuilder = {
    val code = new StringBuilder

    // Imports
    code ++= "import com.google.protobuf.CodedInputStream;\n"
    code ++= "import com.google.protobuf.WireFormat;\n"
    code ++= "import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;\n"
    code ++= "import org.apache.spark.sql.types.StructType;\n"
    code ++= "import org.apache.spark.unsafe.types.UTF8String;\n"
    code ++= "import fastproto.StreamWireParser;\n"
    code ++= "import java.io.IOException;\n"
    code ++= "import fastproto.IntList;\n"
    code ++= "import fastproto.LongList;\n\n"

    // Class declaration
    code ++= s"public final class $className extends StreamWireParser {\n"

    // Field mappings and constants
    generateFieldConstants(code, descriptor, schema)

    // Repeated field accumulators
    generateRepeatedFieldAccumulators(code, descriptor, schema)

    // Nested parser fields
    generateNestedParserFields(code, descriptor, schema)

    // Constructor
    code ++= s"  public $className(StructType schema) {\n"
    code ++= "    super(schema);\n"
    generateRepeatedFieldInitialization(code, descriptor, schema)
    code ++= "  }\n\n"

    // Main parsing method
    generateParseIntoMethod(code, descriptor, schema)

    // Nested parser setter methods
    generateNestedParserSetters(code, descriptor, schema)

    // Helper methods for enum fields
    generateEnumHelperMethods(code, descriptor, schema)

    code ++= "}\n"
    code
  }

  /**
   * Generate nested parser field declarations.
   */
  private def generateNestedParserFields(code: StringBuilder, descriptor: Descriptor, schema: StructType): Unit = {
    val messageFields = descriptor.getFields.asScala.filter { field =>
      field.getType == FieldDescriptor.Type.MESSAGE && schema.fieldNames.contains(field.getName)
    }

    if (messageFields.nonEmpty) {
      code ++= "  // Nested parser fields\n"
      messageFields.foreach { field =>
        code ++= s"  private StreamWireParser nestedConv${field.getNumber};\n"
      }
      code ++= "\n"
    }
  }

  /**
   * Generate setter methods for nested parsers.
   */
  private def generateNestedParserSetters(code: StringBuilder, descriptor: Descriptor, schema: StructType): Unit = {
    val messageFields = descriptor.getFields.asScala.filter { field =>
      field.getType == FieldDescriptor.Type.MESSAGE && schema.fieldNames.contains(field.getName)
    }

    messageFields.foreach { field =>
      val fieldNum = field.getNumber
      code ++= s"  public void setNestedParser${fieldNum}(StreamWireParser conv) {\n"
      code ++= s"    this.nestedConv${fieldNum} = conv;\n"
      code ++= "  }\n\n"
    }
  }

  /**
   * Generate tag constants for direct tag switching.
   */
  private def generateFieldConstants(code: StringBuilder, descriptor: Descriptor, schema: StructType): Unit = {
    code ++= "  // Tag constants for direct switch (field_number << 3 | wire_type)\n"
    descriptor.getFields.asScala.foreach { field =>
      try {
        schema.fieldIndex(field.getName) // Check field exists in schema
        val expectedWireType = getExpectedWireType(field.getType)
        val tag = (field.getNumber << 3) | expectedWireType
        code ++= s"  private static final int FIELD_${field.getNumber}_TAG = $tag;\n"

        if (field.isRepeated) {
          // For repeated fields, also generate packed wire type tag if packable
          field.getType match {
            case FieldDescriptor.Type.STRING | FieldDescriptor.Type.BYTES | FieldDescriptor.Type.MESSAGE =>
              // These types are not packable, skip
            case _ =>
              // Generate packed tag (always LENGTH_DELIMITED for packed fields)
              val packedTag = (field.getNumber << 3) | 2 // WIRETYPE_LENGTH_DELIMITED = 2
              code ++= s"  private static final int FIELD_${field.getNumber}_PACKED_TAG = $packedTag;\n"
          }
        }
      } catch {
        case _: IllegalArgumentException =>
          // Field not in schema, skip
      }
    }
    code ++= "\n"
  }

  /**
   * Generate repeated field accumulator declarations.
   */
  private def generateRepeatedFieldAccumulators(code: StringBuilder, descriptor: Descriptor, schema: StructType): Unit = {
    val repeatedFields = descriptor.getFields.asScala.filter(field =>
      field.isRepeated && schema.fieldNames.contains(field.getName)
    )

    if (repeatedFields.nonEmpty) {
      code ++= "  // Repeated field accumulators\n"
      repeatedFields.foreach { field =>
        val fieldNum = field.getNumber

        if (isVarint32(field.getType)) {
          code ++= s"  private final IntList field${fieldNum}_list = new IntList();\n"
        } else if (isVarint64(field.getType)) {
          code ++= s"  private final LongList field${fieldNum}_list = new LongList();\n"
        } else {
          val javaType = getJavaElementType(field.getType)
          code ++= s"  private $javaType[] field${fieldNum}_values;\n"
          code ++= s"  private int field${fieldNum}_count = 0;\n"
        }
      }
      code ++= "\n"
    }
  }

  /**
   * Generate repeated field initialization in constructor.
   */
  private def generateRepeatedFieldInitialization(code: StringBuilder, descriptor: Descriptor, schema: StructType): Unit = {
    val repeatedFields = descriptor.getFields.asScala.filter(field =>
      field.isRepeated && schema.fieldNames.contains(field.getName)
    )

    repeatedFields.foreach { field =>
      field.getType match {
        case FieldDescriptor.Type.INT32 | FieldDescriptor.Type.SINT32 =>
          // IntList is initialized in field declaration, no initialization needed
        case FieldDescriptor.Type.INT64 | FieldDescriptor.Type.SINT64 =>
          // LongList is initialized in field declaration, no initialization needed
        case _ =>
          val javaType = getJavaElementType(field.getType)
          val initialCapacity = getInitialCapacity(field.getType)
          // For primitive types, javaType is "int", "long", etc. -> new int[8]
          // For byte array types, javaType is "byte[]" -> new byte[8][]
          if (javaType.endsWith("[]")) {
            val baseType = javaType.dropRight(2)
            code ++= s"    field${field.getNumber}_values = new $baseType[$initialCapacity][];\n"
          } else {
            code ++= s"    field${field.getNumber}_values = new $javaType[$initialCapacity];\n"
          }
      }
    }
  }

  /**
   * Generate the main parseInto method with optimized field parsing.
   */
  private def generateParseIntoMethod(code: StringBuilder, descriptor: Descriptor, schema: StructType): Unit = {
    code ++= "  @Override\n"
    code ++= "  protected void parseInto(CodedInputStream input, UnsafeRowWriter writer) {\n\n"

    // Reset repeated field counters
    val repeatedFields = descriptor.getFields.asScala.filter(field =>
      field.isRepeated && schema.fieldNames.contains(field.getName)
    )
    repeatedFields.foreach { field =>
      val fieldNum = field.getNumber
      if (isVarint32(field.getType) || isVarint64(field.getType)) {
        code ++= s"    field${fieldNum}_list.count = 0;\n"
      } else {
        code ++= s"    field${fieldNum}_count = 0;\n"
      }
    }

    code ++= "\n    try {\n"
    code ++= "      while (!input.isAtEnd()) {\n"
    code ++= "        int tag = input.readTag();\n\n"

    // Generate direct tag switch for maximum performance
    val schemaFields = descriptor.getFields.asScala
      .filter(field => schema.fieldNames.contains(field.getName))
      .sortBy(_.getNumber) // Natural order for better branch prediction

    if (schemaFields.nonEmpty) {
      code ++= "        switch (tag) {\n"

      schemaFields.foreach { field =>
        generateFieldTagCases(code, field, schema)
      }

      code ++= "          default:\n"
      code ++= "            // Unknown tag - skip field\n"
      code ++= "            input.skipField(tag);\n"
      code ++= "            break;\n"
      code ++= "        }\n"
    } else {
      code ++= "        // No known fields - skip all\n"
      code ++= "        input.skipField(tag);\n"
    }

    code ++= "      }\n"
    code ++= "    } catch (Exception e) {\n"
    code ++= "      throw new RuntimeException(\"Failed to parse wire format\", e);\n"
    code ++= "    }\n\n"

    // Write accumulated repeated fields
    if (repeatedFields.nonEmpty) {
      code ++= "    // Write accumulated repeated fields\n"
      repeatedFields.foreach { field =>
        generateRepeatedFieldWriting(code, field, schema)
      }
    }

    code ++= "  }\n\n"
  }

  /**
   * Generate switch cases for a specific field's tags.
   */
  private def generateFieldTagCases(code: StringBuilder, field: FieldDescriptor, schema: StructType): Unit = {
    val ordinal = schema.fieldIndex(field.getName)

    if (field.isRepeated) {
      generateRepeatedFieldTagCases(code, field, ordinal)
    } else {
      generateSingularFieldTagCase(code, field, ordinal)
    }
  }

  /**
   * Generate switch case for a singular field tag.
   */
  private def generateSingularFieldTagCase(code: StringBuilder, field: FieldDescriptor, ordinal: Int): Unit = {
    val fieldNum = field.getNumber
    code ++= s"          case FIELD_${fieldNum}_TAG:\n"

    field.getType match {
      case FieldDescriptor.Type.INT32 =>
        code ++= s"            writer.write($ordinal, input.readInt32());\n"
      case FieldDescriptor.Type.INT64 =>
        code ++= s"            writer.write($ordinal, input.readInt64());\n"
      case FieldDescriptor.Type.UINT32 =>
        code ++= s"            writer.write($ordinal, input.readUInt32());\n"
      case FieldDescriptor.Type.UINT64 =>
        code ++= s"            writer.write($ordinal, input.readUInt64());\n"
      case FieldDescriptor.Type.SINT32 =>
        code ++= s"            writer.write($ordinal, input.readSInt32());\n"
      case FieldDescriptor.Type.SINT64 =>
        code ++= s"            writer.write($ordinal, input.readSInt64());\n"
      case FieldDescriptor.Type.FIXED32 =>
        code ++= s"            writer.write($ordinal, input.readFixed32());\n"
      case FieldDescriptor.Type.FIXED64 =>
        code ++= s"            writer.write($ordinal, input.readFixed64());\n"
      case FieldDescriptor.Type.SFIXED32 =>
        code ++= s"            writer.write($ordinal, input.readSFixed32());\n"
      case FieldDescriptor.Type.SFIXED64 =>
        code ++= s"            writer.write($ordinal, input.readSFixed64());\n"
      case FieldDescriptor.Type.FLOAT =>
        code ++= s"            writer.write($ordinal, input.readFloat());\n"
      case FieldDescriptor.Type.DOUBLE =>
        code ++= s"            writer.write($ordinal, input.readDouble());\n"
      case FieldDescriptor.Type.BOOL =>
        code ++= s"            writer.write($ordinal, input.readBool());\n"
      case FieldDescriptor.Type.STRING =>
        code ++= s"            writer.write($ordinal, input.readByteArray());\n"
      case FieldDescriptor.Type.BYTES =>
        code ++= s"            writer.write($ordinal, input.readByteArray());\n"
      case FieldDescriptor.Type.ENUM =>
        // FIXME unify enum representation in single/repeated, and update schema generation accordingly
        // see https://github.com/bumfo/spark-protobuf-backport/pull/10/files/a9ff39ca02f0b7ce811158ded8281940e40aeb3f#r2354592327
        code ++= s"            writer.write($ordinal, UTF8String.fromString(getEnumName${fieldNum}(input.readEnum())));\n"
      case FieldDescriptor.Type.MESSAGE =>
        code ++= s"            byte[] messageBytes = input.readByteArray();\n"
        code ++= s"            writeMessage(messageBytes, $ordinal, nestedConv${fieldNum}, writer);\n"
      case _ =>
        code ++= s"            input.skipField(tag); // Unsupported type\n"
    }

    code ++= "            break;\n"
  }

  /**
   * Generate switch cases for a repeated field (both regular and packed tags).
   */
  private def generateRepeatedFieldTagCases(code: StringBuilder, field: FieldDescriptor, ordinal: Int): Unit = {
    val fieldNum = field.getNumber

    // Generate regular tag case
    code ++= s"          case FIELD_${fieldNum}_TAG:\n"
    code ++= s"            // Single repeated value for field ${field.getName}\n"
    generateSingleRepeatedValueParsing(code, field)
    code ++= "            break;\n"

    // Generate packed tag case if applicable
    field.getType match {
      case FieldDescriptor.Type.STRING | FieldDescriptor.Type.BYTES | FieldDescriptor.Type.MESSAGE =>
        // These types are not packable, skip packed case
      case _ =>
        code ++= s"          case FIELD_${fieldNum}_PACKED_TAG:\n"
        code ++= s"            // Packed repeated values for field ${field.getName}\n"
        generatePackedFieldParsing(code, field)
        code ++= "            break;\n"
    }
  }

  /**
   * Generate packed field parsing logic.
   */
  private def generatePackedFieldParsing(code: StringBuilder, field: FieldDescriptor): Unit = {
    import FieldDescriptor.Type._
    val fieldNum = field.getNumber

    field.getType match {
      case INT32 | UINT32 | ENUM =>
        // Variable-length int32 types use IntList for efficient parsing
        code ++= s"            parsePackedVarint32s(input, field${fieldNum}_list);\n"
      case SINT32 =>
        // SINT32 uses ZigZag encoding with IntList
        code ++= s"            parsePackedSInt32s(input, field${fieldNum}_list);\n"
      case INT64 | UINT64 =>
        // Variable-length int64 types use LongList for efficient parsing
        code ++= s"            parsePackedVarint64s(input, field${fieldNum}_list);\n"
      case SINT64 =>
        // SINT64 uses ZigZag encoding with LongList
        code ++= s"            parsePackedSInt64s(input, field${fieldNum}_list);\n"
      case _ =>
        // Fixed-size types use array with pre-calculated count
        generateFixedSizePackedParsing(code, fieldNum, field.getType)
    }
  }

  /**
   * Generate packed field parsing for fixed-size types.
   */
  private def generateFixedSizePackedParsing(code: StringBuilder, fieldNum: Int, fieldType: FieldDescriptor.Type): Unit = {
    val parseMethod = getPackedParseMethod(fieldType)
    val bytesPerElement = getBytesPerElement(fieldType).getOrElse(
      throw new IllegalArgumentException(s"$fieldType is not a fixed-size type")
    )

    code ++= s"            int packedLength${fieldNum} = input.readRawVarint32();\n"
    code ++= s"            if (packedLength${fieldNum} > 0) {\n"
    code ++= s"              int oldCount${fieldNum} = field${fieldNum}_count;\n"
    code ++= s"              field${fieldNum}_values = $parseMethod(input, field${fieldNum}_values, field${fieldNum}_count, packedLength${fieldNum});\n"
    code ++= s"              field${fieldNum}_count = oldCount${fieldNum} + (packedLength${fieldNum} / $bytesPerElement);\n"
    code ++= s"            }\n"
  }

  /**
   * Generate single repeated value parsing logic.
   */
  private def generateSingleRepeatedValueParsing(code: StringBuilder, field: FieldDescriptor): Unit = {
    val fieldNum = field.getNumber
    val readMethod = getReadMethod(field.getType)

    if (isVarint32(field.getType)) {
      // Variable-length int32 types use IntList
      code ++= s"            field${fieldNum}_list.add(input.$readMethod());\n"
    } else if (isVarint64(field.getType)) {
      // Variable-length int64 types use LongList
      code ++= s"            field${fieldNum}_list.add(input.$readMethod());\n"
    } else {
      // All other types use array-based approach
      generateArrayValueParsing(code, fieldNum, readMethod, field.getType)
    }
  }

  /**
   * Generate array-based value parsing with resize check.
   */
  private def generateArrayValueParsing(code: StringBuilder, fieldNum: Int, readMethod: String, fieldType: FieldDescriptor.Type): Unit = {
    // Generate resize check
    code ++= s"            if (field${fieldNum}_count >= field${fieldNum}_values.length) {\n"
    val resizeMethod = getResizeMethod(fieldType)
    code ++= s"              field${fieldNum}_values = $resizeMethod(field${fieldNum}_values, field${fieldNum}_count, field${fieldNum}_count + 1);\n"
    code ++= s"            }\n"
    // Generate the read - same pattern for all array types
    code ++= s"            field${fieldNum}_values[field${fieldNum}_count++] = input.$readMethod();\n"
  }

  /**
   * Generate output writing for a repeated field.
   */
  private def generateRepeatedFieldWriting(code: StringBuilder, field: FieldDescriptor, schema: StructType): Unit = {
    val fieldNum = field.getNumber
    val ordinal = schema.fieldIndex(field.getName)

    if (isVarint32(field.getType)) {
      code ++= s"    if (field${fieldNum}_list.count > 0) {\n"
      code ++= s"      writeIntArray(field${fieldNum}_list.array, field${fieldNum}_list.count, $ordinal, writer);\n"
      code ++= s"    }\n"
    } else if (isVarint64(field.getType)) {
      code ++= s"    if (field${fieldNum}_list.count > 0) {\n"
      code ++= s"      writeLongArray(field${fieldNum}_list.array, field${fieldNum}_list.count, $ordinal, writer);\n"
      code ++= s"    }\n"
    } else {
      val writeMethod = getWriteArrayMethod(field.getType)
      code ++= s"    if (field${fieldNum}_count > 0) {\n"
      if (field.getType == FieldDescriptor.Type.MESSAGE) {
        code ++= s"      $writeMethod(field${fieldNum}_values, field${fieldNum}_count, $ordinal, nestedConv${fieldNum}, writer);\n"
      } else {
        code ++= s"      $writeMethod(field${fieldNum}_values, field${fieldNum}_count, $ordinal, writer);\n"
      }
      code ++= s"    }\n"
    }
  }

  /**
   * Get the expected wire type for a protobuf field type.
   */
  private def getExpectedWireType(fieldType: FieldDescriptor.Type): Int = {
    import FieldDescriptor.Type._
    fieldType match {
      case DOUBLE => WireFormat.WIRETYPE_FIXED64
      case FLOAT => WireFormat.WIRETYPE_FIXED32
      case INT64 => WireFormat.WIRETYPE_VARINT
      case UINT64 => WireFormat.WIRETYPE_VARINT
      case INT32 => WireFormat.WIRETYPE_VARINT
      case FIXED64 => WireFormat.WIRETYPE_FIXED64
      case FIXED32 => WireFormat.WIRETYPE_FIXED32
      case BOOL => WireFormat.WIRETYPE_VARINT
      case STRING => WireFormat.WIRETYPE_LENGTH_DELIMITED
      case BYTES => WireFormat.WIRETYPE_LENGTH_DELIMITED
      case UINT32 => WireFormat.WIRETYPE_VARINT
      case ENUM => WireFormat.WIRETYPE_VARINT
      case SFIXED32 => WireFormat.WIRETYPE_FIXED32
      case SFIXED64 => WireFormat.WIRETYPE_FIXED64
      case SINT32 => WireFormat.WIRETYPE_VARINT
      case SINT64 => WireFormat.WIRETYPE_VARINT
      case MESSAGE => WireFormat.WIRETYPE_LENGTH_DELIMITED
      case GROUP => throw new UnsupportedOperationException("GROUP type is deprecated")
    }
  }

  /**
   * Get the Java element type for a protobuf field type.
   */
  private def getJavaElementType(fieldType: FieldDescriptor.Type): String = {
    import FieldDescriptor.Type._
    fieldType match {
      case INT32 | UINT32 | SINT32 | FIXED32 | SFIXED32 | ENUM => "int"
      case INT64 | UINT64 | SINT64 | FIXED64 | SFIXED64 => "long"
      case FLOAT => "float"
      case DOUBLE => "double"
      case BOOL => "boolean"
      case STRING | BYTES | MESSAGE => "byte[]"
      case _ => throw new UnsupportedOperationException(s"Unsupported array type: $fieldType")
    }
  }

  /**
   * Generate helper methods for enum fields to convert enum values to names.
   */
  private def generateEnumHelperMethods(code: StringBuilder, descriptor: Descriptor, schema: StructType): Unit = {
    val enumFields = descriptor.getFields.asScala.filter { field =>
      field.getType == FieldDescriptor.Type.ENUM && schema.fieldNames.contains(field.getName)
    }

    enumFields.foreach { field =>
      val fieldNum = field.getNumber
      val enumDescriptor = field.getEnumType

      code ++= s"  private String getEnumName${fieldNum}(int value) {\n"
      code ++= "    switch (value) {\n"

      // Generate case for each enum value
      enumDescriptor.getValues.asScala.foreach { enumValue =>
        val name = enumValue.getName
        val number = enumValue.getNumber
        code ++= s"""      case $number: return "$name";\n"""
      }

      code ++= s"""      default: return "UNKNOWN_ENUM_VALUE_" + value;\n"""
      code ++= "    }\n"
      code ++= "  }\n\n"
    }
  }

  /**
   * Get initial array capacity for repeated fields.
   */
  private def getInitialCapacity(fieldType: FieldDescriptor.Type): Int = {
    // Start with reasonable defaults - will grow if needed
    8
  }
}