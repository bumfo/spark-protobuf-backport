package fastproto

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.WireFormat
import org.apache.spark.sql.types._
import org.codehaus.janino.SimpleCompiler

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Factory object for generating optimized [[AbstractWireFormatConverter]] instances.
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
 * Uses Janino for runtime Java compilation and caches generated converters to
 * avoid redundant compilation.
 */
object WireFormatToRowGenerator {

  // Thread-safe cache for generated converters
  private val converterCache: ConcurrentHashMap[String, AbstractWireFormatConverter] =
    new ConcurrentHashMap()

  /**
   * Generate or retrieve a cached converter for the given descriptor and schema.
   *
   * @param descriptor the protobuf message descriptor
   * @param schema     the target Spark SQL schema
   * @return an optimized converter for wire format parsing
   */
  def generateConverter(descriptor: Descriptor, schema: StructType): AbstractWireFormatConverter = {
    val key = s"${descriptor.getFullName}_${schema.hashCode()}"

    // Check cache first
    Option(converterCache.get(key)) match {
      case Some(converter) => converter
      case None =>
        // Generate new converter
        val converter = createConverterGraph(descriptor, schema)
        // Try to cache it, but use the cached version if someone else beat us to it
        Option(converterCache.putIfAbsent(key, converter)).getOrElse(converter)
    }
  }

  /**
   * Create a converter with all its nested dependencies.
   */
  private def createConverterGraph(descriptor: Descriptor, schema: StructType): AbstractWireFormatConverter = {
    // Create local converter map for this generation cycle
    val localConverters = scala.collection.mutable.Map[String, AbstractWireFormatConverter]()

    // Generate converters for nested types
    val rootConverter = generateConverterInternal(descriptor, schema, localConverters)

    // Wire up nested converter dependencies
    wireDependencies(localConverters, descriptor, schema)

    rootConverter
  }

  /**
   * Generate a single converter and recursively create nested converters.
   */
  private def generateConverterInternal(
      descriptor: Descriptor,
      schema: StructType,
      localConverters: scala.collection.mutable.Map[String, AbstractWireFormatConverter]
  ): AbstractWireFormatConverter = {
    val key = s"${descriptor.getFullName}_${schema.hashCode()}"

    // Check if already being generated
    if (localConverters.contains(key)) {
      return localConverters(key)
    }

    // Generate the converter
    val converter = compileConverter(descriptor, schema)
    localConverters(key) = converter

    // Generate nested converters - only for fields that exist in both descriptor and schema
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

      generateConverterInternal(field.getMessageType, nestedSchema, localConverters)
    }

    converter
  }

  /**
   * Wire up nested converter dependencies after all converters are created.
   */
  private def wireDependencies(
      localConverters: scala.collection.mutable.Map[String, AbstractWireFormatConverter],
      descriptor: Descriptor,
      schema: StructType
  ): Unit = {
    val key = s"${descriptor.getFullName}_${schema.hashCode()}"
    val converter = localConverters(key)

    // Set nested converters - only for fields that exist in both descriptor and schema
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
      val nestedConverter = localConverters.get(nestedKey).orElse(
        Option(converterCache.get(nestedKey))
      ).getOrElse(
        throw new IllegalStateException(s"Nested converter not found: $nestedKey")
      )

      converter.setNestedConverter(field.getNumber, nestedConverter)
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

      wireDependencies(localConverters, field.getMessageType, nestedSchema)
    }
  }

  /**
   * Compile a single converter using Janino.
   */
  private def compileConverter(descriptor: Descriptor, schema: StructType): AbstractWireFormatConverter = {
    val className = s"GeneratedWireConverter_${descriptor.getName}_${System.nanoTime()}"
    val sourceCode = generateSourceCode(className, descriptor, schema)

    // Compile using Janino
    val compiler = new SimpleCompiler()
    compiler.setParentClassLoader(this.getClass.getClassLoader)
    compiler.cook(sourceCode.toString)
    val generatedClass = compiler.getClassLoader.loadClass(className)

    // Instantiate converter
    val constructor = generatedClass.getConstructor(classOf[StructType])
    constructor.newInstance(schema).asInstanceOf[AbstractWireFormatConverter]
  }

  /**
   * Generate Java source code for an optimized wire format converter.
   */
  private def generateSourceCode(className: String, descriptor: Descriptor, schema: StructType): StringBuilder = {
    val code = new StringBuilder

    // Imports
    code ++= "import com.google.protobuf.CodedInputStream;\n"
    code ++= "import com.google.protobuf.WireFormat;\n"
    code ++= "import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;\n"
    code ++= "import org.apache.spark.sql.types.StructType;\n"
    code ++= "import org.apache.spark.unsafe.types.UTF8String;\n"
    code ++= "import fastproto.AbstractWireFormatConverter;\n\n"

    // Class declaration
    code ++= s"public final class $className extends AbstractWireFormatConverter {\n"

    // Field mappings and constants
    generateFieldConstants(code, descriptor, schema)

    // Repeated field accumulators
    generateRepeatedFieldAccumulators(code, descriptor, schema)

    // Constructor
    code ++= s"  public $className(StructType schema) {\n"
    code ++= "    super(schema);\n"
    generateRepeatedFieldInitialization(code, descriptor, schema)
    code ++= "  }\n\n"

    // Main parsing method
    generateWriteDataMethod(code, descriptor, schema)

    // Helper methods for repeated fields
    generateRepeatedFieldMethods(code, descriptor, schema)

    // Helper methods for enum fields
    generateEnumHelperMethods(code, descriptor, schema)

    code ++= "}\n"
    code
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
        val javaType = getJavaElementType(field.getType)
        code ++= s"  private $javaType[] field${field.getNumber}_values;\n"
        code ++= s"  private int field${field.getNumber}_count = 0;\n"
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

  /**
   * Generate the main writeData method with optimized field parsing.
   */
  private def generateWriteDataMethod(code: StringBuilder, descriptor: Descriptor, schema: StructType): Unit = {
    code ++= "  @Override\n"
    code ++= "  protected void writeData(byte[] binary, UnsafeRowWriter writer) {\n"
    code ++= "    CodedInputStream input = CodedInputStream.newInstance(binary);\n\n"

    // Reset repeated field counters
    val repeatedFields = descriptor.getFields.asScala.filter(field =>
      field.isRepeated && schema.fieldNames.contains(field.getName)
    )
    repeatedFields.foreach { field =>
      code ++= s"    field${field.getNumber}_count = 0;\n"
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
        code ++= s"    if (field${field.getNumber}_count > 0) {\n"
        code ++= s"      writeField${field.getNumber}Array(writer);\n"
        code ++= s"    }\n"
      }
    }

    code ++= "  }\n\n"
  }

  /**
   * Generate switch cases for a specific field's tags.
   */
  private def generateFieldTagCases(code: StringBuilder, field: FieldDescriptor, schema: StructType): Unit = {
    val fieldNum = field.getNumber
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
        code ++= s"            byte[] bytes = input.readBytes().toByteArray();\n"
        code ++= s"            writer.write($ordinal, UTF8String.fromBytes(bytes));\n"
      case FieldDescriptor.Type.BYTES =>
        code ++= s"            writer.write($ordinal, input.readBytes().toByteArray());\n"
      case FieldDescriptor.Type.ENUM =>
        code ++= s"            int enumValue = input.readEnum();\n"
        code ++= s"            writer.write($ordinal, UTF8String.fromString(getEnumName${fieldNum}(enumValue)));\n"
      case FieldDescriptor.Type.MESSAGE =>
        code ++= s"            byte[] messageBytes = input.readBytes().toByteArray();\n"
        code ++= s"            writeMessage(messageBytes, $fieldNum, $ordinal, writer);\n"
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
    val fieldNum = field.getNumber
    val methodName = field.getType match {
      case FieldDescriptor.Type.INT32 | FieldDescriptor.Type.UINT32 | FieldDescriptor.Type.SINT32 |
           FieldDescriptor.Type.FIXED32 | FieldDescriptor.Type.SFIXED32 | FieldDescriptor.Type.ENUM => "parsePackedInts"
      case FieldDescriptor.Type.INT64 | FieldDescriptor.Type.UINT64 | FieldDescriptor.Type.SINT64 |
           FieldDescriptor.Type.FIXED64 | FieldDescriptor.Type.SFIXED64 => "parsePackedLongs"
      case FieldDescriptor.Type.FLOAT => "parsePackedFloats"
      case FieldDescriptor.Type.DOUBLE => "parsePackedDoubles"
      case FieldDescriptor.Type.BOOL => "parsePackedBooleans"
      case _ => "unsupported"
    }

    if (methodName != "unsupported") {
      code ++= s"            int parsed = $methodName(input, field${fieldNum}_values, field${fieldNum}_values.length - field${fieldNum}_count);\n"
      code ++= s"            field${fieldNum}_count += parsed;\n"
    }
  }

  /**
   * Generate single repeated value parsing logic.
   */
  private def generateSingleRepeatedValueParsing(code: StringBuilder, field: FieldDescriptor): Unit = {
    val fieldNum = field.getNumber

    // Ensure array capacity
    code ++= s"            if (field${fieldNum}_count >= field${fieldNum}_values.length) {\n"
    val javaType = getJavaElementType(field.getType)
    if (javaType.endsWith("[]")) {
      val baseType = javaType.dropRight(2)
      code ++= s"              $javaType[] newArray = new $baseType[field${fieldNum}_values.length * 2][];\n"
    } else {
      code ++= s"              $javaType[] newArray = new $javaType[field${fieldNum}_values.length * 2];\n"
    }
    code ++= s"              System.arraycopy(field${fieldNum}_values, 0, newArray, 0, field${fieldNum}_count);\n"
    code ++= s"              field${fieldNum}_values = newArray;\n"
    code ++= s"            }\n"

    // Parse single value
    field.getType match {
      case FieldDescriptor.Type.INT32 =>
        code ++= s"            field${fieldNum}_values[field${fieldNum}_count++] = input.readInt32();\n"
      case FieldDescriptor.Type.INT64 =>
        code ++= s"            field${fieldNum}_values[field${fieldNum}_count++] = input.readInt64();\n"
      case FieldDescriptor.Type.FLOAT =>
        code ++= s"            field${fieldNum}_values[field${fieldNum}_count++] = input.readFloat();\n"
      case FieldDescriptor.Type.DOUBLE =>
        code ++= s"            field${fieldNum}_values[field${fieldNum}_count++] = input.readDouble();\n"
      case FieldDescriptor.Type.BOOL =>
        code ++= s"            field${fieldNum}_values[field${fieldNum}_count++] = input.readBool();\n"
      case FieldDescriptor.Type.STRING =>
        code ++= s"            field${fieldNum}_values[field${fieldNum}_count++] = input.readBytes().toByteArray();\n"
      case FieldDescriptor.Type.BYTES =>
        code ++= s"            field${fieldNum}_values[field${fieldNum}_count++] = input.readBytes().toByteArray();\n"
      case FieldDescriptor.Type.MESSAGE =>
        code ++= s"            field${fieldNum}_values[field${fieldNum}_count++] = input.readBytes().toByteArray();\n"
      case _ =>
        code ++= s"            input.skipField(tag); // Unsupported repeated type\n"
    }
  }

  /**
   * Generate helper methods for writing repeated field arrays.
   */
  private def generateRepeatedFieldMethods(code: StringBuilder, descriptor: Descriptor, schema: StructType): Unit = {
    val repeatedFields = descriptor.getFields.asScala.filter(field =>
      field.isRepeated && schema.fieldNames.contains(field.getName)
    )

    repeatedFields.foreach { field =>
      val fieldNum = field.getNumber
      val ordinal = schema.fieldIndex(field.getName)

      code ++= s"  private void writeField${fieldNum}Array(UnsafeRowWriter writer) {\n"

      field.getType match {
        case FieldDescriptor.Type.INT32 | FieldDescriptor.Type.UINT32 | FieldDescriptor.Type.SINT32 |
             FieldDescriptor.Type.FIXED32 | FieldDescriptor.Type.SFIXED32 | FieldDescriptor.Type.ENUM =>
          code ++= s"    int[] trimmed = new int[field${fieldNum}_count];\n"
          code ++= s"    System.arraycopy(field${fieldNum}_values, 0, trimmed, 0, field${fieldNum}_count);\n"
          code ++= s"    writeIntArray(trimmed, $ordinal, writer);\n"

        case FieldDescriptor.Type.INT64 | FieldDescriptor.Type.UINT64 | FieldDescriptor.Type.SINT64 |
             FieldDescriptor.Type.FIXED64 | FieldDescriptor.Type.SFIXED64 =>
          code ++= s"    long[] trimmed = new long[field${fieldNum}_count];\n"
          code ++= s"    System.arraycopy(field${fieldNum}_values, 0, trimmed, 0, field${fieldNum}_count);\n"
          code ++= s"    writeLongArray(trimmed, $ordinal, writer);\n"

        case FieldDescriptor.Type.FLOAT =>
          code ++= s"    float[] trimmed = new float[field${fieldNum}_count];\n"
          code ++= s"    System.arraycopy(field${fieldNum}_values, 0, trimmed, 0, field${fieldNum}_count);\n"
          code ++= s"    writeFloatArray(trimmed, $ordinal, writer);\n"

        case FieldDescriptor.Type.DOUBLE =>
          code ++= s"    double[] trimmed = new double[field${fieldNum}_count];\n"
          code ++= s"    System.arraycopy(field${fieldNum}_values, 0, trimmed, 0, field${fieldNum}_count);\n"
          code ++= s"    writeDoubleArray(trimmed, $ordinal, writer);\n"

        case FieldDescriptor.Type.BOOL =>
          code ++= s"    boolean[] trimmed = new boolean[field${fieldNum}_count];\n"
          code ++= s"    System.arraycopy(field${fieldNum}_values, 0, trimmed, 0, field${fieldNum}_count);\n"
          code ++= s"    writeBooleanArray(trimmed, $ordinal, writer);\n"

        case FieldDescriptor.Type.STRING =>
          code ++= s"    byte[][] trimmed = new byte[field${fieldNum}_count][];\n"
          code ++= s"    System.arraycopy(field${fieldNum}_values, 0, trimmed, 0, field${fieldNum}_count);\n"
          code ++= s"    writeStringArray(trimmed, $ordinal, writer);\n"

        case FieldDescriptor.Type.BYTES =>
          code ++= s"    byte[][] trimmed = new byte[field${fieldNum}_count][];\n"
          code ++= s"    System.arraycopy(field${fieldNum}_values, 0, trimmed, 0, field${fieldNum}_count);\n"
          code ++= s"    writeBytesArray(trimmed, $ordinal, writer);\n"

        case FieldDescriptor.Type.MESSAGE =>
          code ++= s"    byte[][] trimmed = new byte[field${fieldNum}_count][];\n"
          code ++= s"    System.arraycopy(field${fieldNum}_values, 0, trimmed, 0, field${fieldNum}_count);\n"
          code ++= s"    writeMessageArray(trimmed, $fieldNum, $ordinal, writer);\n"

        case _ =>
          code ++= s"    // Unsupported array type for field ${field.getName}\n"
      }

      code ++= "  }\n\n"

      // Generate packability check method
      field.getType match {
        case FieldDescriptor.Type.STRING | FieldDescriptor.Type.BYTES | FieldDescriptor.Type.MESSAGE =>
          code ++= s"  private static boolean isPackable${field.getType.name()}() { return false; }\n"
        case _ =>
          code ++= s"  private static boolean isPackable${field.getType.name()}() { return true; }\n"
      }
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