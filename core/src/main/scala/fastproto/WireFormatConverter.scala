package fastproto

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.{CodedInputStream, WireFormat}
import fastproto.AbstractWireFormatConverter._
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

/**
 * A high-performance converter that reads protobuf binary data directly from wire format
 * using CodedInputStream, bypassing the need to materialize intermediate Message objects.
 *
 * This optimized converter provides significant performance improvements by:
 * - Extending AbstractWireFormatConverter for optimized helper methods
 * - Using type-specific primitive accumulators (no boxing)
 * - Leveraging packed field parsing methods from AbstractWireFormatConverter
 * - Direct byte copying for strings/bytes/messages
 * - Single-pass streaming parse to UnsafeRow
 *
 * @param descriptor the protobuf message descriptor
 * @param schema     the corresponding Spark SQL schema
 */
class WireFormatConverter(
    descriptor: Descriptor,
    override val schema: StructType)
  extends AbstractWireFormatConverter(schema) {

  import WireFormatConverter._

  // Build field number → row ordinal mapping during construction - use arrays for better performance
  private val (fieldMappingArray, maxFieldNumber) = buildFieldMappingArray()

  // Cache nested converters for message fields - use array for O(1) lookup
  private val nestedConvertersArray: Array[WireFormatConverter] = buildNestedConvertersArray()

  override protected def parseAndWriteFields(binary: Array[Byte], writer: UnsafeRowWriter): Unit = {
    val input = CodedInputStream.newInstance(binary)

    // Reset all field accumulators for this conversion
    resetAccumulators()

    // Parse the wire format
    while (!input.isAtEnd()) {
      val tag = input.readTag()
      val fieldNumber = WireFormat.getTagFieldNumber(tag)
      val wireType = WireFormat.getTagWireType(tag)

      if (fieldNumber <= maxFieldNumber && fieldMappingArray(fieldNumber) != null) {
        parseField(input, tag, wireType, fieldMappingArray(fieldNumber), writer)
      } else {
        // Skip unknown fields
        input.skipField(tag)
      }
    }

    // Write accumulated repeated fields
    writeAccumulatedRepeatedFields(writer)
  }

  private def buildFieldMappingArray(): (Array[FieldMapping], Int) = {
    val tempMapping = scala.collection.mutable.Map[Int, FieldMapping]()
    var maxFieldNum = 0

    descriptor.getFields.asScala.foreach { field =>
      val fieldNum = field.getNumber
      maxFieldNum = Math.max(maxFieldNum, fieldNum)

      // Find corresponding field in Spark schema by name
      schema.fields.zipWithIndex.find(_._1.name == field.getName) match {
        case Some((sparkField, ordinal)) =>
          // Create type-specific accumulator based on field type
          val accumulator = if (field.isRepeated) {
            createAccumulator(field.getType)
          } else {
            null
          }

          tempMapping(fieldNum) = FieldMapping(
            fieldDescriptor = field,
            rowOrdinal = ordinal,
            sparkDataType = sparkField.dataType,
            isRepeated = field.isRepeated,
            accumulator = accumulator
          )
        case None =>
        // Field exists in protobuf but not in Spark schema - skip it
      }
    }

    // Create array and populate it
    val mappingArray = new Array[FieldMapping](maxFieldNum + 1)
    tempMapping.foreach { case (fieldNum, mapping) =>
      mappingArray(fieldNum) = mapping
    }

    (mappingArray, maxFieldNum)
  }

  private def createAccumulator(fieldType: FieldDescriptor.Type): Any = {
    import FieldDescriptor.Type._
    fieldType match {
      // Variable-length int32 types use IntList
      case INT32 | SINT32 | UINT32 | ENUM => new IntList()

      // Variable-length int64 types use LongList
      case INT64 | SINT64 | UINT64 => new LongList()

      // Fixed-size int32 types also use IntList (different packed parsing)
      case FIXED32 | SFIXED32 => new IntList()

      // Fixed-size int64 types also use LongList (different packed parsing)
      case FIXED64 | SFIXED64 => new LongList()

      // Float types use FloatList
      case FLOAT => new FloatList()

      // Double types use DoubleList
      case DOUBLE => new DoubleList()

      // Boolean types use BooleanList
      case BOOL => new BooleanList()

      // String/Bytes/Message types use ByteArrayList
      case STRING | BYTES | MESSAGE => new ByteArrayList()

      case GROUP => throw new UnsupportedOperationException("GROUP type is deprecated and not supported")
    }
  }

  private def buildNestedConvertersArray(): Array[WireFormatConverter] = {
    val convertersArray = new Array[WireFormatConverter](maxFieldNumber + 1)

    var i = 0
    while (i < fieldMappingArray.length) {
      val mapping = fieldMappingArray(i)
      if (mapping != null && mapping.fieldDescriptor.getType == FieldDescriptor.Type.MESSAGE) {
        val nestedDescriptor = mapping.fieldDescriptor.getMessageType
        val nestedSchema = mapping.sparkDataType match {
          case struct: StructType => struct
          case arrayType: ArrayType =>
            arrayType.elementType.asInstanceOf[StructType]
          case _ => throw new IllegalArgumentException(s"Expected StructType or ArrayType[StructType] for message field ${mapping.fieldDescriptor.getName}")
        }
        convertersArray(i) = new WireFormatConverter(nestedDescriptor, nestedSchema)
      }
      i += 1
    }

    convertersArray
  }

  private def resetAccumulators(): Unit = {
    var i = 0
    while (i < fieldMappingArray.length) {
      val mapping = fieldMappingArray(i)
      if (mapping != null && mapping.accumulator != null) {
        mapping.accumulator match {
          case list: IntList => list.count = 0
          case list: LongList => list.count = 0
          case list: FloatList => list.count = 0
          case list: DoubleList => list.count = 0
          case list: BooleanList => list.count = 0
          case list: ByteArrayList => list.count = 0
        }
      }
      i += 1
    }
  }

  private def parseField(
      input: CodedInputStream,
      tag: Int,
      wireType: Int,
      mapping: FieldMapping,
      writer: UnsafeRowWriter): Unit = {
    import FieldDescriptor.Type._

    // Validate wire type matches expected type for this field
    val expectedWireType = getExpectedWireType(mapping.fieldDescriptor.getType)
    if (wireType != expectedWireType && !isValidWireTypeForField(wireType, mapping.fieldDescriptor.getType, mapping.isRepeated)) {
      // Wire type mismatch - skip this field to avoid parsing errors
      input.skipField(tag)
      return
    }

    if (mapping.isRepeated) {
      // For repeated fields, accumulate values using type-specific accumulators
      mapping.fieldDescriptor.getType match {
        // Variable-length int32 types
        case INT32 | UINT32 =>
          val list = mapping.accumulator.asInstanceOf[IntList]
          if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
            parsePackedVarint32s(input, list)
          } else {
            list.add(input.readRawVarint32())
          }

        case ENUM =>
          val list = mapping.accumulator.asInstanceOf[IntList]
          if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
            parsePackedVarint32s(input, list)
          } else {
            list.add(input.readEnum())
          }

        case SINT32 =>
          val list = mapping.accumulator.asInstanceOf[IntList]
          if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
            parsePackedSInt32s(input, list)
          } else {
            list.add(input.readSInt32())
          }

        // Variable-length int64 types
        case INT64 | UINT64 =>
          val list = mapping.accumulator.asInstanceOf[LongList]
          if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
            parsePackedVarint64s(input, list)
          } else {
            list.add(input.readRawVarint64())
          }

        case SINT64 =>
          val list = mapping.accumulator.asInstanceOf[LongList]
          if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
            parsePackedSInt64s(input, list)
          } else {
            list.add(input.readSInt64())
          }

        // Fixed-size int32 types
        case FIXED32 | SFIXED32 =>
          val list = mapping.accumulator.asInstanceOf[IntList]
          if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
            val packedLength = input.readRawVarint32()
            list.array = parsePackedFixed32s(input, list.array, list.count, packedLength)
            list.count += packedLength / 4
          } else {
            list.add(input.readRawLittleEndian32())
          }

        // Fixed-size int64 types
        case FIXED64 | SFIXED64 =>
          val list = mapping.accumulator.asInstanceOf[LongList]
          if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
            val packedLength = input.readRawVarint32()
            list.array = parsePackedFixed64s(input, list.array, list.count, packedLength)
            list.count += packedLength / 8
          } else {
            list.add(input.readRawLittleEndian64())
          }

        // Float type
        case FLOAT =>
          val list = mapping.accumulator.asInstanceOf[FloatList]
          if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
            val packedLength = input.readRawVarint32()
            list.array = parsePackedFloats(input, list.array, list.count, packedLength)
            list.count += packedLength / 4
          } else {
            list.add(input.readFloat())
          }

        // Double type
        case DOUBLE =>
          val list = mapping.accumulator.asInstanceOf[DoubleList]
          if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
            val packedLength = input.readRawVarint32()
            list.array = parsePackedDoubles(input, list.array, list.count, packedLength)
            list.count += packedLength / 8
          } else {
            list.add(input.readDouble())
          }

        // Boolean type
        case BOOL =>
          val list = mapping.accumulator.asInstanceOf[BooleanList]
          if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
            val packedLength = input.readRawVarint32()
            list.array = parsePackedBooleans(input, list.array, list.count, packedLength)
            list.count += packedLength
          } else {
            list.add(input.readBool())
          }

        // String/Bytes/Message types
        case STRING | BYTES | MESSAGE =>
          val list = mapping.accumulator.asInstanceOf[ByteArrayList]
          list.add(input.readByteArray())

        case GROUP =>
          throw new UnsupportedOperationException("GROUP type is deprecated and not supported")
      }
    } else {
      // Single field - write directly using existing logic
      mapping.fieldDescriptor.getType match {
        case DOUBLE =>
          writer.write(mapping.rowOrdinal, input.readDouble())
        case FLOAT =>
          writer.write(mapping.rowOrdinal, input.readFloat())
        case INT64 | UINT64 =>
          writer.write(mapping.rowOrdinal, input.readRawVarint64())
        case INT32 | UINT32 =>
          writer.write(mapping.rowOrdinal, input.readRawVarint32())
        case FIXED64 | SFIXED64 =>
          writer.write(mapping.rowOrdinal, input.readRawLittleEndian64())
        case FIXED32 | SFIXED32 =>
          writer.write(mapping.rowOrdinal, input.readRawLittleEndian32())
        case BOOL =>
          writer.write(mapping.rowOrdinal, input.readBool())
        case STRING =>
          val bytes = input.readByteArray()
          writer.write(mapping.rowOrdinal, UTF8String.fromBytes(bytes))
        case BYTES =>
          writer.write(mapping.rowOrdinal, input.readByteArray())
        case ENUM =>
          val enumValue = input.readEnum()
          val enumDescriptor = mapping.fieldDescriptor.getEnumType
          val enumValueDescriptor = enumDescriptor.findValueByNumber(enumValue)
          val enumName = if (enumValueDescriptor != null) enumValueDescriptor.getName else enumValue.toString
          writer.write(mapping.rowOrdinal, UTF8String.fromString(enumName))
        case SINT32 =>
          writer.write(mapping.rowOrdinal, input.readSInt32())
        case SINT64 =>
          writer.write(mapping.rowOrdinal, input.readSInt64())
        case MESSAGE =>
          parseNestedMessage(input, mapping, writer)
        case GROUP =>
          throw new UnsupportedOperationException("GROUP type is deprecated and not supported")
      }
    }
  }

  private def parseNestedMessage(
      input: CodedInputStream,
      mapping: FieldMapping,
      writer: UnsafeRowWriter): Unit = {
    val messageBytes = input.readByteArray()
    val converter = nestedConvertersArray(mapping.fieldDescriptor.getNumber)
    if (converter != null) {
      // Use helper method from AbstractWireFormatConverter
      writeMessage(messageBytes, mapping.rowOrdinal, converter, writer)
    } else {
      throw new IllegalStateException(s"No nested converter found for field ${mapping.fieldDescriptor.getName}")
    }
  }

  private def writeAccumulatedRepeatedFields(writer: UnsafeRowWriter): Unit = {
    var fieldNumber = 0
    while (fieldNumber < fieldMappingArray.length) {
      val mapping = fieldMappingArray(fieldNumber)
      if (mapping != null && mapping.isRepeated && mapping.accumulator != null) {
        mapping.accumulator match {
          case list: IntList if list.count > 0 =>
            // Handle enum conversion for ENUM fields
            if (mapping.fieldDescriptor.getType == FieldDescriptor.Type.ENUM) {
              writeEnumArray(list, mapping, writer)
            } else {
              writeIntArray(list.array, list.count, mapping.rowOrdinal, writer)
            }

          case list: LongList if list.count > 0 =>
            writeLongArray(list.array, list.count, mapping.rowOrdinal, writer)

          case list: FloatList if list.count > 0 =>
            writeFloatArray(list.array, list.count, mapping.rowOrdinal, writer)

          case list: DoubleList if list.count > 0 =>
            writeDoubleArray(list.array, list.count, mapping.rowOrdinal, writer)

          case list: BooleanList if list.count > 0 =>
            writeBooleanArray(list.array, list.count, mapping.rowOrdinal, writer)

          case list: ByteArrayList if list.count > 0 =>
            mapping.fieldDescriptor.getType match {
              case FieldDescriptor.Type.MESSAGE =>
                val converter = nestedConvertersArray(fieldNumber).asInstanceOf[AbstractWireFormatConverter]
                writeMessageArray(list.array, list.count, mapping.rowOrdinal, converter, writer)
              case FieldDescriptor.Type.STRING =>
                writeStringArray(list.array, list.count, mapping.rowOrdinal, writer)
              case FieldDescriptor.Type.BYTES =>
                writeBytesArray(list.array, list.count, mapping.rowOrdinal, writer)
              case _ =>
                throw new IllegalStateException(s"Unexpected field type ${mapping.fieldDescriptor.getType} for ByteArrayList")
            }

          case _ => // Empty lists or null - skip
        }
      }
      fieldNumber += 1
    }
  }

  private def writeEnumArray(list: IntList, mapping: FieldMapping, writer: UnsafeRowWriter): Unit = {
    // Convert enum values to string array
    val enumDescriptor = mapping.fieldDescriptor.getEnumType
    val stringBytes = new Array[Array[Byte]](list.count)

    var i = 0
    while (i < list.count) {
      val enumValue = list.array(i)
      val enumValueDescriptor = enumDescriptor.findValueByNumber(enumValue)
      val enumName = if (enumValueDescriptor != null) enumValueDescriptor.getName else enumValue.toString
      stringBytes(i) = enumName.getBytes("UTF-8")
      i += 1
    }

    writeStringArray(stringBytes, list.count, mapping.rowOrdinal, writer)
  }

  /**
   * Returns the expected wire type for a given protobuf field type.
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
      case GROUP => throw new UnsupportedOperationException("GROUP type is deprecated and not supported")
    }
  }

  /**
   * Checks if a wire type is valid for a field type, considering repeated fields and packed encoding.
   */
  private def isValidWireTypeForField(wireType: Int, fieldType: FieldDescriptor.Type, isRepeated: Boolean): Boolean = {
    val expectedWireType = getExpectedWireType(fieldType)

    if (wireType == expectedWireType) {
      return true
    }

    // For repeated fields, also allow LENGTH_DELIMITED for packed encoding
    if (isRepeated && wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED && isPackable(fieldType)) {
      return true
    }

    false
  }

  private def isPackable(fieldType: FieldDescriptor.Type): Boolean = {
    import FieldDescriptor.Type._
    fieldType match {
      case STRING | BYTES | MESSAGE | GROUP => false
      case _ => true
    }
  }
}

object WireFormatConverter {
  /**
   * Mapping information for a protobuf field to its corresponding Spark row position.
   */
  private case class FieldMapping(
      fieldDescriptor: FieldDescriptor,
      rowOrdinal: Int,
      sparkDataType: org.apache.spark.sql.types.DataType,
      isRepeated: Boolean,
      accumulator: Any)  // Direct list: IntList, LongList, FloatList, etc.
}