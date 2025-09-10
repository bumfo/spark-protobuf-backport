package fastproto

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.{CodedInputStream, WireFormat}
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeArrayWriter, UnsafeRowWriter}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * A high-performance converter that reads protobuf binary data directly from wire format
 * using CodedInputStream, bypassing the need to materialize intermediate Message objects.
 *
 * This converter provides significant performance improvements by:
 * - Eliminating Message object allocations
 * - Single-pass streaming parse to UnsafeRow
 * - Selective field parsing (skips unused fields)
 * - Direct byte copying for strings/bytes
 *
 * @param descriptor the protobuf message descriptor
 * @param schema     the corresponding Spark SQL schema
 */
class WireFormatConverter(
    descriptor: Descriptor,
    override val schema: StructType)
  extends AbstractRowConverter(schema) {

  import WireFormatConverter._

  // Build field number → row ordinal mapping during construction
  private val fieldMapping: Map[Int, FieldMapping] = buildFieldMapping()

  // Cache nested converters for message fields
  private val nestedConverters: Map[Int, WireFormatConverter] = buildNestedConverters()

  // Track repeated field values during parsing
  private val repeatedFieldValues: mutable.Map[Int, mutable.ArrayBuffer[Any]] = mutable.Map.empty

  override protected def writeData(binary: Array[Byte], writer: UnsafeRowWriter): Unit = {
    val input = CodedInputStream.newInstance(binary)

    // Clear repeated field buffers for this conversion
    repeatedFieldValues.clear()

    // Parse the wire format
    while (!input.isAtEnd()) {
      val tag = input.readTag()
      val fieldNumber = WireFormat.getTagFieldNumber(tag)
      val wireType = WireFormat.getTagWireType(tag)

      fieldMapping.get(fieldNumber) match {
        case Some(mapping) =>
          parseField(input, tag, wireType, mapping, writer)
        case None =>
          // Skip unknown fields
          input.skipField(tag)
      }
    }

    // Write accumulated repeated fields
    writeAccumulatedRepeatedFields(writer)
  }

  private def buildFieldMapping(): Map[Int, FieldMapping] = {
    val mapping = mutable.Map[Int, FieldMapping]()

    descriptor.getFields.asScala.foreach { field =>
      // Find corresponding field in Spark schema by name
      schema.fields.zipWithIndex.find(_._1.name == field.getName) match {
        case Some((sparkField, ordinal)) =>
          mapping(field.getNumber) = FieldMapping(
            fieldDescriptor = field,
            rowOrdinal = ordinal,
            sparkDataType = sparkField.dataType,
            isRepeated = field.isRepeated
          )
        case None =>
        // Field exists in protobuf but not in Spark schema - skip it
      }
    }

    mapping.toMap
  }

  private def buildNestedConverters(): Map[Int, WireFormatConverter] = {
    val converters = mutable.Map[Int, WireFormatConverter]()

    fieldMapping.foreach { case (fieldNumber, mapping) =>
      if (mapping.fieldDescriptor.getType == FieldDescriptor.Type.MESSAGE) {
        val nestedDescriptor = mapping.fieldDescriptor.getMessageType
        val nestedSchema = mapping.sparkDataType match {
          case struct: StructType => struct
          case arrayType: ArrayType =>
            arrayType.elementType.asInstanceOf[StructType]
          case _ => throw new IllegalArgumentException(s"Expected StructType or ArrayType[StructType] for message field ${mapping.fieldDescriptor.getName}")
        }
        converters(fieldNumber) = new WireFormatConverter(nestedDescriptor, nestedSchema)
      }
    }

    converters.toMap
  }

  private def parseField(
      input: CodedInputStream,
      tag: Int,
      wireType: Int,
      mapping: FieldMapping,
      writer: UnsafeRowWriter): Unit = {
    import FieldDescriptor.Type._

    if (mapping.isRepeated) {
      // For repeated fields, accumulate values
      val fieldNumber = mapping.fieldDescriptor.getNumber
      val values = repeatedFieldValues.getOrElseUpdate(fieldNumber, mutable.ArrayBuffer.empty[Any])

      // Handle packed repeated fields (length-delimited)
      if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED && isPackable(mapping.fieldDescriptor.getType)) {
        val length = input.readRawVarint32()
        val oldLimit = input.pushLimit(length)

        while (input.getBytesUntilLimit > 0) {
          values += readPackedValue(input, mapping.fieldDescriptor.getType)
        }

        input.popLimit(oldLimit)
      } else if (mapping.fieldDescriptor.getType == FieldDescriptor.Type.MESSAGE) {
        // Handle repeated MESSAGE fields directly to enable writer sharing
        val messageBytes = input.readBytes().toByteArray
        nestedConverters.get(fieldNumber) match {
          case Some(converter) =>
            // Store message bytes for later processing with writer sharing
            values += messageBytes
          case None =>
            throw new IllegalStateException(s"No nested converter found for field ${mapping.fieldDescriptor.getName}")
        }
      } else {
        // Non-packed repeated field - read single value
        values += readSingleValue(input, mapping.fieldDescriptor.getType, mapping)
      }
    } else {
      // Single field
      mapping.fieldDescriptor.getType match {
        case DOUBLE =>
          writer.write(mapping.rowOrdinal, input.readDouble())
        case FLOAT =>
          writer.write(mapping.rowOrdinal, input.readFloat())
        case INT64 =>
          writer.write(mapping.rowOrdinal, input.readInt64())
        case UINT64 =>
          writer.write(mapping.rowOrdinal, input.readUInt64())
        case INT32 =>
          writer.write(mapping.rowOrdinal, input.readInt32())
        case FIXED64 =>
          writer.write(mapping.rowOrdinal, input.readFixed64())
        case FIXED32 =>
          writer.write(mapping.rowOrdinal, input.readFixed32())
        case BOOL =>
          writer.write(mapping.rowOrdinal, input.readBool())
        case STRING =>
          val bytes = input.readBytes().toByteArray
          writer.write(mapping.rowOrdinal, UTF8String.fromBytes(bytes))
        case BYTES =>
          writer.write(mapping.rowOrdinal, input.readBytes().toByteArray)
        case UINT32 =>
          writer.write(mapping.rowOrdinal, input.readUInt32())
        case ENUM =>
          writer.write(mapping.rowOrdinal, input.readEnum())
        case SFIXED32 =>
          writer.write(mapping.rowOrdinal, input.readSFixed32())
        case SFIXED64 =>
          writer.write(mapping.rowOrdinal, input.readSFixed64())
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
    val messageBytes = input.readBytes().toByteArray
    nestedConverters.get(mapping.fieldDescriptor.getNumber) match {
      case Some(converter) =>
        // Use writer sharing pattern like ProtoToRowGenerator
        val offset = writer.cursor()
        converter.convert(messageBytes, writer)
        writer.setOffsetAndSizeFromPreviousCursor(mapping.rowOrdinal, offset)
      case None =>
        throw new IllegalStateException(s"No nested converter found for field ${mapping.fieldDescriptor.getName}")
    }
  }

  private def writeAccumulatedRepeatedFields(writer: UnsafeRowWriter): Unit = {
    repeatedFieldValues.foreach { case (fieldNumber, values) =>
      fieldMapping.get(fieldNumber) match {
        case Some(mapping) if mapping.isRepeated =>
          writeArray(values.toArray, mapping, writer)
        case _ => // Should not happen
      }
    }
  }

  private def isPackable(fieldType: FieldDescriptor.Type): Boolean = {
    import FieldDescriptor.Type._
    fieldType match {
      case STRING | BYTES | MESSAGE | GROUP => false
      case _ => true
    }
  }

  private def readPackedValue(input: CodedInputStream, fieldType: FieldDescriptor.Type): Any = {
    import FieldDescriptor.Type._
    fieldType match {
      case DOUBLE => input.readDouble()
      case FLOAT => input.readFloat()
      case INT64 => input.readInt64()
      case UINT64 => input.readUInt64()
      case INT32 => input.readInt32()
      case FIXED64 => input.readFixed64()
      case FIXED32 => input.readFixed32()
      case BOOL => input.readBool()
      case UINT32 => input.readUInt32()
      case ENUM => input.readEnum()
      case SFIXED32 => input.readSFixed32()
      case SFIXED64 => input.readSFixed64()
      case SINT32 => input.readSInt32()
      case SINT64 => input.readSInt64()
      case _ => throw new IllegalArgumentException(s"Type $fieldType is not packable")
    }
  }

  private def readSingleValue(input: CodedInputStream, fieldType: FieldDescriptor.Type, mapping: FieldMapping): Any = {
    import FieldDescriptor.Type._
    fieldType match {
      case DOUBLE => input.readDouble()
      case FLOAT => input.readFloat()
      case INT64 => input.readInt64()
      case UINT64 => input.readUInt64()
      case INT32 => input.readInt32()
      case FIXED64 => input.readFixed64()
      case FIXED32 => input.readFixed32()
      case BOOL => input.readBool()
      case STRING => UTF8String.fromBytes(input.readBytes().toByteArray)
      case BYTES => input.readBytes().toByteArray
      case UINT32 => input.readUInt32()
      case ENUM => input.readEnum()
      case SFIXED32 => input.readSFixed32()
      case SFIXED64 => input.readSFixed64()
      case SINT32 => input.readSInt32()
      case SINT64 => input.readSInt64()
      case MESSAGE =>
        throw new IllegalStateException("MESSAGE fields should be handled directly in parseField, not through readSingleValue")
      case GROUP => throw new UnsupportedOperationException("GROUP type is deprecated and not supported")
    }
  }

  private def writeArray(values: Array[Any], mapping: FieldMapping, writer: UnsafeRowWriter): Unit = {
    val elementSize = getElementSize(mapping.fieldDescriptor.getType)
    val offset = writer.cursor()
    val arrayWriter = new UnsafeArrayWriter(writer, elementSize)

    arrayWriter.initialize(values.length)

    if (mapping.fieldDescriptor.getType == FieldDescriptor.Type.MESSAGE) {
      // Handle nested messages with writer sharing
      val fieldNumber = mapping.fieldDescriptor.getNumber
      nestedConverters.get(fieldNumber) match {
        case Some(converter) =>
          for (i <- values.indices) {
            val messageBytes = values(i).asInstanceOf[Array[Byte]]
            val elemOffset = arrayWriter.cursor()
            converter.convert(messageBytes, writer)
            arrayWriter.setOffsetAndSizeFromPreviousCursor(i, elemOffset)
          }
        case None =>
          throw new IllegalStateException(s"No nested converter found for field ${mapping.fieldDescriptor.getName}")
      }
    } else {
      // Handle primitive types
      for (i <- values.indices) {
        writeArrayElement(arrayWriter, i, values(i), mapping.fieldDescriptor.getType)
      }
    }

    writer.setOffsetAndSizeFromPreviousCursor(mapping.rowOrdinal, offset)
  }

  private def getElementSize(fieldType: FieldDescriptor.Type): Int = {
    import FieldDescriptor.Type._
    fieldType match {
      case DOUBLE | INT64 | UINT64 | FIXED64 | SFIXED64 | SINT64 => 8
      case FLOAT | INT32 | UINT32 | FIXED32 | SFIXED32 | SINT32 | ENUM => 4
      case BOOL => 1
      case STRING | BYTES | MESSAGE => 8 // Variable length fields use 8 bytes for offset/size
      case GROUP => throw new UnsupportedOperationException("GROUP type is deprecated and not supported")
    }
  }

  private def writeArrayElement(arrayWriter: UnsafeArrayWriter, index: Int, value: Any, fieldType: FieldDescriptor.Type): Unit = {
    import FieldDescriptor.Type._
    fieldType match {
      case DOUBLE => arrayWriter.write(index, value.asInstanceOf[Double])
      case FLOAT => arrayWriter.write(index, value.asInstanceOf[Float])
      case INT64 => arrayWriter.write(index, value.asInstanceOf[Long])
      case UINT64 => arrayWriter.write(index, value.asInstanceOf[Long])
      case INT32 => arrayWriter.write(index, value.asInstanceOf[Int])
      case FIXED64 => arrayWriter.write(index, value.asInstanceOf[Long])
      case FIXED32 => arrayWriter.write(index, value.asInstanceOf[Int])
      case BOOL => arrayWriter.write(index, value.asInstanceOf[Boolean])
      case STRING => arrayWriter.write(index, value.asInstanceOf[UTF8String])
      case BYTES => arrayWriter.write(index, value.asInstanceOf[Array[Byte]])
      case UINT32 => arrayWriter.write(index, value.asInstanceOf[Int])
      case ENUM => arrayWriter.write(index, value.asInstanceOf[Int])
      case SFIXED32 => arrayWriter.write(index, value.asInstanceOf[Int])
      case SFIXED64 => arrayWriter.write(index, value.asInstanceOf[Long])
      case SINT32 => arrayWriter.write(index, value.asInstanceOf[Int])
      case SINT64 => arrayWriter.write(index, value.asInstanceOf[Long])
      case MESSAGE => throw new IllegalStateException("MESSAGE array elements should be handled directly in writeArray, not through writeArrayElement")
      case GROUP => throw new UnsupportedOperationException("GROUP type is deprecated and not supported")
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
      isRepeated: Boolean)
}