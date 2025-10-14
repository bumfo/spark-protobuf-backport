package fastproto

import com.google.protobuf.Descriptors.FieldDescriptor.Type
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.{CodedInputStream, WireFormat}
import fastproto.StreamWireParser._
import org.apache.spark.sql.types.{ArrayType, StructType}

import scala.collection.mutable

/**
 * A high-performance parser that reads protobuf binary data directly from wire format
 * using CodedInputStream, bypassing the need to materialize intermediate Message objects.
 *
 * This optimized parser provides significant performance improvements by:
 * - Extending StreamWireParser for optimized helper methods
 * - Using type-specific primitive accumulators (no boxing)
 * - Leveraging packed field parsing methods from StreamWireParser
 * - Direct byte copying for strings/bytes/messages
 * - Single-pass streaming parse to UnsafeRow
 *
 * @param descriptor the protobuf message descriptor
 * @param schema     the corresponding Spark SQL schema
 */
class WireFormatParser(
    descriptor: Descriptor,
    override val schema: StructType,
    isRecursive: Boolean = false,
    nestedParsersArrayOpt: Option[Array[WireFormatParser.ParserRef]] = None)
  extends StreamWireParser(schema) {

  import WireFormatParser._

  // Build field mappings using primitive arrays for better cache locality
  private val maxFieldNumber = if (descriptor.getFields.isEmpty) 0 else {
    var max = 0
    val fields = descriptor.getFields
    var i = 0
    while (i < fields.size()) {
      val fieldNum = fields.get(i).getNumber
      if (fieldNum > max) max = fieldNum
      i += 1
    }
    max
  }

  // Primitive arrays for hot path data - better cache locality
  private val rowOrdinals = new Array[Int](maxFieldNumber + 1)
  private val fieldTypes = new Array[Type](maxFieldNumber + 1)
  private val fieldWireTypes = new Array[Byte](maxFieldNumber + 1)
  private val isRepeatedFlags = new Array[Boolean](maxFieldNumber + 1)
  private val fieldDescriptors = new Array[FieldDescriptor](maxFieldNumber + 1)

  // Initialize arrays to -1 for unset fields
  java.util.Arrays.fill(rowOrdinals, -1)

  // Build field mappings in single pass
  locally {
    buildFieldMappings(descriptor, schema)
  }

  private def buildFieldMappings(descriptor: Descriptor, schema: StructType): Unit = {
    // Pre-compute schema field name to ordinal mapping for O(1) lookups
    val schemaFieldMap = {
      val map = new java.util.HashMap[String, Integer]()
      var i = 0
      while (i < schema.fields.length) {
        map.put(schema.fields(i).name, i)
        i += 1
      }
      map
    }

    // Single pass through descriptor fields
    val fields = descriptor.getFields
    var i = 0
    while (i < fields.size()) {
      val field = fields.get(i)
      val fieldNum = field.getNumber

      // Find corresponding field in Spark schema by name
      // GROUP fields won't be in schema (filtered by SchemaConverters), so ordinalObj will be null
      val ordinalObj = schemaFieldMap.get(field.getName)
      if (ordinalObj ne null) {
        val ordinal = ordinalObj.intValue()

        // Populate primitive arrays
        rowOrdinals(fieldNum) = ordinal
        fieldTypes(fieldNum) = field.getType
        fieldWireTypes(fieldNum) = getExpectedWireType(field.getType).toByte
        isRepeatedFlags(fieldNum) = field.isRepeated
        fieldDescriptors(fieldNum) = field
      }
      // Field exists in protobuf but not in Spark schema - skip it
      i += 1
    }
  }

  // Cache nested parsers for message fields - use array for O(1) lookup
  private val nestedParsersArray: Array[ParserRef] =
    nestedParsersArrayOpt.getOrElse(buildOptimizedNestedParsers(descriptor, schema))

  // Instance-level ParseState cannot be reused for PrimitiveArrayWriter (needs writer reference)
  // Always create new ParseState per parse
  override protected def parseInto(input: CodedInputStream, writer: RowWriter): Unit = {
    val state = new ParseState(maxFieldNumber, writer)
    parseIntoWithState(input, writer, state)
  }

  private def parseIntoWithState(input: CodedInputStream, writer: RowWriter, state: ParseState): Unit = {
    // Parse the wire format
    while (!input.isAtEnd) {
      val tag = input.readTag()
      val fieldNumber = WireFormat.getTagFieldNumber(tag)
      val wireType = WireFormat.getTagWireType(tag)

      if (fieldNumber <= maxFieldNumber && rowOrdinals(fieldNumber) >= 0) {
        parseFieldWithState(input, tag, wireType, fieldNumber, writer, state)
      } else {
        // Skip unknown fields
        input.skipField(tag)
      }
    }

    // Write accumulated repeated fields
    writeAccumulatedRepeatedFields(writer, state)
  }


  private def parseFieldWithState(
      input: CodedInputStream,
      tag: Int,
      wireType: Int,
      fieldNumber: Int,
      writer: RowWriter,
      state: ParseState): Unit = {

    val expect = fieldWireTypes(fieldNumber)
    if (isRepeatedFlags(fieldNumber)) {
      if (wireType == expect) {
        parseUnpackedRepeatedField(input, fieldNumber, state)
      } else if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED && isPackable(fieldTypes(fieldNumber))) {
        parsePackedRepeatedField(input, fieldNumber, state)
      } else {
        input.skipField(tag)
      }
    } else {
      if (wireType == expect) {
        parseSingleField(input, fieldNumber, writer, state)
      } else {
        input.skipField(tag)
      }
    }
  }

  private def parsePackedRepeatedField(
      input: CodedInputStream,
      fieldNumber: Int,
      state: ParseState): Unit = {
    import FieldDescriptor.Type._
    val fieldType = fieldTypes(fieldNumber)

    // Packed repeated fields - wire type is always LENGTH_DELIMITED
    fieldType match {
      // Variable-length int32 types
      case INT32 | UINT32 =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[IntList]
        parsePackedVarint32s(input, list)

      case ENUM =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[IntList]
        parsePackedVarint32s(input, list)

      case SINT32 =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[IntList]
        parsePackedSInt32s(input, list)

      // Variable-length int64 types
      case INT64 | UINT64 =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[LongList]
        parsePackedVarint64s(input, list)

      case SINT64 =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[LongList]
        parsePackedSInt64s(input, list)

      // Fixed-size int32 types
      case FIXED32 | SFIXED32 =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[IntList]
        val packedLength = input.readRawVarint32()
        list.array = parsePackedFixed32s(input, list.array, list.count, packedLength)
        list.count += packedLength / 4

      // Fixed-size int64 types
      case FIXED64 | SFIXED64 =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[LongList]
        val packedLength = input.readRawVarint32()
        list.array = parsePackedFixed64s(input, list.array, list.count, packedLength)
        list.count += packedLength / 8

      // Float type
      case FLOAT =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[FloatList]
        val packedLength = input.readRawVarint32()
        list.array = parsePackedFloats(input, list.array, list.count, packedLength)
        list.count += packedLength / 4

      // Double type
      case DOUBLE =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[DoubleList]
        val packedLength = input.readRawVarint32()
        list.array = parsePackedDoubles(input, list.array, list.count, packedLength)
        list.count += packedLength / 8

      // Boolean type
      case BOOL =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[BooleanList]
        val packedLength = input.readRawVarint32()
        list.array = parsePackedBooleans(input, list.array, list.count, packedLength)
        list.count += packedLength

      case _ =>
        throw new UnsupportedOperationException(s"Field type $fieldType is not packable")
    }
  }

  private def parseUnpackedRepeatedField(
      input: CodedInputStream,
      fieldNumber: Int,
      state: ParseState): Unit = {
    import FieldDescriptor.Type._
    val fieldType = fieldTypes(fieldNumber)

    // Unpacked repeated fields - individual values with expected wire types
    fieldType match {
      // Primitive types: INT32/64, FLOAT, DOUBLE, BOOL - use PrimitiveArrayWriter
      // Variable-length int32 types
      case INT32 | UINT32 =>
        parseUnpackedPrimitiveInt32(input, fieldNumber, fieldType, state, input.readRawVarint32())

      case ENUM =>
        parseUnpackedPrimitiveInt32(input, fieldNumber, fieldType, state, input.readEnum())

      case SINT32 =>
        parseUnpackedPrimitiveInt32(input, fieldNumber, fieldType, state, input.readSInt32())

      // Variable-length int64 types
      case INT64 | UINT64 =>
        parseUnpackedPrimitiveInt64(input, fieldNumber, fieldType, state, input.readRawVarint64())

      case SINT64 =>
        parseUnpackedPrimitiveInt64(input, fieldNumber, fieldType, state, input.readSInt64())

      // Fixed-size int32 types
      case FIXED32 | SFIXED32 =>
        parseUnpackedPrimitiveInt32(input, fieldNumber, fieldType, state, input.readRawLittleEndian32())

      // Fixed-size int64 types
      case FIXED64 | SFIXED64 =>
        parseUnpackedPrimitiveInt64(input, fieldNumber, fieldType, state, input.readRawLittleEndian64())

      // Float type
      case FLOAT =>
        parseUnpackedPrimitiveFloat(input, fieldNumber, fieldType, state, input.readFloat())

      // Double type
      case DOUBLE =>
        parseUnpackedPrimitiveDouble(input, fieldNumber, fieldType, state, input.readDouble())

      // Boolean type
      case BOOL =>
        parseUnpackedPrimitiveBool(input, fieldNumber, fieldType, state, input.readBool())

      // String/Bytes types - variable-length, use FastList directly
      case STRING | BYTES =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[BytesList]
        list.add(input.readByteArray())

      // Message types - variable-length, use FastList directly
      case MESSAGE =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[BufferList]
        list.add(input.readByteBuffer())

      case GROUP =>
        throw new UnsupportedOperationException("GROUP type is deprecated and not supported")
    }
  }

  // Helper methods for unpacked primitive repeated fields with PrimitiveArrayWriter state machine

  private def parseUnpackedPrimitiveInt32(
      input: CodedInputStream,
      fieldNumber: Int,
      fieldType: FieldDescriptor.Type,
      state: ParseState,
      value: Int): Unit = {

    val currentState = state.getFieldState(fieldNumber)

    if (currentState == 3) { // STATE_FALLBACK
      // Use FastList
      val list = state.getAccumulator(fieldNumber).asInstanceOf[IntList]
      list.add(value)

    } else if (currentState == 2) { // STATE_COMPLETED
      // Interleaving detected! Convert to fallback
      convertToFallback(fieldNumber, fieldType, state)
      val list = state.getAccumulator(fieldNumber).asInstanceOf[IntList]
      list.add(value)

    } else {
      // Optimistic path: PrimitiveArrayWriter
      if (state.getActiveWriterField != fieldNumber) {
        // Switching repeated fields
        if (state.getActiveWriterField != -1) {
          completePrimitiveArrayWriter(state.getActiveWriterField, state)
        }

        if (currentState == 0) { // STATE_UNUSED
          state.setAccumulator(fieldNumber, new PrimitiveArrayWriter(state.getWriter.toUnsafeWriter, 4, 0))
        }
        state.setActiveWriterField(fieldNumber)
        state.setFieldState(fieldNumber, 1) // STATE_ACTIVE
      }

      // Write to active PrimitiveArrayWriter
      val pw = state.getAccumulator(fieldNumber).asInstanceOf[PrimitiveArrayWriter]
      pw.writeInt(value)
    }
  }

  private def parseUnpackedPrimitiveInt64(
      input: CodedInputStream,
      fieldNumber: Int,
      fieldType: FieldDescriptor.Type,
      state: ParseState,
      value: Long): Unit = {

    val currentState = state.getFieldState(fieldNumber)

    if (currentState == 3) { // STATE_FALLBACK
      val list = state.getAccumulator(fieldNumber).asInstanceOf[LongList]
      list.add(value)

    } else if (currentState == 2) { // STATE_COMPLETED
      convertToFallback(fieldNumber, fieldType, state)
      val list = state.getAccumulator(fieldNumber).asInstanceOf[LongList]
      list.add(value)

    } else {
      if (state.getActiveWriterField != fieldNumber) {
        if (state.getActiveWriterField != -1) {
          completePrimitiveArrayWriter(state.getActiveWriterField, state)
        }
        if (currentState == 0) { // STATE_UNUSED
          state.setAccumulator(fieldNumber, new PrimitiveArrayWriter(state.getWriter.toUnsafeWriter, 8, 0))
        }
        state.setActiveWriterField(fieldNumber)
        state.setFieldState(fieldNumber, 1) // STATE_ACTIVE
      }

      val pw = state.getAccumulator(fieldNumber).asInstanceOf[PrimitiveArrayWriter]
      pw.writeLong(value)
    }
  }

  private def parseUnpackedPrimitiveFloat(
      input: CodedInputStream,
      fieldNumber: Int,
      fieldType: FieldDescriptor.Type,
      state: ParseState,
      value: Float): Unit = {

    val currentState = state.getFieldState(fieldNumber)

    if (currentState == 3) { // STATE_FALLBACK
      val list = state.getAccumulator(fieldNumber).asInstanceOf[FloatList]
      list.add(value)

    } else if (currentState == 2) { // STATE_COMPLETED
      convertToFallback(fieldNumber, fieldType, state)
      val list = state.getAccumulator(fieldNumber).asInstanceOf[FloatList]
      list.add(value)

    } else {
      if (state.getActiveWriterField != fieldNumber) {
        if (state.getActiveWriterField != -1) {
          completePrimitiveArrayWriter(state.getActiveWriterField, state)
        }
        if (currentState == 0) { // STATE_UNUSED
          state.setAccumulator(fieldNumber, new PrimitiveArrayWriter(state.getWriter.toUnsafeWriter, 4, 0))
        }
        state.setActiveWriterField(fieldNumber)
        state.setFieldState(fieldNumber, 1) // STATE_ACTIVE
      }

      val pw = state.getAccumulator(fieldNumber).asInstanceOf[PrimitiveArrayWriter]
      pw.writeFloat(value)
    }
  }

  private def parseUnpackedPrimitiveDouble(
      input: CodedInputStream,
      fieldNumber: Int,
      fieldType: FieldDescriptor.Type,
      state: ParseState,
      value: Double): Unit = {

    val currentState = state.getFieldState(fieldNumber)

    if (currentState == 3) { // STATE_FALLBACK
      val list = state.getAccumulator(fieldNumber).asInstanceOf[DoubleList]
      list.add(value)

    } else if (currentState == 2) { // STATE_COMPLETED
      convertToFallback(fieldNumber, fieldType, state)
      val list = state.getAccumulator(fieldNumber).asInstanceOf[DoubleList]
      list.add(value)

    } else {
      if (state.getActiveWriterField != fieldNumber) {
        if (state.getActiveWriterField != -1) {
          completePrimitiveArrayWriter(state.getActiveWriterField, state)
        }
        if (currentState == 0) { // STATE_UNUSED
          state.setAccumulator(fieldNumber, new PrimitiveArrayWriter(state.getWriter.toUnsafeWriter, 8, 0))
        }
        state.setActiveWriterField(fieldNumber)
        state.setFieldState(fieldNumber, 1) // STATE_ACTIVE
      }

      val pw = state.getAccumulator(fieldNumber).asInstanceOf[PrimitiveArrayWriter]
      pw.writeDouble(value)
    }
  }

  private def parseUnpackedPrimitiveBool(
      input: CodedInputStream,
      fieldNumber: Int,
      fieldType: FieldDescriptor.Type,
      state: ParseState,
      value: Boolean): Unit = {

    val currentState = state.getFieldState(fieldNumber)

    if (currentState == 3) { // STATE_FALLBACK
      val list = state.getAccumulator(fieldNumber).asInstanceOf[BooleanList]
      list.add(value)

    } else if (currentState == 2) { // STATE_COMPLETED
      convertToFallback(fieldNumber, fieldType, state)
      val list = state.getAccumulator(fieldNumber).asInstanceOf[BooleanList]
      list.add(value)

    } else {
      if (state.getActiveWriterField != fieldNumber) {
        if (state.getActiveWriterField != -1) {
          completePrimitiveArrayWriter(state.getActiveWriterField, state)
        }
        if (currentState == 0) { // STATE_UNUSED
          state.setAccumulator(fieldNumber, new PrimitiveArrayWriter(state.getWriter.toUnsafeWriter, 1, 0))
        }
        state.setActiveWriterField(fieldNumber)
        state.setFieldState(fieldNumber, 1) // STATE_ACTIVE
      }

      val pw = state.getAccumulator(fieldNumber).asInstanceOf[PrimitiveArrayWriter]
      pw.writeBoolean(value)
    }
  }


  private def parseSingleField(
      input: CodedInputStream,
      fieldNumber: Int,
      writer: RowWriter,
      state: ParseState): Unit = {
    import FieldDescriptor.Type._

    val rowOrdinal = rowOrdinals(fieldNumber)
    val fieldType = fieldTypes(fieldNumber)

    // Complete active PrimitiveArrayWriter if this is a variable-length field
    // Variable-length writes grow the buffer and invalidate PrimitiveArrayWriter's writePosition
    if (state.getActiveWriterField != -1 && isVariableLengthType(fieldType)) {
      completePrimitiveArrayWriter(state.getActiveWriterField, state)
    }

    // Convert raw values based on field type
    fieldType match {
      case INT64 | UINT64 =>
        writer.write(rowOrdinal, input.readRawVarint64)
      case INT32 | UINT32 | ENUM =>
        writer.write(rowOrdinal, input.readRawVarint32)
      case DOUBLE =>
        writer.write(rowOrdinal, java.lang.Double.longBitsToDouble(input.readRawLittleEndian64))
      case FLOAT =>
        writer.write(rowOrdinal, java.lang.Float.intBitsToFloat(input.readRawLittleEndian32()))
      case FIXED64 | SFIXED64 =>
        writer.write(rowOrdinal, input.readRawLittleEndian64)
      case FIXED32 | SFIXED32 =>
        writer.write(rowOrdinal, input.readRawLittleEndian32)
      case BOOL =>
        writer.write(rowOrdinal, input.readRawVarint64 != 0)
      case BYTES | STRING =>
        writer.writeBytes(rowOrdinal, input.readByteArray)
      case MESSAGE =>
        writeNestedMessage(input, fieldNumber, writer)
      case SINT32 =>
        writer.write(rowOrdinal, CodedInputStream.decodeZigZag32(input.readRawVarint32))
      case SINT64 =>
        writer.write(rowOrdinal, CodedInputStream.decodeZigZag64(input.readRawVarint64))
      case GROUP =>
        throw new UnsupportedOperationException("GROUP type is deprecated and not supported")
    }
  }

  private def isVariableLengthType(fieldType: FieldDescriptor.Type): Boolean = {
    import FieldDescriptor.Type._
    fieldType match {
      case STRING | BYTES | MESSAGE => true
      case _ => false
    }
  }

  /**
   * Complete the active PrimitiveArrayWriter by calling complete() and marking it as completed.
   */
  private def completePrimitiveArrayWriter(fieldNumber: Int, state: ParseState): Unit = {
    val acc = state.getAccumulator(fieldNumber)
    if (acc.isInstanceOf[PrimitiveArrayWriter]) {
      acc.asInstanceOf[PrimitiveArrayWriter].complete()
      state.setFieldState(fieldNumber, 2) // STATE_COMPLETED
      state.setActiveWriterField(-1)
    }
  }

  /**
   * Convert a completed PrimitiveArrayWriter to FastList due to detected interleaving.
   * Extracts values from the buffer and creates appropriate FastList.
   */
  private def convertToFallback(fieldNumber: Int, fieldType: FieldDescriptor.Type, state: ParseState): Unit = {
    import FieldDescriptor.Type._

    val pw = state.getAccumulator(fieldNumber).asInstanceOf[PrimitiveArrayWriter]
    val count = pw.size()

    if (count == 0) {
      // Empty writer, just create empty FastList
      val fallbackList = fieldType match {
        case INT32 | SINT32 | UINT32 | ENUM | FIXED32 | SFIXED32 => new IntList()
        case INT64 | SINT64 | UINT64 | FIXED64 | SFIXED64 => new LongList()
        case FLOAT => new FloatList()
        case DOUBLE => new DoubleList()
        case BOOL => new BooleanList()
        case _ => throw new IllegalStateException(s"Unexpected field type for PrimitiveArrayWriter: $fieldType")
      }
      state.setAccumulator(fieldNumber, fallbackList)
      state.setFieldState(fieldNumber, 3) // STATE_FALLBACK
      return
    }

    // Extract values from buffer
    val writer = state.getWriter
    val buffer = writer.toUnsafeWriter.getBuffer
    val dataOffset = pw.getDataOffset()

    fieldType match {
      case INT32 | SINT32 | UINT32 | ENUM | FIXED32 | SFIXED32 =>
        val list = new IntList()
        var i = 0
        while (i < count) {
          list.add(org.apache.spark.unsafe.Platform.getInt(buffer, dataOffset + i * 4))
          i += 1
        }
        state.setAccumulator(fieldNumber, list)

      case INT64 | SINT64 | UINT64 | FIXED64 | SFIXED64 =>
        val list = new LongList()
        var i = 0
        while (i < count) {
          list.add(org.apache.spark.unsafe.Platform.getLong(buffer, dataOffset + i * 8))
          i += 1
        }
        state.setAccumulator(fieldNumber, list)

      case FLOAT =>
        val list = new FloatList()
        var i = 0
        while (i < count) {
          list.add(org.apache.spark.unsafe.Platform.getFloat(buffer, dataOffset + i * 4))
          i += 1
        }
        state.setAccumulator(fieldNumber, list)

      case DOUBLE =>
        val list = new DoubleList()
        var i = 0
        while (i < count) {
          list.add(org.apache.spark.unsafe.Platform.getDouble(buffer, dataOffset + i * 8))
          i += 1
        }
        state.setAccumulator(fieldNumber, list)

      case BOOL =>
        val list = new BooleanList()
        var i = 0
        while (i < count) {
          list.add(org.apache.spark.unsafe.Platform.getBoolean(buffer, dataOffset + i))
          i += 1
        }
        state.setAccumulator(fieldNumber, list)

      case _ =>
        throw new IllegalStateException(s"Unexpected field type for PrimitiveArrayWriter: $fieldType")
    }

    state.setFieldState(fieldNumber, 3) // STATE_FALLBACK
  }

  private def writeNestedMessage(
      input: CodedInputStream,
      fieldNumber: Int,
      writer: RowWriter): Unit = {
    val parserRef = nestedParsersArray(fieldNumber)
    if ((parserRef ne null) && (parserRef.parser ne null)) {
      val parser = parserRef.parser
      val rowOrdinal = rowOrdinals(fieldNumber)

      val offset = writer.cursor
      parseNestedMessage(input, parser, writer.toUnsafeWriter)
      writer.writeVariableField(rowOrdinal, offset)
    } else {
      // Skip the message if no parser is available (field not in schema)
      input.skipRawBytes(input.readRawVarint32)
    }
  }

  private def writeAccumulatedRepeatedFields(writer: RowWriter, state: ParseState): Unit = {
    import FieldDescriptor.Type._

    // Complete any active PrimitiveArrayWriter first
    if (state.getActiveWriterField != -1) {
      completePrimitiveArrayWriter(state.getActiveWriterField, state)
    }

    var fieldNumber = 0
    while (fieldNumber <= maxFieldNumber) {
      if (rowOrdinals(fieldNumber) >= 0 && isRepeatedFlags(fieldNumber)) {
        val accumulator = state.getAccumulator(fieldNumber)
        if (accumulator ne null) {
          val rowOrdinal = rowOrdinals(fieldNumber)
          val fieldType = fieldTypes(fieldNumber)
          val fieldState = state.getFieldState(fieldNumber)

          // Check if this is a completed PrimitiveArrayWriter
          if (fieldState == 2 && accumulator.isInstanceOf[PrimitiveArrayWriter]) { // STATE_COMPLETED
            // PrimitiveArrayWriter - already completed, just write offset
            val pw = accumulator.asInstanceOf[PrimitiveArrayWriter]
            if (pw.size() > 0) {
              writer.writeVariableField(rowOrdinal, pw.getStartingOffset)
            }

          } else {
            // FastList fallback path
            accumulator match {
              case list: IntList if list.count > 0 =>
                writeIntArray(list.array, list.count, rowOrdinal, writer)

              case list: LongList if list.count > 0 =>
                writeLongArray(list.array, list.count, rowOrdinal, writer)

              case list: FloatList if list.count > 0 =>
                writeFloatArray(list.array, list.count, rowOrdinal, writer)

              case list: DoubleList if list.count > 0 =>
                writeDoubleArray(list.array, list.count, rowOrdinal, writer)

              case list: BooleanList if list.count > 0 =>
                writeBooleanArray(list.array, list.count, rowOrdinal, writer)

              case list: BytesList if list.count > 0 =>
                fieldType match {
                  case BYTES | STRING =>
                    writeBytesArray(list.array, list.count, rowOrdinal, writer)
                  case _ =>
                    throw new IllegalStateException(s"Unexpected field type $fieldType for BytesList")
                }

              case list: BufferList if list.count > 0 =>
                val parserRef = nestedParsersArray(fieldNumber)
                if ((parserRef ne null) && (parserRef.parser ne null)) {
                  val parser = parserRef.parser
                  writeMessageArrayFromBuffers(list.array, list.count, rowOrdinal, parser, writer)
                }

              case _ => // Empty lists, PrimitiveArrayWriter with size 0, or null - skip
            }
          }
        }
      }
      fieldNumber += 1
    }
  }

  private def writeEnumArray(list: IntList, fieldNumber: Int, writer: RowWriter): Unit = {
    // Convert enum values to string array
    val enumDescriptor = fieldDescriptors(fieldNumber).getEnumType
    val rowOrdinal = rowOrdinals(fieldNumber)
    val stringBytes = new Array[Array[Byte]](list.count)

    var i = 0
    while (i < list.count) {
      val enumValue = list.array(i)
      val enumValueDescriptor = enumDescriptor.findValueByNumber(enumValue)
      val enumName = if (enumValueDescriptor ne null) enumValueDescriptor.getName else enumValue.toString
      stringBytes(i) = enumName.getBytes("UTF-8")
      i += 1
    }

    writeStringArray(stringBytes, list.count, rowOrdinal, writer)
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
      // case GROUP => throw new UnsupportedOperationException("GROUP type is deprecated and not supported")
      case _ => -1
    }
  }

  private def isPackable(fieldType: FieldDescriptor.Type): Boolean = {
    import FieldDescriptor.Type._
    fieldType match {
      case STRING | BYTES | MESSAGE | GROUP => false
      case _ => true
    }
  }

}

object WireFormatParser {

  /**
   * Parse state that holds all accumulators for a single parse operation.
   * This ensures thread-safe recursive parsing by isolating state per call.
   *
   * Supports optimistic PrimitiveArrayWriter with automatic fallback to FastList
   * when interleaving is detected.
   */
  private class ParseState(maxFieldNumber: Int, writer: RowWriter) {
    // Accumulators: PrimitiveArrayWriter (optimistic) or FastList (fallback)
    private val accumulators = new Array[AnyRef](maxFieldNumber + 1)

    // Field state: 0=unused, 1=active, 2=completed, 3=fallback
    private val fieldState = new Array[Byte](maxFieldNumber + 1)

    // Currently active repeated field with PrimitiveArrayWriter (-1 = none)
    private var activeWriterField = -1

    // Reference to RowWriter for creating PrimitiveArrayWriter instances
    private val rowWriter = writer

    // State constants
    private val STATE_UNUSED: Byte = 0
    private val STATE_ACTIVE: Byte = 1
    private val STATE_COMPLETED: Byte = 2
    private val STATE_FALLBACK: Byte = 3

    def getOrCreateAccumulator(fieldNumber: Int, fieldType: FieldDescriptor.Type): AnyRef = {
      import FieldDescriptor.Type._
      if (accumulators(fieldNumber) eq null) {
        accumulators(fieldNumber) = fieldType match {
          case INT32 | SINT32 | UINT32 | ENUM | FIXED32 | SFIXED32 => new IntList()
          case INT64 | SINT64 | UINT64 | FIXED64 | SFIXED64 => new LongList()
          case FLOAT => new FloatList()
          case DOUBLE => new DoubleList()
          case BOOL => new BooleanList()
          case STRING | BYTES => new BytesList()
          case MESSAGE => new BufferList()
          case GROUP => throw new UnsupportedOperationException("GROUP type is deprecated and not supported")
        }
      }
      accumulators(fieldNumber)
    }

    def getAccumulator(fieldNumber: Int): AnyRef = {
      accumulators(fieldNumber)
    }

    def setAccumulator(fieldNumber: Int, accumulator: AnyRef): Unit = {
      accumulators(fieldNumber) = accumulator
    }

    def getFieldState(fieldNumber: Int): Byte = {
      fieldState(fieldNumber)
    }

    def setFieldState(fieldNumber: Int, state: Byte): Unit = {
      fieldState(fieldNumber) = state
    }

    def getActiveWriterField: Int = activeWriterField

    def setActiveWriterField(fieldNumber: Int): Unit = {
      activeWriterField = fieldNumber
    }

    def getWriter: RowWriter = rowWriter

    def reset(): Unit = {
      var i = 0
      while (i <= maxFieldNumber) {
        val acc = accumulators(i)
        if (acc ne null) {
          acc match {
            case list: FastList => list.reset()
            case _ => // PrimitiveArrayWriter instances are not reused
          }
          accumulators(i) = null
        }
        fieldState(i) = STATE_UNUSED
        i += 1
      }
      activeWriterField = -1
    }
  }

  private val threadVisited = ThreadLocal.withInitial(() => mutable.HashMap[(String, Boolean, StructType), ParserRef]())

  /**
   * Smart construction that detects recursion and optimizes entire parser tree.
   * Uses ThreadLocal caching to reuse visited parser references within the same thread,
   * enabling efficient cycle detection and parser reuse for recursive message structures.
   *
   * @param descriptor the protobuf message descriptor
   * @param schema     the corresponding Spark SQL schema
   * @return optimized WireFormatParser with recursion detection
   */
  def apply(descriptor: Descriptor, schema: StructType): WireFormatParser =
    buildOptimizedParser(descriptor, schema, isRecursive = false, threadVisited.get()).parser

  private def buildOptimizedParser(
      descriptor: Descriptor,
      schema: StructType,
      isRecursive: Boolean,
      visited: mutable.HashMap[(String, Boolean, StructType), ParserRef]): ParserRef = {

    val key = (descriptor.getFullName, isRecursive, schema)

    // Return existing ParserRef if already built (handles cycles)
    visited.get(key) match {
      case Some(ref) if ref.parser == null && !isRecursive =>
        // Cycle detected on non-recursive path - switch to recursive
        return buildOptimizedParser(descriptor, schema, isRecursive = true, visited)
      case Some(ref) => return ref
      case None =>
    }

    // Mark as being built with placeholder ParserRef
    val ref = ParserRef(null)
    visited(key) = ref

    // Build nested parsers array first
    val nestedParsers = buildOptimizedNestedParsers(descriptor, schema, isRecursive, visited)

    // Create parser with pre-built nested parsers array
    val parser = new WireFormatParser(descriptor, schema, isRecursive, Some(nestedParsers))

    // Update the ParserRef
    ref.parser = parser

    ref
  }

  private def buildOptimizedNestedParsers(
      descriptor: Descriptor,
      schema: StructType,
      isRecursive: Boolean = false,
      visited: mutable.HashMap[(String, Boolean, StructType), ParserRef] = mutable.HashMap()): Array[ParserRef] = {

    // Calculate max field number
    var maxFieldNum = 0
    val fields = descriptor.getFields
    var i = 0
    while (i < fields.size()) {
      val fieldNum = fields.get(i).getNumber
      if (fieldNum > maxFieldNum) maxFieldNum = fieldNum
      i += 1
    }

    val parsersArray = new Array[ParserRef](maxFieldNum + 1)

    // Pre-compute schema field name to index mapping
    val schemaFieldMap = {
      val map = new java.util.HashMap[String, (org.apache.spark.sql.types.DataType, Int)]()
      var idx = 0
      while (idx < schema.fields.length) {
        val field = schema.fields(idx)
        map.put(field.name, (field.dataType, idx))
        idx += 1
      }
      map
    }

    // Build parsers for message fields
    // Note: GROUP fields have JavaType.MESSAGE but Type.GROUP, so Type.MESSAGE check excludes them
    i = 0
    while (i < fields.size()) {
      val field = fields.get(i)
      if (field.getType == FieldDescriptor.Type.MESSAGE) {
        val fieldMapping = schemaFieldMap.get(field.getName)
        if (fieldMapping ne null) {
          val (sparkDataType, _) = fieldMapping
          val nestedDescriptor = field.getMessageType

          val nestedSchema = sparkDataType match {
            case struct: StructType => struct
            case arrayType: ArrayType =>
              arrayType.elementType.asInstanceOf[StructType]
            case _ => throw new IllegalArgumentException(s"Expected StructType or ArrayType[StructType] for message field ${field.getName}")
          }

          parsersArray(field.getNumber) = buildOptimizedParser(nestedDescriptor, nestedSchema, isRecursive, visited)
        }
      }
      i += 1
    }

    parsersArray
  }

  case class ParserRef(var parser: WireFormatParser)

  private val TAG_TYPE_BITS = 3
  private val TAG_TYPE_MASK = (1 << TAG_TYPE_BITS) - 1

}