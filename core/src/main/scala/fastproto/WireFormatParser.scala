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
 * TODO support skipping GROUP wire type by default
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
      val ordinalObj = schemaFieldMap.get(field.getName)
      if (ordinalObj ne null) {
        val ordinal = ordinalObj.intValue()

        // Populate primitive arrays
        rowOrdinals(fieldNum) = ordinal
        fieldTypes(fieldNum) = field.getType
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

  // Instance-level ParseState - reuse to avoid allocation on each parse (only for non-recursive types)
  private val instanceParseState: ParseState =
    if (!isRecursive) new ParseState(maxFieldNumber) else null


  override protected def parseInto(input: CodedInputStream, writer: RowWriter): Unit = {
    val state = if (instanceParseState ne null) {
      instanceParseState.reset()
      instanceParseState
    } else {
      new ParseState(maxFieldNumber)
    }
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

    val fieldType = fieldTypes(fieldNumber)

    // Validate wire type matches expected type for this field
    val expectedWireType = getExpectedWireType(fieldType)
    if (wireType != expectedWireType && !isValidWireTypeForField(wireType, fieldType, isRepeatedFlags(fieldNumber))) {
      // Wire type mismatch - skip this field to avoid parsing errors
      input.skipField(tag)
      return
    }

    // JIT friendly: split repeated and single field parsing into separate methods to reduce bytecode size
    if (isRepeatedFlags(fieldNumber)) {
      parseRepeatedFieldWithState(input, wireType, fieldNumber, state)
    } else {
      parseSingleField(input, fieldNumber, writer)
    }
  }

  private def parseRepeatedFieldWithState(
      input: CodedInputStream,
      wireType: Int,
      fieldNumber: Int,
      state: ParseState): Unit = {
    import FieldDescriptor.Type._
    val fieldType = fieldTypes(fieldNumber)

    // For repeated fields, accumulate values using type-specific accumulators
    fieldType match {
      // Variable-length int32 types
      case INT32 | UINT32 =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[IntList]
        if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
          parsePackedVarint32s(input, list)
        } else {
          list.add(input.readRawVarint32())
        }

      case ENUM =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[IntList]
        if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
          parsePackedVarint32s(input, list)
        } else {
          list.add(input.readEnum())
        }

      case SINT32 =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[IntList]
        if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
          parsePackedSInt32s(input, list)
        } else {
          list.add(input.readSInt32())
        }

      // Variable-length int64 types
      case INT64 | UINT64 =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[LongList]
        if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
          parsePackedVarint64s(input, list)
        } else {
          list.add(input.readRawVarint64())
        }

      case SINT64 =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[LongList]
        if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
          parsePackedSInt64s(input, list)
        } else {
          list.add(input.readSInt64())
        }

      // Fixed-size int32 types
      case FIXED32 | SFIXED32 =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[IntList]
        if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
          val packedLength = input.readRawVarint32()
          list.array = parsePackedFixed32s(input, list.array, list.count, packedLength)
          list.count += packedLength / 4
        } else {
          list.add(input.readRawLittleEndian32())
        }

      // Fixed-size int64 types
      case FIXED64 | SFIXED64 =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[LongList]
        if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
          val packedLength = input.readRawVarint32()
          list.array = parsePackedFixed64s(input, list.array, list.count, packedLength)
          list.count += packedLength / 8
        } else {
          list.add(input.readRawLittleEndian64())
        }

      // Float type
      case FLOAT =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[FloatList]
        if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
          val packedLength = input.readRawVarint32()
          list.array = parsePackedFloats(input, list.array, list.count, packedLength)
          list.count += packedLength / 4
        } else {
          list.add(input.readFloat())
        }

      // Double type
      case DOUBLE =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[DoubleList]
        if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
          val packedLength = input.readRawVarint32()
          list.array = parsePackedDoubles(input, list.array, list.count, packedLength)
          list.count += packedLength / 8
        } else {
          list.add(input.readDouble())
        }

      // Boolean type
      case BOOL =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[BooleanList]
        if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
          val packedLength = input.readRawVarint32()
          list.array = parsePackedBooleans(input, list.array, list.count, packedLength)
          list.count += packedLength
        } else {
          list.add(input.readBool())
        }

      // String/Bytes types
      case STRING | BYTES =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[BytesList]
        list.add(input.readByteArray())

      // Message types use ByteBuffer to avoid copying
      case MESSAGE =>
        val list = state.getOrCreateAccumulator(fieldNumber, fieldType).asInstanceOf[BufferList]
        list.add(input.readByteBuffer())

      case GROUP =>
        throw new UnsupportedOperationException("GROUP type is deprecated and not supported")
    }
  }

  private def parseSingleField(
      input: CodedInputStream,
      fieldNumber: Int,
      writer: RowWriter): Unit = {
    import FieldDescriptor.Type._

    val rowOrdinal = rowOrdinals(fieldNumber)
    val fieldType = fieldTypes(fieldNumber)

    // Single field - write directly to the row
    fieldType match {
      case DOUBLE =>
        writer.write(rowOrdinal, input.readDouble())
      case FLOAT =>
        writer.write(rowOrdinal, input.readFloat())
      case INT64 | UINT64 =>
        writer.write(rowOrdinal, input.readRawVarint64())
      case INT32 | UINT32 =>
        writer.write(rowOrdinal, input.readRawVarint32())
      case FIXED64 | SFIXED64 =>
        writer.write(rowOrdinal, input.readRawLittleEndian64())
      case FIXED32 | SFIXED32 =>
        writer.write(rowOrdinal, input.readRawLittleEndian32())
      case BOOL =>
        writer.write(rowOrdinal, input.readBool())
      case STRING =>
        writer.writeBytes(rowOrdinal, input.readByteArray())
      case BYTES =>
        writer.writeBytes(rowOrdinal, input.readByteArray())
      case ENUM =>
        writer.write(rowOrdinal, input.readEnum())
      case SINT32 =>
        writer.write(rowOrdinal, input.readSInt32())
      case SINT64 =>
        writer.write(rowOrdinal, input.readSInt64())
      case MESSAGE =>
        writeNestedMessage(input, fieldNumber, writer)
      case GROUP =>
        throw new UnsupportedOperationException("GROUP type is deprecated and not supported")
    }
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
      val messageBytes = input.readByteArray()
      // Message read but not processed - it's not in the schema
    }
  }

  private def writeAccumulatedRepeatedFields(writer: RowWriter, state: ParseState): Unit = {
    var fieldNumber = 0
    while (fieldNumber <= maxFieldNumber) {
      if (rowOrdinals(fieldNumber) >= 0 && isRepeatedFlags(fieldNumber)) {
        val accumulator = state.getAccumulator(fieldNumber)
        if (accumulator ne null) {
          val rowOrdinal = rowOrdinals(fieldNumber)
          val fieldType = fieldTypes(fieldNumber)

          accumulator match {
            case list: IntList if list.count > 0 =>
              // Handle enum conversion for ENUM fields
              if (fieldType == FieldDescriptor.Type.ENUM) {
                writeEnumArray(list, fieldNumber, writer)
              } else {
                writeIntArray(list.array, list.count, rowOrdinal, writer)
              }

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
                case FieldDescriptor.Type.STRING =>
                  writeStringArray(list.array, list.count, rowOrdinal, writer)
                case FieldDescriptor.Type.BYTES =>
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

            case _ => // Empty lists or null - skip
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

object WireFormatParser {

  /**
   * Parse state that holds all accumulators for a single parse operation.
   * This ensures thread-safe recursive parsing by isolating state per call.
   */
  private class ParseState(maxFieldNumber: Int) {
    // Single array of FastList for all field types - cast to specific subtype when needed
    private val lists = new Array[FastList](maxFieldNumber + 1)

    def getOrCreateAccumulator(fieldNumber: Int, fieldType: FieldDescriptor.Type): AnyRef = {
      import FieldDescriptor.Type._
      if (lists(fieldNumber) eq null) {
        lists(fieldNumber) = fieldType match {
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
      lists(fieldNumber)
    }

    def getAccumulator(fieldNumber: Int): AnyRef = {
      lists(fieldNumber)
    }

    def reset(): Unit = {
      var i = 0
      while (i <= maxFieldNumber) {
        if (lists(i) ne null) lists(i).reset()
        i += 1
      }
    }
  }

  private val threadVisited = ThreadLocal.withInitial(() => mutable.HashMap[(String, Boolean), ParserRef]())

  /**
   * Smart construction that detects recursion and optimizes entire parser tree.
   * Uses ThreadLocal caching to reuse visited parser references within the same thread,
   * enabling efficient cycle detection and parser reuse for recursive message structures.
   *
   * @param descriptor the protobuf message descriptor
   * @param schema the corresponding Spark SQL schema
   * @return optimized WireFormatParser with recursion detection
   */
  def apply(descriptor: Descriptor, schema: StructType): WireFormatParser =
    buildOptimizedParser(descriptor, schema, isRecursive = false, threadVisited.get()).parser

  private def buildOptimizedParser(
      descriptor: Descriptor,
      schema: StructType,
      isRecursive: Boolean,
      visited: mutable.HashMap[(String, Boolean), ParserRef]): ParserRef = {

    val key = (descriptor.getFullName, isRecursive)

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
      visited: mutable.HashMap[(String, Boolean), ParserRef] = mutable.HashMap()): Array[ParserRef] = {

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
}