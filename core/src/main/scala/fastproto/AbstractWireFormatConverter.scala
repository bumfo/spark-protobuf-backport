package fastproto

import com.google.protobuf.CodedInputStream
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeArrayWriter, UnsafeRowWriter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Abstract base class for generated WireFormat converters.
 *
 * This class provides common functionality used by all generated converters,
 * reducing the size of generated code and improving JVM optimization by keeping
 * hot methods small enough for inlining.
 *
 * @param schema the Spark SQL schema for the target row structure
 */
abstract class AbstractWireFormatConverter(schema: StructType) extends AbstractRowConverter(schema) {

  /**
   * Array of nested converters indexed by field number.
   * Populated by generated code for message fields.
   */
  protected var nestedConverters: Array[AbstractWireFormatConverter] = _

  /**
   * Set a nested converter for a specific field number.
   * Called during converter initialization for message fields.
   */
  def setNestedConverter(fieldNumber: Int, converter: AbstractWireFormatConverter): Unit = {
    if (nestedConverters == null) {
      nestedConverters = new Array[AbstractWireFormatConverter](fieldNumber + 1)
    }
    if (fieldNumber >= nestedConverters.length) {
      val newArray = new Array[AbstractWireFormatConverter](fieldNumber + 1)
      System.arraycopy(nestedConverters, 0, newArray, 0, nestedConverters.length)
      nestedConverters = newArray
    }
    nestedConverters(fieldNumber) = converter
  }

  /**
   * Get a nested converter for a field number.
   * Returns null if no converter is registered for the field.
   */
  protected def getNestedConverter(fieldNumber: Int): AbstractWireFormatConverter = {
    if (nestedConverters != null && fieldNumber < nestedConverters.length) {
      nestedConverters(fieldNumber)
    } else {
      null
    }
  }

  /**
   * Write a repeated string field as an array to the UnsafeRow.
   * Optimized for direct byte array writing without UTF8String allocation per element.
   */
  protected def writeStringArray(values: Array[Array[Byte]], ordinal: Int, writer: UnsafeRowWriter): Unit = {
    val offset = writer.cursor()
    val arrayWriter = new UnsafeArrayWriter(writer, 8) // Variable-length strings use 8-byte offset/size
    arrayWriter.initialize(values.length)

    var i = 0
    while (i < values.length) {
      arrayWriter.write(i, UTF8String.fromBytes(values(i)))
      i += 1
    }

    writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset)
  }

  /**
   * Write a repeated byte array field as an array to the UnsafeRow.
   */
  protected def writeBytesArray(values: Array[Array[Byte]], ordinal: Int, writer: UnsafeRowWriter): Unit = {
    val offset = writer.cursor()
    val arrayWriter = new UnsafeArrayWriter(writer, 8)
    arrayWriter.initialize(values.length)

    var i = 0
    while (i < values.length) {
      arrayWriter.write(i, values(i))
      i += 1
    }

    writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset)
  }

  /**
   * Write a repeated int field as an array to the UnsafeRow.
   */
  protected def writeIntArray(values: Array[Int], ordinal: Int, writer: UnsafeRowWriter): Unit = {
    val offset = writer.cursor()
    val arrayWriter = new UnsafeArrayWriter(writer, 4)
    arrayWriter.initialize(values.length)

    var i = 0
    while (i < values.length) {
      arrayWriter.write(i, values(i))
      i += 1
    }

    writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset)
  }

  /**
   * Write a repeated long field as an array to the UnsafeRow.
   */
  protected def writeLongArray(values: Array[Long], ordinal: Int, writer: UnsafeRowWriter): Unit = {
    val offset = writer.cursor()
    val arrayWriter = new UnsafeArrayWriter(writer, 8)
    arrayWriter.initialize(values.length)

    var i = 0
    while (i < values.length) {
      arrayWriter.write(i, values(i))
      i += 1
    }

    writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset)
  }

  /**
   * Write a repeated float field as an array to the UnsafeRow.
   */
  protected def writeFloatArray(values: Array[Float], ordinal: Int, writer: UnsafeRowWriter): Unit = {
    val offset = writer.cursor()
    val arrayWriter = new UnsafeArrayWriter(writer, 4)
    arrayWriter.initialize(values.length)

    var i = 0
    while (i < values.length) {
      arrayWriter.write(i, values(i))
      i += 1
    }

    writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset)
  }

  /**
   * Write a repeated double field as an array to the UnsafeRow.
   */
  protected def writeDoubleArray(values: Array[Double], ordinal: Int, writer: UnsafeRowWriter): Unit = {
    val offset = writer.cursor()
    val arrayWriter = new UnsafeArrayWriter(writer, 8)
    arrayWriter.initialize(values.length)

    var i = 0
    while (i < values.length) {
      arrayWriter.write(i, values(i))
      i += 1
    }

    writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset)
  }

  /**
   * Write a repeated boolean field as an array to the UnsafeRow.
   */
  protected def writeBooleanArray(values: Array[Boolean], ordinal: Int, writer: UnsafeRowWriter): Unit = {
    val offset = writer.cursor()
    val arrayWriter = new UnsafeArrayWriter(writer, 1)
    arrayWriter.initialize(values.length)

    var i = 0
    while (i < values.length) {
      arrayWriter.write(i, values(i))
      i += 1
    }

    writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset)
  }

  /**
   * Write a repeated message field as an array to the UnsafeRow.
   * Uses nested converter with writer sharing for optimal performance.
   */
  protected def writeMessageArray(messageBytes: Array[Array[Byte]], fieldNumber: Int, ordinal: Int, writer: UnsafeRowWriter): Unit = {
    val converter = getNestedConverter(fieldNumber)
    if (converter == null) {
      throw new IllegalStateException(s"No nested converter found for field $fieldNumber")
    }

    val offset = writer.cursor()
    val arrayWriter = new UnsafeArrayWriter(writer, 8)
    arrayWriter.initialize(messageBytes.length)

    var i = 0
    while (i < messageBytes.length) {
      val elemOffset = arrayWriter.cursor()
      converter.convert(messageBytes(i), writer)
      arrayWriter.setOffsetAndSizeFromPreviousCursor(i, elemOffset)
      i += 1
    }

    writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset)
  }

  /**
   * Write a single nested message field to the UnsafeRow.
   * Uses nested converter with writer sharing.
   */
  protected def writeMessage(messageBytes: Array[Byte], fieldNumber: Int, ordinal: Int, writer: UnsafeRowWriter): Unit = {
    val converter = getNestedConverter(fieldNumber)
    if (converter == null) {
      throw new IllegalStateException(s"No nested converter found for field $fieldNumber")
    }

    val offset = writer.cursor()
    converter.convert(messageBytes, writer)
    writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset)
  }

  /**
   * Parse packed repeated values from a LENGTH_DELIMITED wire format.
   * Returns the number of values parsed.
   */
  protected def parsePackedInts(input: CodedInputStream, buffer: Array[Int], maxCount: Int): Int = {
    val length = input.readRawVarint32()
    val oldLimit = input.pushLimit(length)
    var count = 0

    while (input.getBytesUntilLimit > 0 && count < maxCount) {
      buffer(count) = input.readInt32()
      count += 1
    }

    input.popLimit(oldLimit)
    count
  }

  /**
   * Parse packed repeated longs from a LENGTH_DELIMITED wire format.
   */
  protected def parsePackedLongs(input: CodedInputStream, buffer: Array[Long], maxCount: Int): Int = {
    val length = input.readRawVarint32()
    val oldLimit = input.pushLimit(length)
    var count = 0

    while (input.getBytesUntilLimit > 0 && count < maxCount) {
      buffer(count) = input.readInt64()
      count += 1
    }

    input.popLimit(oldLimit)
    count
  }

  /**
   * Parse packed repeated floats from a LENGTH_DELIMITED wire format.
   */
  protected def parsePackedFloats(input: CodedInputStream, buffer: Array[Float], maxCount: Int): Int = {
    val length = input.readRawVarint32()
    val oldLimit = input.pushLimit(length)
    var count = 0

    while (input.getBytesUntilLimit > 0 && count < maxCount) {
      buffer(count) = input.readFloat()
      count += 1
    }

    input.popLimit(oldLimit)
    count
  }

  /**
   * Parse packed repeated doubles from a LENGTH_DELIMITED wire format.
   */
  protected def parsePackedDoubles(input: CodedInputStream, buffer: Array[Double], maxCount: Int): Int = {
    val length = input.readRawVarint32()
    val oldLimit = input.pushLimit(length)
    var count = 0

    while (input.getBytesUntilLimit > 0 && count < maxCount) {
      buffer(count) = input.readDouble()
      count += 1
    }

    input.popLimit(oldLimit)
    count
  }

  /**
   * Parse packed repeated booleans from a LENGTH_DELIMITED wire format.
   */
  protected def parsePackedBooleans(input: CodedInputStream, buffer: Array[Boolean], maxCount: Int): Int = {
    val length = input.readRawVarint32()
    val oldLimit = input.pushLimit(length)
    var count = 0

    while (input.getBytesUntilLimit > 0 && count < maxCount) {
      buffer(count) = input.readBool()
      count += 1
    }

    input.popLimit(oldLimit)
    count
  }
}