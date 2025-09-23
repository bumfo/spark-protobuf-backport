package fastproto

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeRowWriter, UnsafeWriter}
import org.apache.spark.sql.types.StructType

abstract class BufferSharingParser(val schema: StructType) extends Parser {
  protected val instanceWriter = new UnsafeRowWriter(schema.length)

  def acquireWriter(parentWriter: UnsafeWriter): UnsafeRowWriter = {
    if (parentWriter == null) {
      instanceWriter.reset()
      UnsafeRowWriterHelper.setAllFieldsNull(instanceWriter)
      instanceWriter
    } else {
      val writer = acquireNestedWriter(parentWriter)
      writer.resetRowWriter()
      UnsafeRowWriterHelper.setAllFieldsNull(writer)
      writer
    }
  }

  def acquireNestedWriter(parentWriter: UnsafeWriter): UnsafeRowWriter = new UnsafeRowWriter(parentWriter, schema.length)

  /**
   * Core parsing method that implementations must override to write protobuf data to UnsafeRowWriter.
   * <p>
   * This abstract method defines the contract for parsing protobuf binary data and writing
   * the extracted fields to specific ordinal positions in the provided writer. Implementations
   * are responsible for protobuf format parsing and field-to-ordinal mapping.
   * <p>
   * <b>Interface Design:</b>
   * <ul>
   * <li><b>Absolute Ordinals:</b> Write to specific positions (0, 1, 2, etc.) in the writer</li>
   * <li><b>Error Handling:</b> Should throw RuntimeException for parsing failures</li>
   * <li><b>Writer Contract:</b> Use provided writer without modifying its configuration</li>
   * </ul>
   * <p>
   * This method is called by [[parseWithSharedBuffer(Array[Byte], UnsafeWriter)]] after writer acquisition.
   * The convert method handles buffer sharing and writer lifecycle, while parseInto focuses
   * on parsing and field extraction logic.
   *
   * @param binary the protobuf binary data to parse
   * @param writer the UnsafeRowWriter to populate with parsed field data
   */
  def parseInto(binary: Array[Byte], writer: UnsafeRowWriter): Unit

  /**
   * Core parsing method with partial byte array support that implementations must override.
   * <p>
   * This abstract method allows parsing from a slice of a byte array without requiring
   * array copying, which improves performance when processing embedded or streamed data.
   * <p>
   * <b>Implementation Requirements:</b>
   * <ul>
   * <li><b>Bounds Checking:</b> Ensure offset + length <= binary.length</li>
   * <li><b>Slice Processing:</b> Only parse bytes from [offset, offset + length)</li>
   * <li><b>Same Contract:</b> Follow same ordinal and error handling as parseInto(Array[Byte], UnsafeRowWriter)</li>
   * </ul>
   *
   * @param binary the byte array containing protobuf data
   * @param offset the starting position in the array
   * @param length the number of bytes to read from the array
   * @param writer the UnsafeRowWriter to populate with parsed field data
   */
  def parseInto(binary: Array[Byte], offset: Int, length: Int, writer: UnsafeRowWriter): Unit

  /**
   * Convert protobuf binary data using a shared UnsafeWriter for BufferHolder sharing.
   * This method enables efficient nested conversions by sharing the underlying buffer
   * across the entire row tree, reducing memory allocations. Moved from Parser trait
   * to this class since only buffer-sharing parsers support this functionality.
   * TODO: manually inline this method in suitable places to reuse acquired writer (e.g. in loop)
   */
  def parseWithSharedBuffer(binary: Array[Byte], parentWriter: UnsafeWriter): InternalRow = {
    val writer = acquireWriter(parentWriter)
    parseInto(binary, writer)
    if (parentWriter == null) writer.getRow else null
  }

  /**
   * Convert partial protobuf binary data using a shared UnsafeWriter for BufferHolder sharing.
   * This method allows parsing from a slice of a byte array without requiring array copying,
   * which improves performance when processing embedded or streamed protobuf data.
   *
   * @param binary the byte array containing protobuf data
   * @param offset the starting position in the array
   * @param length the number of bytes to read from the array
   * @param parentWriter the parent UnsafeWriter for buffer sharing, or null for standalone parsing
   * @return InternalRow containing parsed data, or null if using shared buffer
   */
  def parseWithSharedBuffer(binary: Array[Byte], offset: Int, length: Int, parentWriter: UnsafeWriter): InternalRow = {
    val writer = acquireWriter(parentWriter)
    parseInto(binary, offset, length, writer)
    if (parentWriter == null) writer.getRow else null
  }

  override def parse(binary: Array[Byte]): InternalRow = parseWithSharedBuffer(binary, null)

  /**
   * Parse protobuf data from a partial byte array.
   * This method allows parsing from a slice of a byte array without requiring array copying.
   *
   * @param binary the byte array containing protobuf data
   * @param offset the starting position in the array
   * @param length the number of bytes to read from the array
   * @return InternalRow containing the parsed protobuf data
   */
  def parse(binary: Array[Byte], offset: Int, length: Int): InternalRow = parseWithSharedBuffer(binary, offset, length, null)
}