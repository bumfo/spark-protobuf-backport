package fastproto

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeRowWriter, UnsafeWriter}
import org.apache.spark.sql.types.StructType

abstract class BufferSharingParser(val schema: StructType) extends Parser {
  protected val instanceWriter = new UnsafeRowWriter(schema.length)

  protected def acquireWriter(parentWriter: UnsafeWriter): UnsafeRowWriter = {
    if (parentWriter == null) {
      instanceWriter.reset()
      instanceWriter.zeroOutNullBytes()
      instanceWriter
    } else {
      val writer = new UnsafeRowWriter(parentWriter, schema.length)
      writer.resetRowWriter()
      writer.zeroOutNullBytes()
      writer
    }
  }

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
  protected def parseInto(binary: Array[Byte], writer: UnsafeRowWriter): Unit

  /**
   * Convert protobuf binary data using a shared UnsafeWriter for BufferHolder sharing.
   * This method enables efficient nested conversions by sharing the underlying buffer
   * across the entire row tree, reducing memory allocations. Moved from Parser trait
   * to this class since only buffer-sharing parsers support this functionality.
   */
  def parseWithSharedBuffer(binary: Array[Byte], parentWriter: UnsafeWriter): InternalRow = {
    val writer = acquireWriter(parentWriter)
    parseInto(binary, writer)
    if (parentWriter == null) writer.getRow else null
  }

  override def parse(binary: Array[Byte]): InternalRow = parseWithSharedBuffer(binary, null)
}