package fastproto

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeRowWriter, UnsafeWriter}
import org.apache.spark.sql.types.StructType

abstract class AbstractMessageParser[T](schema: StructType)
  extends BufferSharingParser(schema) with MessageParser[T] {

  def parseInto(message: T, writer: RowWriter): Unit

  /**
   * Default implementation for partial byte array parsing.
   * Since AbstractMessageParser works with compiled message objects,
   * this method creates a slice of the byte array and delegates to
   * the existing parseInto(Array[Byte], RowWriter) method.
   */
  override def parseInto(binary: Array[Byte], offset: Int, length: Int, writer: RowWriter): Unit = {
    // Create a slice of the array for this implementation
    // Note: This involves array copying, but AbstractMessageParser subclasses
    // typically work with pre-parsed messages rather than raw bytes
    val slice = java.util.Arrays.copyOfRange(binary, offset, offset + length)
    parseInto(slice, writer)
  }

  /**
   * Parse a message using a shared UnsafeWriter for BufferHolder sharing.
   * This method enables efficient nested conversions by sharing the underlying
   * buffer across the entire row tree, reducing memory allocations.
   *
   * When parentWriter is provided, the implementation should create a new
   * UnsafeRowWriter that shares the BufferHolder from the parent writer.
   *
   * @param message      the compiled Protobuf message instance
   * @param parentWriter the parent UnsafeWriter to share BufferHolder with, can be null
   * @return an [[InternalRow]] containing the extracted field values
   */
  def parseWithSharedBuffer(message: T, parentWriter: UnsafeWriter): InternalRow = {
    val writer = acquireWriter(parentWriter)
    parseInto(message, writer)
    if (parentWriter == null) writer.getRow else null
  }

  override def parse(message: T): InternalRow = parseWithSharedBuffer(message, null)
}