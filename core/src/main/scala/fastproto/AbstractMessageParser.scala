package fastproto

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeRowWriter, UnsafeWriter}
import org.apache.spark.sql.types.StructType

abstract class AbstractMessageParser[T](schema: StructType)
  extends BufferSharingParser(schema) with MessageParser[T] {

  protected def parseInto(message: T, writer: UnsafeRowWriter): Unit

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