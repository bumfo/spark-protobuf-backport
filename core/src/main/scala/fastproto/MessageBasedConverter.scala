package fastproto

import org.apache.spark.sql.catalyst.InternalRow

/**
 * Interface for converters that can work with both protobuf binary data and
 * compiled protobuf message objects. This interface extends [[RowConverter]]
 * to add message-based conversion capabilities.
 * 
 * Implementations are typically generated at runtime by the 
 * [[fastproto.ProtoToRowGenerator]] using Janino compilation, providing
 * efficient conversion from compiled protobuf classes to Spark's internal
 * row representation.
 *
 * @tparam T the type of the compiled Protobuf message
 */
trait MessageBasedConverter[T] extends RowConverter {
  /**
   * Convert a compiled protobuf message into Spark's internal row representation.
   * The returned [[InternalRow]] should have one entry per field defined in the
   * message descriptor.
   *
   * @param message the compiled Protobuf message instance
   * @return an [[InternalRow]] containing the extracted field values
   */
  def convert(message: T): InternalRow = {
    convertWithSharedBuffer(message, null)
  }

  /**
   * Convert a message using a shared UnsafeWriter for BufferHolder sharing.
   * This method enables efficient nested conversions by sharing the underlying
   * buffer across the entire row tree, reducing memory allocations.
   *
   * When parentWriter is provided, the implementation should create a new
   * UnsafeRowWriter that shares the BufferHolder from the parent writer.
   *
   * @param message the compiled Protobuf message instance
   * @param parentWriter the parent UnsafeWriter to share BufferHolder with, can be null
   * @return an [[InternalRow]] containing the extracted field values
   */
  def convertWithSharedBuffer(message: T, parentWriter: org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter): InternalRow
}