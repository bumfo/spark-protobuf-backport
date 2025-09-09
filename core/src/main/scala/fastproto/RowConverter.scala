package fastproto

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * A simple interface for converting compiled Protobuf messages into Spark's
 * [[org.apache.spark.sql.catalyst.InternalRow]].  Implementations of this
 * trait are usually generated at runtime by the [[fastproto.ProtoToRowGenerator]]
 * using Janino.  The generic type parameter `T` must correspond to a
 * generated Java class extending `com.google.protobuf.Message`.  The
 * conversion should populate a new [[InternalRow]] with values extracted
 * directly from the message using its accessor methods.
 *
 * @tparam T the type of the compiled Protobuf message
 */
trait RowConverter[T] extends Serializable {
  /**
   * Convert a single message into Spark's internal row representation.  The
   * returned [[InternalRow]] should have one entry per field defined in the
   * message descriptor.  Consumers can subsequently turn the returned row
   * into an [[org.apache.spark.sql.catalyst.expressions.UnsafeRow]] using
   * [[org.apache.spark.sql.catalyst.expressions.codegen.UnsafeProjection]].
   *
   * @param message the compiled Protobuf message instance
   * @return an [[InternalRow]] containing the extracted field values
   */
  def convert(message: T): InternalRow

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
  def convert(message: T, parentWriter: org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter): InternalRow = {
    // Default implementation for backward compatibility - just delegates to single-arg version
    convert(message)
  }

  /**
   * The Catalyst schema corresponding to this converter.  This schema
   * describes the structure of the [[InternalRow]] produced by [[convert]].
   * Implementations should return the [[StructType]] used to build the
   * UnsafeRow.  This allows callers to inspect the field names and types
   * without regenerating the schema from a descriptor.
   */
  def schema: StructType
}
