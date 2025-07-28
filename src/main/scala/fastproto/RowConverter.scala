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
   * The Catalyst schema corresponding to this converter.  This schema
   * describes the structure of the [[InternalRow]] produced by [[convert]].
   * Implementations should return the [[StructType]] used to build the
   * UnsafeRow.  This allows callers to inspect the field names and types
   * without regenerating the schema from a descriptor.
   */
  def schema: StructType
}
