package fastproto

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Base interface for converting protobuf binary data into Spark's
 * [[org.apache.spark.sql.catalyst.InternalRow]]. This interface supports
 * both direct binary conversion and message-based conversion through
 * its sub-interfaces.
 * 
 * Implementations are usually generated at runtime by the 
 * [[fastproto.ProtoToRowGenerator]] using Janino compilation.
 */
trait RowConverter extends Serializable {
  /**
   * Convert protobuf binary data into Spark's internal row representation.
   * This is the primary conversion method that takes raw protobuf bytes
   * and produces an [[InternalRow]].
   *
   * @param binary the protobuf binary data to convert
   * @return an [[InternalRow]] containing the extracted field values
   */
  def convert(binary: Array[Byte]): InternalRow = {
    convert(binary, null)
  }

  /**
   * Convert protobuf binary data using a shared UnsafeWriter for BufferHolder sharing.
   * This method enables efficient nested conversions by sharing the underlying
   * buffer across the entire row tree, reducing memory allocations.
   * 
   * When parentWriter is provided, the implementation should create a new
   * UnsafeRowWriter that shares the BufferHolder from the parent writer.
   *
   * @param binary the protobuf binary data to convert
   * @param parentWriter the parent UnsafeWriter to share BufferHolder with, can be null
   * @return an [[InternalRow]] containing the extracted field values
   */
  def convert(binary: Array[Byte], parentWriter: org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter): InternalRow

  /**
   * The Catalyst schema corresponding to this converter.  This schema
   * describes the structure of the [[InternalRow]] produced by [[convert]].
   * Implementations should return the [[StructType]] used to build the
   * UnsafeRow.  This allows callers to inspect the field names and types
   * without regenerating the schema from a descriptor.
   */
  def schema: StructType
}
