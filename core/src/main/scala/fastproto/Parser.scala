package fastproto

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter
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
trait Parser extends Serializable {
  /**
   * Convert protobuf binary data into Spark's internal row representation.
   * This is the primary conversion method that takes raw protobuf bytes
   * and produces an [[InternalRow]].
   *
   * @param binary the protobuf binary data to convert
   * @return an [[InternalRow]] containing the extracted field values
   */
  def parse(binary: Array[Byte]): InternalRow

  /**
   * The Catalyst schema corresponding to this converter.  This schema
   * describes the structure of the [[InternalRow]] produced by [[parse]].
   * Implementations should return the [[StructType]] used to build the
   * UnsafeRow.  This allows callers to inspect the field names and types
   * without regenerating the schema from a descriptor.
   */
  def schema: StructType
}
