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
   * <p>
   * This method enables efficient nested conversions by sharing the underlying buffer
   * across the entire row tree, reducing memory allocations. It is the foundation
   * for proper nested message handling in array and complex structure contexts.
   * <p>
   * <b>Buffer Sharing Behavior:</b>
   * <ul>
   * <li><b>parentWriter == null:</b> Creates a fresh UnsafeRowWriter with its own buffer,
   *     calls writeData(), and returns the completed InternalRow</li>
   * <li><b>parentWriter != null:</b> Creates a new UnsafeRowWriter that shares the
   *     BufferHolder from parentWriter, writes data at the current cursor position,
   *     and returns null (data is written directly to shared buffer)</li>
   * </ul>
   * <p>
   * <b>Critical Usage Pattern:</b>
   * This method is essential for nested message arrays where each element must be
   * written to a separate row space without interfering with the parent row's ordinals.
   * It creates proper data isolation between nested and parent structures.
   * <p>
   * <b>Example - Nested Message Array:</b>
   * <pre>
   * for (int i = 0; i < size; i++) {
   *     int elemOffset = arrayWriter.cursor();
   *     converter.convert(messageBytes[i], writer);  // Shares buffer, writes at cursor
   *     arrayWriter.setOffsetAndSizeFromPreviousCursor(i, elemOffset);
   * }
   * </pre>
   *
   * @param binary       the protobuf binary data to convert (wire format)
   * @param parentWriter the parent UnsafeWriter to share BufferHolder with, can be null
   * @return an InternalRow if parentWriter is null, otherwise null (data written to shared buffer)
   * @throws RuntimeException if protobuf parsing fails or encounters invalid wire format
   *
   * @see AbstractRowConverter#writeData(Array[Byte], UnsafeRowWriter) for absolute ordinal writing
   */
  def convert(binary: Array[Byte], parentWriter: UnsafeWriter): InternalRow

  /**
   * The Catalyst schema corresponding to this converter.  This schema
   * describes the structure of the [[InternalRow]] produced by [[convert]].
   * Implementations should return the [[StructType]] used to build the
   * UnsafeRow.  This allows callers to inspect the field names and types
   * without regenerating the schema from a descriptor.
   */
  def schema: StructType
}
