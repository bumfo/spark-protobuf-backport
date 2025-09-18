package fastproto

import org.apache.spark.sql.catalyst.InternalRow

/**
 * Interface for parsers that can work with both protobuf binary data and
 * compiled protobuf message objects. This interface extends [[Parser]]
 * to add message-based conversion capabilities.
 *
 * Implementations are typically generated at runtime by the 
 * [[fastproto.ProtoToRowGenerator]] using Janino compilation, providing
 * efficient conversion from compiled protobuf classes to Spark's internal
 * row representation.
 *
 * @tparam T the type of the compiled Protobuf message
 */
trait MessageParser[T] extends Parser {
  /**
   * Parse a compiled protobuf message into Spark's internal row representation.
   * The returned [[InternalRow]] should have one entry per field defined in the
   * message descriptor.
   *
   * @param message the compiled Protobuf message instance
   * @return an [[InternalRow]] containing the extracted field values
   */
  def parse(message: T): InternalRow
}