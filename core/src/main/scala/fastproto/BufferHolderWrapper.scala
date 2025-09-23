package fastproto

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter

abstract class BufferHolderWrapper(
    writer: UnsafeWriter,
    numFields: Int) extends UnsafeWriter(writer.getBufferHolder) {
  val nullBitsSize = UnsafeRow.calculateBitSetWidthInBytes(numFields)
  val fixedSize = nullBitsSize + 8 * numFields
  startingOffset = cursor
}
