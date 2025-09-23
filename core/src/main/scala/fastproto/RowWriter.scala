package fastproto

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

/**
 * Trait for row writers that manage null bits automatically.
 * Requires extending UnsafeWriter but encapsulates it from the public interface.
 */
trait RowWriter {
  self: UnsafeWriter =>

  /** Clear the null bit for a field, marking it as non-null */
  def clearNullBit(ordinal: Int): Unit

  def setNullAt(ordinal: Int): Unit

  /** Cast this RowWriter to UnsafeWriter for accessing base class methods */
  def toUnsafeWriter: UnsafeWriter = this

  /** Get the UnsafeRow result */
  def getRow: UnsafeRow

  def cursor: Int

  /** Reset the row writer and set all fields to null */
  def resetRowWriter(): Unit

  // Write methods that automatically clear null bits
  def write(ordinal: Int, value: Boolean): Unit
  def write(ordinal: Int, value: Byte): Unit
  def write(ordinal: Int, value: Short): Unit
  def write(ordinal: Int, value: Int): Unit
  def write(ordinal: Int, value: Long): Unit
  def write(ordinal: Int, value: Float): Unit
  def write(ordinal: Int, value: Double): Unit
  def write(ordinal: Int, input: Decimal, precision: Int, scale: Int): Unit

  def writeBytes(ordinal: Int, value: Array[Byte]): Unit

  /**
   * Note: For UTF8 strings from byte arrays, prefer writeBytes(ordinal, bytes) over
   * writeUTF8String(ordinal, UTF8String.fromBytes(bytes)) to avoid intermediate object creation.
   */
  def writeUTF8String(ordinal: Int, value: UTF8String): Unit

  def writeVariableField(ordinal: Int, previousCursor: Int): Unit
}