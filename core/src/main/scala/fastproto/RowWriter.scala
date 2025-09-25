package fastproto

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

import java.nio.ByteBuffer

/**
 * Trait for row writers that manage null bits automatically.
 * Requires extending UnsafeWriter but encapsulates it from the public interface.
 */
trait RowWriter {
  self: UnsafeWriter =>

  // Core State & Lifecycle

  /**
   * Reset the row writer and set all fields to null for nested writers.
   * This method is designed for nested row writers that don't own their own UnsafeRow.
   *
   * @throws IllegalStateException if called on a top-level writer that owns a row
   */
  def resetRowWriter(): Unit = {
    if (hasRow) throw new IllegalStateException("resetRowWriter() should not be called on top-level writers - use initRow() instead")
    reserveRowSpace()
    setAllNullBytes()
  }

  /**
   * Initialize the row for writing by resetting the buffer and setting all fields to null.
   * This method is required for both new writer instances and when reusing writers for new rows.
   *
   * @throws IllegalStateException if called on a nested writer that doesn't own a row
   */
  def initRow(): Unit = {
    if (!hasRow) throw new IllegalStateException("initRow() should only be called on top-level writers that own a row")
    reset() // TODO: also clear buffer to zeros for security
    setAllNullBytes()
  }

  /**
   * Reserves buffer space for a new row's fixed-length data.
   * This method handles memory allocation and cursor management without null bit initialization.
   */
  protected def reserveRowSpace(): Unit

  /**
   * Returns whether this writer owns its own UnsafeRow instance.
   * Top-level writers return true, nested writers return false.
   */
  def hasRow: Boolean

  /**
   * Gets the final UnsafeRow with correct size metadata.
   * Only valid for top-level writers that own their row.
   *
   * @return the completed UnsafeRow, or null for nested writers
   */
  def getRow: UnsafeRow

  /**
   * Gets the current cursor position in the underlying buffer.
   * Used for tracking where variable-length data should be written.
   */
  def cursor: Int

  // Type System Bridge

  /**
   * Cast this RowWriter to UnsafeWriter for accessing base class methods
   * prefer unsafeWriterOrNull in scala for null-safety
   */
  def toUnsafeWriter: UnsafeWriter = this

  // Null Management
  def setNullAt(ordinal: Int): Unit

  protected def setAllNullBytes(): Unit

  /** Clear the null bit for a field, marking it as non-null */
  def clearNullBit(ordinal: Int): Unit

  // Primitive Writers (auto-clear null bits)
  def write(ordinal: Int, value: Boolean): Unit
  def write(ordinal: Int, value: Byte): Unit
  def write(ordinal: Int, value: Short): Unit
  def write(ordinal: Int, value: Int): Unit
  def write(ordinal: Int, value: Long): Unit
  def write(ordinal: Int, value: Float): Unit
  def write(ordinal: Int, value: Double): Unit
  def write(ordinal: Int, input: Decimal, precision: Int, scale: Int): Unit

  // Variable-Length Writers (auto-clear null bits)
  def writeBytes(ordinal: Int, value: Array[Byte]): Unit
  def writeBytes(ordinal: Int, value: ByteBuffer): Unit

  /**
   * Note: For UTF8 strings from byte arrays, prefer writeBytes(ordinal, bytes) over
   * writeUTF8String(ordinal, UTF8String.fromBytes(bytes)) to avoid intermediate object creation.
   */
  def writeUTF8String(ordinal: Int, value: UTF8String): Unit

  def writeVariableField(ordinal: Int, previousCursor: Int): Unit
}

object RowWriter {
  implicit class ToUnsafeWriter(val writer: RowWriter) extends AnyVal {
    // noinspection ScalaDeprecation
    @inline def unsafeWriterOrNull: UnsafeWriter = if (writer ne null) writer.toUnsafeWriter else null
  }
}
