package fastproto

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeRowWriter, UnsafeWriter}
import org.apache.spark.sql.types.StructType

abstract class AbstractRowConverter(val schema: StructType) extends RowConverter {
  protected val instanceWriter = new UnsafeRowWriter(schema.length)

  protected def prepareWriter(parentWriter: UnsafeWriter): UnsafeRowWriter = {
    if (parentWriter == null) {
      instanceWriter.reset()
      instanceWriter.zeroOutNullBytes()
      instanceWriter
    } else {
      val writer = new UnsafeRowWriter(parentWriter, schema.length)
      writer.resetRowWriter()
      writer.zeroOutNullBytes()
      writer
    }
  }

  /**
   * Core parsing method that implementations must override to write protobuf data to UnsafeRowWriter.
   * <p>
   * This abstract method defines the contract for parsing protobuf binary data and writing
   * the extracted fields to specific ordinal positions in the provided writer. Implementations
   * are responsible for protobuf format parsing and field-to-ordinal mapping.
   * <p>
   * <b>Interface Design:</b>
   * <ul>
   * <li><b>Absolute Ordinals:</b> Write to specific positions (0, 1, 2, etc.) in the writer</li>
   * <li><b>Error Handling:</b> Should throw RuntimeException for parsing failures</li>
   * <li><b>Writer Contract:</b> Use provided writer without modifying its configuration</li>
   * </ul>
   * <p>
   * This method is called by [[#convert(Array[Byte], UnsafeWriter)]] after writer preparation.
   * The convert method handles buffer sharing and writer lifecycle, while writeData focuses
   * on parsing and field extraction logic.
   *
   * @param binary the protobuf binary data to parse
   * @param writer the UnsafeRowWriter to populate with parsed field data
   */
  protected def writeData(binary: Array[Byte], writer: UnsafeRowWriter): Unit

  override def convert(binary: Array[Byte], parentWriter: UnsafeWriter): InternalRow = {
    val writer = prepareWriter(parentWriter)
    writeData(binary, writer)
    if (parentWriter == null) writer.getRow else null
  }
}