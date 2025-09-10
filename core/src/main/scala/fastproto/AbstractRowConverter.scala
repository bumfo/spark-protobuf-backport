package fastproto

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeRowWriter, UnsafeWriter}
import org.apache.spark.sql.types.StructType

abstract class AbstractRowConverter(val schema: StructType) extends RowConverter {
  protected val instanceWriter = new UnsafeRowWriter(schema.length)

  protected final def prepareWriter(parentWriter: UnsafeWriter): UnsafeRowWriter = {
    if (parentWriter == null) {
      instanceWriter.reset()
      instanceWriter.zeroOutNullBytes()
      instanceWriter
    } else {
      val writer = new UnsafeRowWriter(parentWriter, schema.length)
      writer.resetRowWriter()
      writer
    }
  }

  protected def writeData(binary: Array[Byte], writer: UnsafeRowWriter): Unit

  override final def convert(binary: Array[Byte], parentWriter: UnsafeWriter): InternalRow = {
    val writer = prepareWriter(parentWriter)
    writeData(binary, writer)
    if (parentWriter == null) writer.getRow else null
  }
}