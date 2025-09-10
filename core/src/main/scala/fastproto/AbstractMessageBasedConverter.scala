package fastproto

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeRowWriter, UnsafeWriter}
import org.apache.spark.sql.types.StructType

abstract class AbstractMessageBasedConverter[T](schema: StructType) 
  extends AbstractRowConverter(schema) with MessageBasedConverter[T] {

  protected def writeMessage(message: T, writer: UnsafeRowWriter): Unit

  override final def convert(message: T, parentWriter: UnsafeWriter): InternalRow = {
    val writer = prepareWriter(parentWriter)
    writeMessage(message, writer)
    if (parentWriter == null) writer.getRow else null
  }
}