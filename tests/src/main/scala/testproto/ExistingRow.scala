package testproto

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

case class ExistingRow(row: InternalRow, schema: DataType) extends LeafExpression with CodegenFallback {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = row

  override def dataType: DataType = schema
}
