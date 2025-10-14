package benchmark

import com.google.protobuf.Descriptors.Descriptor
import fastproto.InlineParserGenerator
import org.apache.spark.sql.protobuf.backport.utils.SchemaConverters
import org.apache.spark.sql.types.StructType

object ShowInlineGeneratedCode {
  def main(args: Array[String]): Unit = {
    println("=" * 80)
    println("Inline Parser Generator Demo")
    println("=" * 80)
    println()

    // Generate for ComplexMessageA
    showGeneratedParser("ComplexMessageA", ComplexBenchmarkProtos.ComplexMessageA.getDescriptor)

    println()
    println("=" * 80)

    // Generate for ComplexMessageB (nested)
    showGeneratedParser("ComplexMessageB", ComplexBenchmarkProtos.ComplexMessageB.getDescriptor)
  }

  def showGeneratedParser(name: String, descriptor: Descriptor): Unit = {
    val schema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    println(s"### Generated Parser for $name")
    println()
    println(s"Schema fields: ${schema.fields.length}")
    println(s"Proto fields: ${descriptor.getFields.size()}")
    println()

    val generatedCode = InlineParserGenerator.generateParser(
      s"Generated_${name}_Parser",
      descriptor,
      schema
    )

    val lineCount = generatedCode.split("\n").length
    println(s"Generated code: $lineCount lines")
    println()
    println("Generated code:")
    println("-" * 80)
    println(generatedCode)
    println("-" * 80)
  }
}
