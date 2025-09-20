package benchmark

import benchmark.DomBenchmarkProtos.DomNode
import fastproto.{ProtoToRowGenerator, WireFormatToRowGenerator}
import fastproto.{EquivalenceOptions, RowEquivalenceChecker}
import org.apache.spark.sql.protobuf.backport.utils.SchemaConverters
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Test to verify enum handling equivalence between parsers.
 * Uses simple non-recursive messages to avoid stack overflow issues.
 */
class EnumEquivalenceTest extends AnyFunSuite with Matchers {

  test("Enum fields should be handled equivalently between parsers") {
    // Create a simple DomNode with enum field (without children to avoid recursion)
    val domNode = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("div")
      .setTextContent("Hello World")
      .putAttributes("id", "test")
      .putAttributes("class", "container")
      .build()

    val binaryData = domNode.toByteArray
    val descriptor = domNode.getDescriptorForType

    // Create schemas with different enum representations
    val stringEnumSchema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]
    // Note: We would need to extend SchemaConverters to support enumAsInt parameter
    // For now, manually create a schema with IntegerType for enums
    val intEnumSchema = createIntEnumSchema(stringEnumSchema)

    // Generate parsers with their respective schemas
    val protoToRowParser = ProtoToRowGenerator.generateParser(descriptor, classOf[DomNode], stringEnumSchema)
    val wireFormatParser = WireFormatToRowGenerator.generateParser(descriptor, intEnumSchema)

    // Parse the same data with both parsers
    val protoToRowRow = protoToRowParser.parse(binaryData)
    val wireFormatRow = wireFormatParser.parse(binaryData)

    // Use the RowEquivalenceChecker to compare with different schemas
    RowEquivalenceChecker.assertRowsEquivalent(
      protoToRowRow,
      stringEnumSchema,
      wireFormatRow,
      intEnumSchema,
      Some(descriptor),
      EquivalenceOptions.default,
      "root"
    )
  }

  /**
   * Helper method to create a schema with IntegerType for enum fields.
   * This is a simplified version - a full implementation would recursively handle nested types.
   */
  private def createIntEnumSchema(stringSchema: StructType): StructType = {
    val newFields = stringSchema.fields.map { field =>
      val newDataType = field.dataType match {
        case StringType => IntegerType  // Convert all string types to int (simplified)
        case other => other
      }
      field.copy(dataType = newDataType)
    }
    StructType(newFields)
  }
}