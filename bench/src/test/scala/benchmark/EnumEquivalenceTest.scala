package benchmark

import benchmark.DomBenchmarkProtos.DomNode
import fastproto._
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
    val stringEnumSchema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = false)
    val intEnumSchema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)

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

}