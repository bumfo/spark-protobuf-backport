package benchmark

import benchmark.{DomBenchmarkProtos, RecursiveSchemaConverters}
import benchmark.DomBenchmarkProtos.DomDocument
import fastproto.{ProtoToRowGenerator, WireFormatToRowGenerator}
import fastproto.{EquivalenceOptions, RowEquivalenceChecker}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Test to verify ProtoToRowGenerator and WireFormatToRowGenerator produce
 * equivalent results for DOM structures with map fields.
 *
 * This test ensures both parser implementations handle the same binary → InternalRow
 * conversion correctly, particularly for recursive structures and map fields.
 *
 * NOTE: Enum fields may be represented differently between parsers:
 * - As strings (enum names like "ELEMENT_NODE")
 * - As integers (enum values like 0)
 * The test handles both representations as equivalent.
 */
class DomParserEquivalenceTest extends AnyFunSuite with Matchers {

  private val shallowDom = DomTestDataGenerator.createDomDocument(maxDepth = 3, breadthFactor = 2)
  private val binaryData = shallowDom.toByteArray
  private val descriptor = shallowDom.getDescriptorForType
  private val sparkSchema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor)

  test("ProtoToRowGenerator and WireFormatToRowGenerator should produce equivalent results for shallow DOM") {
    val wireFormatParser = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)
    val protoToRowParser = ProtoToRowGenerator.generateParser(descriptor, classOf[DomDocument], sparkSchema)

    val wireFormatRow = wireFormatParser.parse(binaryData)
    val protoToRowRow = protoToRowParser.parse(binaryData)

    // Both should produce the same number of fields
    wireFormatRow.numFields shouldBe protoToRowRow.numFields

    // Compare all fields systematically using the equivalence checker
    RowEquivalenceChecker.assertRowsEquivalent(
      wireFormatRow,
      protoToRowRow,
      sparkSchema,
      Some(descriptor),
      EquivalenceOptions.default,
      "root"
    )
  }

  test("Both parsers should handle map fields correctly") {
    val wireFormatParser = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)
    val protoToRowParser = ProtoToRowGenerator.generateParser(descriptor, classOf[DomDocument], sparkSchema)

    val wireFormatRow = wireFormatParser.parse(binaryData)
    val protoToRowRow = protoToRowParser.parse(binaryData)

    // Find the meta_tags field (should be a map represented as array of structs)
    val metaTagsFieldIndex = sparkSchema.fieldIndex("meta_tags")
    val wireFormatMetaTags = wireFormatRow.getArray(metaTagsFieldIndex)
    val protoToRowMetaTags = protoToRowRow.getArray(metaTagsFieldIndex)

    // Both should have the same number of map entries
    wireFormatMetaTags.numElements() shouldBe protoToRowMetaTags.numElements()

    // Compare map entries (both should be arrays of structs with key/value fields)
    val mapSchema = sparkSchema.fields(metaTagsFieldIndex).dataType.asInstanceOf[ArrayType]
      .elementType.asInstanceOf[StructType]

    for (i <- 0 until wireFormatMetaTags.numElements()) {
      val wireFormatEntry = wireFormatMetaTags.getStruct(i, mapSchema.size)
      val protoToRowEntry = protoToRowMetaTags.getStruct(i, mapSchema.size)

      // Compare key and value fields using the equivalence checker
      RowEquivalenceChecker.assertRowsEquivalent(
        wireFormatEntry,
        protoToRowEntry,
        mapSchema,
        Some(descriptor),
        EquivalenceOptions.default,
        s"meta_tags[$i]"
      )
    }
  }

  test("Both parsers should handle nested DomNode structures equivalently") {
    val wireFormatParser = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)
    val protoToRowParser = ProtoToRowGenerator.generateParser(descriptor, classOf[DomDocument], sparkSchema)

    val wireFormatRow = wireFormatParser.parse(binaryData)
    val protoToRowRow = protoToRowParser.parse(binaryData)

    // Navigate to the root DomNode
    val rootFieldIndex = sparkSchema.fieldIndex("root")
    val wireFormatRoot = wireFormatRow.getStruct(rootFieldIndex, sparkSchema.fields(rootFieldIndex).dataType.asInstanceOf[StructType].size)
    val protoToRowRoot = protoToRowRow.getStruct(rootFieldIndex, sparkSchema.fields(rootFieldIndex).dataType.asInstanceOf[StructType].size)

    val rootSchema = sparkSchema.fields(rootFieldIndex).dataType.asInstanceOf[StructType]
    RowEquivalenceChecker.assertRowsEquivalent(
      wireFormatRoot,
      protoToRowRoot,
      rootSchema,
      Some(descriptor),
      EquivalenceOptions.default,
      "root"
    )
  }

}