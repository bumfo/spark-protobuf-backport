package benchmark

import benchmark.DomBenchmarkProtos.DomDocument
import fastproto.{EquivalenceOptions, ProtoToRowGenerator, RowEquivalenceChecker, WireFormatToRowGenerator}
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

  private val shallowDom = DomTestDataGenerator.createDomDocument(maxDepth = 1, breadthFactor = 1)
  private val binaryData = shallowDom.toByteArray
  private val descriptor = shallowDom.getDescriptorForType

  // ProtoToRowParser uses string representation for enums (current behavior)
  private val protoToRowSchema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = false)

  // WireFormatParser stores enum values as integers - schema should reflect this
  private val wireFormatSchema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)

  ignore("ProtoToRowGenerator and WireFormatToRowGenerator should produce equivalent results for shallow DOM") {
    val wireFormatParser = WireFormatToRowGenerator.generateParser(descriptor, wireFormatSchema)
    val protoToRowParser = ProtoToRowGenerator.generateParser(descriptor, classOf[DomDocument], protoToRowSchema)

    val wireFormatRow = wireFormatParser.parse(binaryData)
    val protoToRowRow = protoToRowParser.parse(binaryData)

    // Both should produce the same number of fields
    wireFormatRow.numFields shouldBe protoToRowRow.numFields

    // Compare all fields systematically using the equivalence checker
    RowEquivalenceChecker.assertRowsEquivalent(
      wireFormatRow,
      wireFormatSchema,
      protoToRowRow,
      protoToRowSchema,
      Some(descriptor),
      EquivalenceOptions.default,
      "root"
    )
  }

  ignore("Both parsers should handle map fields correctly") {
    val wireFormatParser = WireFormatToRowGenerator.generateParser(descriptor, wireFormatSchema)
    val protoToRowParser = ProtoToRowGenerator.generateParser(descriptor, classOf[DomDocument], protoToRowSchema)

    val wireFormatRow = wireFormatParser.parse(binaryData)
    val protoToRowRow = protoToRowParser.parse(binaryData)

    // Find the meta_tags field (should be a map represented as array of structs)
    val wireFormatMetaTagsFieldIndex = wireFormatSchema.fieldIndex("meta_tags")
    val protoToRowMetaTagsFieldIndex = protoToRowSchema.fieldIndex("meta_tags")
    val wireFormatMetaTags = wireFormatRow.getArray(wireFormatMetaTagsFieldIndex)
    val protoToRowMetaTags = protoToRowRow.getArray(protoToRowMetaTagsFieldIndex)

    // Both should have the same number of map entries
    wireFormatMetaTags.numElements() shouldBe protoToRowMetaTags.numElements()

    // Compare map entries (both should be arrays of structs with key/value fields)
    val wireFormatMapSchema = wireFormatSchema.fields(wireFormatMetaTagsFieldIndex).dataType.asInstanceOf[ArrayType]
      .elementType.asInstanceOf[StructType]
    val protoToRowMapSchema = protoToRowSchema.fields(protoToRowMetaTagsFieldIndex).dataType.asInstanceOf[ArrayType]
      .elementType.asInstanceOf[StructType]

    for (i <- 0 until wireFormatMetaTags.numElements()) {
      val wireFormatEntry = wireFormatMetaTags.getStruct(i, wireFormatMapSchema.size)
      val protoToRowEntry = protoToRowMetaTags.getStruct(i, protoToRowMapSchema.size)

      // Compare key and value fields using the equivalence checker
      RowEquivalenceChecker.assertRowsEquivalent(
        wireFormatEntry,
        wireFormatMapSchema,
        protoToRowEntry,
        protoToRowMapSchema,
        Some(descriptor),
        EquivalenceOptions.default,
        s"meta_tags[$i]"
      )
    }
  }

  ignore("Both parsers should handle nested DomNode structures equivalently") {
    val wireFormatParser = WireFormatToRowGenerator.generateParser(descriptor, wireFormatSchema)
    val protoToRowParser = ProtoToRowGenerator.generateParser(descriptor, classOf[DomDocument], protoToRowSchema)

    val wireFormatRow = wireFormatParser.parse(binaryData)
    val protoToRowRow = protoToRowParser.parse(binaryData)

    // Navigate to the root DomNode
    val wireFormatRootFieldIndex = wireFormatSchema.fieldIndex("root")
    val protoToRowRootFieldIndex = protoToRowSchema.fieldIndex("root")
    val wireFormatRoot = wireFormatRow.getStruct(wireFormatRootFieldIndex, wireFormatSchema.fields(wireFormatRootFieldIndex).dataType.asInstanceOf[StructType].size)
    val protoToRowRoot = protoToRowRow.getStruct(protoToRowRootFieldIndex, protoToRowSchema.fields(protoToRowRootFieldIndex).dataType.asInstanceOf[StructType].size)

    val wireFormatRootSchema = wireFormatSchema.fields(wireFormatRootFieldIndex).dataType.asInstanceOf[StructType]
    val protoToRowRootSchema = protoToRowSchema.fields(protoToRowRootFieldIndex).dataType.asInstanceOf[StructType]

    // Get the nested DomNode descriptor for the root field
    val rootFieldDescriptor = descriptor.findFieldByName("root")
    val domNodeDescriptor = rootFieldDescriptor.getMessageType

    RowEquivalenceChecker.assertRowsEquivalent(
      wireFormatRoot,
      wireFormatRootSchema,
      protoToRowRoot,
      protoToRowRootSchema,
      Some(domNodeDescriptor),
      EquivalenceOptions.default,
      "root"
    )
  }

}