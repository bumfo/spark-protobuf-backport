package properties

import com.google.protobuf.Message
import fastproto.{EquivalenceOptions, InlineParserToRowGenerator, RecursiveSchemaConverters, RowEquivalenceChecker, WireFormatParser, WireFormatToRowGenerator}
import org.apache.spark.sql.catalyst.InternalRow
import org.scalacheck.Prop.forAll
import org.scalacheck.Properties
import org.scalatest.Tag
import testproto.AllTypesProtos._
import testproto.Generators

/**
 * Property-based tests for parser equivalence.
 *
 * Verifies that different parser implementations produce equivalent InternalRows
 * for the same binary protobuf input.
 *
 * Test scope:
 * - WireFormatParser (direct implementation) vs GeneratedWireFormatParser (Janino codegen)
 * - InlineParser (Janino codegen) vs WireFormatParser (direct implementation)
 * - Note: GeneratedMessageParser and DynamicMessageParser have known limitations
 *   and are not included in equivalence testing
 */
object ParserEquivalenceProperties extends Properties("ParserEquivalence") {

  // Tag for property-based tests
  object Property extends Tag("Property")

  /**
   * Test that WireFormatParser and GeneratedWireFormatParser produce equivalent results
   * for primitive types.
   */
  property("parsers agree on AllPrimitiveTypes") = forAll(Generators.genAnyPrimitives) { message: AllPrimitiveTypes =>
    val binary = message.toByteArray
    val descriptor = message.getDescriptorForType
    val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)

    val wireParser = new WireFormatParser(descriptor, schema)
    val generatedParser = WireFormatToRowGenerator.generateParser(descriptor, schema)

    val wireRow = wireParser.parse(binary)
    val generatedRow = generatedParser.parse(binary)

    // Use RowEquivalenceChecker for structural equivalence (handles enum int vs string)
    try {
      RowEquivalenceChecker.assertRowsEquivalent(wireRow, generatedRow, schema, Some(descriptor))
      true
    } catch {
      case e: Exception =>
        println(s"Parser equivalence failed: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }

  /**
   * Test that parsers agree on repeated fields (packed encoding).
   */
  property("parsers agree on AllRepeatedTypes (packed)") = forAll(Generators.genFullRepeated) { message: AllRepeatedTypes =>
    val binary = message.toByteArray
    val descriptor = message.getDescriptorForType
    val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)

    val wireParser = new WireFormatParser(descriptor, schema)
    val generatedParser = WireFormatToRowGenerator.generateParser(descriptor, schema)

    val wireRow = wireParser.parse(binary)
    val generatedRow = generatedParser.parse(binary)

    try {
      RowEquivalenceChecker.assertRowsEquivalent(wireRow, generatedRow, schema, Some(descriptor))
      true
    } catch {
      case e: Exception =>
        println(s"Parser equivalence failed: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }

  /**
   * Test that parsers agree on unpacked repeated fields.
   */
  property("parsers agree on AllUnpackedRepeatedTypes") = forAll(Generators.genFullUnpackedRepeated) { message: AllUnpackedRepeatedTypes =>
    val binary = message.toByteArray
    val descriptor = message.getDescriptorForType
    val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)

    val wireParser = new WireFormatParser(descriptor, schema)
    val generatedParser = WireFormatToRowGenerator.generateParser(descriptor, schema)

    val wireRow = wireParser.parse(binary)
    val generatedRow = generatedParser.parse(binary)

    try {
      RowEquivalenceChecker.assertRowsEquivalent(wireRow, generatedRow, schema, Some(descriptor))
      true
    } catch {
      case e: Exception =>
        println(s"Parser equivalence failed: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }

  /**
   * Test that parsers agree on sparse messages (randomly omitted fields).
   */
  property("parsers agree on sparse messages") = forAll(Generators.genSparsePrimitives) { message: AllPrimitiveTypes =>
    val binary = message.toByteArray
    val descriptor = message.getDescriptorForType
    val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)

    val wireParser = new WireFormatParser(descriptor, schema)
    val generatedParser = WireFormatToRowGenerator.generateParser(descriptor, schema)

    val wireRow = wireParser.parse(binary)
    val generatedRow = generatedParser.parse(binary)

    try {
      RowEquivalenceChecker.assertRowsEquivalent(wireRow, generatedRow, schema, Some(descriptor))
      true
    } catch {
      case e: Exception =>
        println(s"Parser equivalence failed: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }

  /**
   * Test that parsers agree on complete messages with nested structures.
   */
  property("parsers agree on CompleteMessage") = forAll(Generators.genCompleteMessage) { message: CompleteMessage =>
    val binary = message.toByteArray
    val descriptor = message.getDescriptorForType
    val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)

    val wireParser = new WireFormatParser(descriptor, schema)
    val generatedParser = WireFormatToRowGenerator.generateParser(descriptor, schema)

    val wireRow = wireParser.parse(binary)
    val generatedRow = generatedParser.parse(binary)

    try {
      RowEquivalenceChecker.assertRowsEquivalent(wireRow, generatedRow, schema, Some(descriptor))
      true
    } catch {
      case e: Exception =>
        println(s"Parser equivalence failed: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }

  /**
   * Test that InlineParser and WireFormatParser produce equivalent results
   * for primitive types.
   */
  property("InlineParser agrees with WireFormatParser on AllPrimitiveTypes") = forAll(Generators.genAnyPrimitives) { message: AllPrimitiveTypes =>
    val binary = message.toByteArray
    val descriptor = message.getDescriptorForType
    val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)

    val wireParser = new WireFormatParser(descriptor, schema)
    val inlineParser = InlineParserToRowGenerator.generateParser(descriptor, schema)

    val wireRow = wireParser.parse(binary)
    val inlineRow = inlineParser.parse(binary)

    try {
      RowEquivalenceChecker.assertRowsEquivalent(wireRow, inlineRow, schema, Some(descriptor))
      true
    } catch {
      case e: Exception =>
        println(s"InlineParser equivalence failed: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }

  /**
   * Test that InlineParser agrees on repeated fields (packed encoding).
   */
  property("InlineParser agrees with WireFormatParser on AllRepeatedTypes (packed)") = forAll(Generators.genFullRepeated) { message: AllRepeatedTypes =>
    val binary = message.toByteArray
    val descriptor = message.getDescriptorForType
    val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)

    val wireParser = new WireFormatParser(descriptor, schema)
    val inlineParser = InlineParserToRowGenerator.generateParser(descriptor, schema)

    val wireRow = wireParser.parse(binary)
    val inlineRow = inlineParser.parse(binary)

    try {
      RowEquivalenceChecker.assertRowsEquivalent(wireRow, inlineRow, schema, Some(descriptor))
      true
    } catch {
      case e: Exception =>
        println(s"InlineParser equivalence failed: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }

  /**
   * Test that InlineParser agrees on unpacked repeated fields.
   */
  property("InlineParser agrees with WireFormatParser on AllUnpackedRepeatedTypes") = forAll(Generators.genFullUnpackedRepeated) { message: AllUnpackedRepeatedTypes =>
    val binary = message.toByteArray
    val descriptor = message.getDescriptorForType
    val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)

    val wireParser = new WireFormatParser(descriptor, schema)
    val inlineParser = InlineParserToRowGenerator.generateParser(descriptor, schema)

    val wireRow = wireParser.parse(binary)
    val inlineRow = inlineParser.parse(binary)

    try {
      RowEquivalenceChecker.assertRowsEquivalent(wireRow, inlineRow, schema, Some(descriptor))
      true
    } catch {
      case e: Exception =>
        println(s"InlineParser equivalence failed: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }

  /**
   * Test that InlineParser agrees on sparse messages (randomly omitted fields).
   */
  property("InlineParser agrees with WireFormatParser on sparse messages") = forAll(Generators.genSparsePrimitives) { message: AllPrimitiveTypes =>
    val binary = message.toByteArray
    val descriptor = message.getDescriptorForType
    val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)

    val wireParser = new WireFormatParser(descriptor, schema)
    val inlineParser = InlineParserToRowGenerator.generateParser(descriptor, schema)

    val wireRow = wireParser.parse(binary)
    val inlineRow = inlineParser.parse(binary)

    try {
      RowEquivalenceChecker.assertRowsEquivalent(wireRow, inlineRow, schema, Some(descriptor))
      true
    } catch {
      case e: Exception =>
        println(s"InlineParser equivalence failed: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }

  /**
   * Test that InlineParser agrees on complete messages with nested structures.
   */
  property("InlineParser agrees with WireFormatParser on CompleteMessage") = forAll(Generators.genCompleteMessage) { message: CompleteMessage =>
    val binary = message.toByteArray
    val descriptor = message.getDescriptorForType
    val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)

    val wireParser = new WireFormatParser(descriptor, schema)
    val inlineParser = InlineParserToRowGenerator.generateParser(descriptor, schema)

    val wireRow = wireParser.parse(binary)
    val inlineRow = inlineParser.parse(binary)

    try {
      RowEquivalenceChecker.assertRowsEquivalent(wireRow, inlineRow, schema, Some(descriptor))
      true
    } catch {
      case e: Exception =>
        println(s"InlineParser equivalence failed: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }
}