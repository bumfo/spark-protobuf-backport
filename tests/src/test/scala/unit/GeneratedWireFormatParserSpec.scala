package unit

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Message
import fastproto.{Parser, WireFormatToRowGenerator}
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for Generated WireFormatParser (code-generated via Janino).
 * Tests all protobuf features including sint32/sint64, nested messages, maps, and edge cases.
 * Nullability is validated as part of each test scenario.
 */
class GeneratedWireFormatParserSpec extends AnyFlatSpec with Matchers with ParserBehaviors {

  private def createParser(descriptor: Descriptor, schema: StructType, messageClass: Option[Class[_ <: Message]]): Parser = {
    WireFormatToRowGenerator.generateParser(descriptor, schema)
  }

  "Generated WireFormatParser" should behave like primitiveTypeParser(createParser)
  it should behave like repeatedFieldParser(createParser)
  it should behave like unpackedRepeatedFieldParser(createParser)
  it should behave like nestedMessageParser(createParser)
  it should behave like mapFieldParser(createParser)
  it should behave like edgeCaseParser(createParser)
  it should behave like partialSchemaParser(createParser)
}