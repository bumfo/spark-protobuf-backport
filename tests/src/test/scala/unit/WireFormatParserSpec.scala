package unit

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Message
import fastproto.{Parser, WireFormatParser}
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for WireFormatParser (direct implementation).
 * Tests all protobuf features including sint32/sint64, nested messages, maps, and edge cases.
 * Nullability is validated as part of each test scenario.
 */
class WireFormatParserSpec extends AnyFlatSpec with Matchers with ParserBehaviors {

  private def createParser(descriptor: Descriptor, schema: StructType, messageClass: Option[Class[_ <: Message]]): Parser = {
    new WireFormatParser(descriptor, schema)
  }

  "WireFormatParser" should behave like primitiveTypeParser(createParser)
  it should behave like repeatedFieldParser(createParser)
  it should behave like unpackedRepeatedFieldParser(createParser)
  it should behave like nestedMessageParser(createParser)
  it should behave like mapFieldParser(createParser)
  it should behave like edgeCaseParser(createParser)
}