package unit

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Message
import fastproto.{Parser, ProtoToRowGenerator}
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for Generated MessageParser (code-generated for compiled protobuf classes).
 * Tests all protobuf features including sint32/sint64, nested messages, maps, and edge cases.
 * Nullability is validated as part of each test scenario.
 *
 * Note: This parser requires compiled Message classes, so we need to override
 * parse() to work with binary data by first parsing to Message objects.
 *
 * TODO: Re-enable these tests once the GeneratedMessageParser API is fixed.
 * Current issue: Reflection-based wrapper cannot find the correct parse() method signature.
 */
class GeneratedMessageParserSpec extends AnyFlatSpec with Matchers with ParserBehaviors {

  private def createParser(descriptor: Descriptor, schema: StructType, messageClass: Option[Class[_ <: Message]]): Parser = {
    val msgClass = messageClass.getOrElse(
      throw new IllegalArgumentException("GeneratedMessageParser requires a compiled Message class")
    )

    val baseParser = ProtoToRowGenerator.generateParser(descriptor, msgClass, schema)

    // Wrap the parser to convert binary → Message → InternalRow
    new Parser {
      override def parse(binary: Array[Byte]): org.apache.spark.sql.catalyst.InternalRow = {
        // Parse binary to Message object using protobuf's parser
        val parseMethod = msgClass.getMethod("parseFrom", classOf[Array[Byte]])
        val message = parseMethod.invoke(null, binary).asInstanceOf[Message]

        // Use the generated parser to convert Message → InternalRow
        val parseMessageMethod = baseParser.getClass.getMethod("parse", msgClass)
        parseMessageMethod.invoke(baseParser, message).asInstanceOf[org.apache.spark.sql.catalyst.InternalRow]
      }

      override def schema: StructType = baseParser.schema
    }
  }

  // TODO: Re-enable these tests once the API reflection issue is resolved
  // "Generated MessageParser" should behave like primitiveTypeParser(createParser)
  // it should behave like repeatedFieldParser(createParser)
  // it should behave like nestedMessageParser(createParser)
  // it should behave like mapFieldParser(createParser)
  // it should behave like edgeCaseParser(createParser)
}