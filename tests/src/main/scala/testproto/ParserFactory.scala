package testproto

import com.google.protobuf.{Descriptors, Message}
import fastproto._
import org.apache.spark.sql.protobuf.backport.DynamicMessageParser
import org.apache.spark.sql.types.StructType

/**
 * Factory for creating all 4 parser types uniformly.
 * Enables consistent testing across parser implementations.
 */
object ParserFactory {

  sealed trait ParserType {
    def name: String
  }

  object ParserType {
    case object WireFormatDirect extends ParserType {
      val name = "WireFormatParser"
    }
    case object WireFormatGenerated extends ParserType {
      val name = "Generated WireFormatParser"
    }
    case object MessageGenerated extends ParserType {
      val name = "Generated MessageParser"
    }
    case object Dynamic extends ParserType {
      val name = "DynamicMessageParser"
    }

    val all: Seq[ParserType] = Seq(
      WireFormatDirect,
      WireFormatGenerated,
      MessageGenerated,
      Dynamic
    )

    // Only parsers that work with binary data
    val binaryParsers: Seq[ParserType] = Seq(
      WireFormatDirect,
      WireFormatGenerated,
      Dynamic
    )

    // Only parsers that work with Message objects
    val messageParsers: Seq[ParserType] = Seq(
      MessageGenerated
    )
  }

  /**
   * Create parser of specified type.
   * For MessageGenerated, messageClass must be provided.
   */
  def createParser(
      parserType: ParserType,
      descriptor: Descriptors.Descriptor,
      schema: StructType,
      messageClass: Option[Class[_ <: Message]] = None
  ): Parser = {
    parserType match {
      case ParserType.WireFormatDirect =>
        new WireFormatParser(descriptor, schema)

      case ParserType.WireFormatGenerated =>
        WireFormatToRowGenerator.generateParser(descriptor, schema)

      case ParserType.MessageGenerated =>
        val msgClass = messageClass.getOrElse(
          throw new IllegalArgumentException("messageClass required for MessageGenerated parser")
        )
        ProtoToRowGenerator.generateParser(descriptor, msgClass, schema)

      case ParserType.Dynamic =>
        new DynamicMessageParser(descriptor, schema)
    }
  }

  /**
   * Create all applicable parsers for testing.
   * Returns (ParserType, Parser) pairs.
   */
  def createAllParsers(
      descriptor: Descriptors.Descriptor,
      schema: StructType,
      messageClass: Option[Class[_ <: Message]] = None
  ): Seq[(ParserType, Parser)] = {
    val parsers = Seq.newBuilder[(ParserType, Parser)]

    // Add binary parsers (always available)
    ParserType.binaryParsers.foreach { pt =>
      parsers += ((pt, createParser(pt, descriptor, schema)))
    }

    // Add message parser if class is available
    messageClass.foreach { mc =>
      parsers += ((ParserType.MessageGenerated, createParser(ParserType.MessageGenerated, descriptor, schema, Some(mc))))
    }

    parsers.result()
  }
}