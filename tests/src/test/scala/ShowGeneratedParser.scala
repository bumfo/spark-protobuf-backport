import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Message
import fastproto.{InlineParserGenerator, RecursiveSchemaConverters}
import testproto.AllTypesProtos._
import testproto.EdgeCasesProtos._
import testproto.NestedProtos._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

object ShowGeneratedParser {

  // Available message types
  private val availableMessages = Map(
    "AllRepeatedTypes" -> AllRepeatedTypes.getDescriptor,
    "AllUnpackedRepeatedTypes" -> AllUnpackedRepeatedTypes.getDescriptor,
    "AllPrimitiveTypes" -> AllPrimitiveTypes.getDescriptor,
    "CompleteMessage" -> CompleteMessage.getDescriptor,
    "Nested" -> Nested.getDescriptor,
    "Recursive" -> Recursive.getDescriptor,
    "MutualA" -> MutualA.getDescriptor,
    "MutualB" -> MutualB.getDescriptor,
    "EmptyMessage" -> EmptyMessage.getDescriptor,
    "SingleField" -> SingleField.getDescriptor
  )

  def main(args: Array[String]): Unit = {
    val messageName = if (args.nonEmpty) args(0) else "AllRepeatedTypes"

    val descriptor = availableMessages.getOrElse(messageName, {
      println(s"Error: Unknown message type '$messageName'")
      println(s"Available messages: ${availableMessages.keys.toSeq.sorted.mkString(", ")}")
      sys.exit(1)
    })

    val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
    val className = s"GeneratedInlineParser_$messageName"

    val code = InlineParserGenerator.generateParser(className, descriptor, schema)

    println("\n" + "="*80)
    println(s"Generated InlineParser Code for $messageName")
    println("="*80)
    println(code)
    println("="*80 + "\n")

    // Save to file
    val outputPath = s"/tmp/generated_inline_parser_${messageName.toLowerCase}.txt"
    Files.write(Paths.get(outputPath), code.getBytes(StandardCharsets.UTF_8))
    println(s"Generated code saved to: $outputPath\n")
  }
}
