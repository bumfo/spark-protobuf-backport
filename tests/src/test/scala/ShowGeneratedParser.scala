import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Message
import fastproto.{InlineParserGenerator, InlineParserToRowGenerator, RecursiveSchemaConverters}
import org.apache.spark.sql.types._
import testproto.AllTypesProtos._
import testproto.EdgeCasesProtos._
import testproto.NestedProtos._
import benchmark.SimpleBenchmarkProtos._
import benchmark.ComplexBenchmarkProtos._
import benchmark.ScalarBenchmarkProtos._
import benchmark.DomBenchmarkProtos._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

object ShowGeneratedParser {

  // Available message types from tests
  private val testMessages = Map(
    "AllRepeatedTypes" -> AllRepeatedTypes.getDescriptor,
    "AllUnpackedRepeatedTypes" -> AllUnpackedRepeatedTypes.getDescriptor,
    "AllPrimitiveTypes" -> AllPrimitiveTypes.getDescriptor,
    "CompleteMessage" -> CompleteMessage.getDescriptor,
    "Nested" -> Nested.getDescriptor,
    "Recursive" -> Recursive.getDescriptor,
    "MutualA" -> MutualA.getDescriptor,
    "MutualB" -> MutualB.getDescriptor,
    "EmptyMessage" -> EmptyMessage.getDescriptor,
    "SingleField" -> SingleField.getDescriptor,
    "SingleIntField" -> SingleIntField.getDescriptor,
  )

  // Available message types from bench
  private val benchMessages = Map(
    "SimpleMessage" -> SimpleMessage.getDescriptor,
    "ComplexMessageA" -> ComplexMessageA.getDescriptor,
    "ComplexMessageB" -> ComplexMessageB.getDescriptor,
    "ScalarMessage" -> ScalarMessage.getDescriptor,
    "ScalarArrayMessage" -> ScalarArrayMessage.getDescriptor,
    "ScalarArrayUnpackedMessage" -> ScalarArrayUnpackedMessage.getDescriptor,
    "DomDocument" -> DomDocument.getDescriptor,
    "DomNode" -> DomNode.getDescriptor,
    "DomStatistics" -> DomStatistics.getDescriptor,
  )

  private val availableMessages = testMessages ++ benchMessages

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      printUsage()
      sys.exit(0)
    }

    val messageName = args(0)
    val showCompiled = args.contains("--compiled")
    val customSchema = parseCustomSchema(args)

    val descriptor = availableMessages.getOrElse(messageName, {
      println(s"Error: Unknown message type '$messageName'")
      printUsage()
      sys.exit(1)
    })

    val schema = customSchema.getOrElse(
      RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
    )

    val className = s"GeneratedInlineParser_$messageName"

    if (showCompiled) {
      // Show the actual compiled code using InlineParserToRowGenerator
      println("\n" + "="*80)
      println(s"Compiled InlineParser Code for $messageName")
      println("="*80)
      println("\nGenerating and compiling parser...")

      val parser = InlineParserToRowGenerator.generateParser(descriptor, schema)
      val parserClass = parser.getClass

      println(s"Parser class: ${parserClass.getName}")
      println(s"Schema hash: ${schema.hashCode()}")
      println(s"\nNote: The compiled bytecode is loaded via Janino. Source code generation:")
      println("="*80)
    }

    val code = InlineParserGenerator.generateParser(className, descriptor, schema)

    println("\n" + "="*80)
    println(s"Generated InlineParser Code for $messageName")
    println(s"Schema: ${if (customSchema.isDefined) "Custom (pruned)" else "Full"}")
    println("="*80)
    println(code)
    println("="*80 + "\n")

    // Save to file
    val suffix = if (customSchema.isDefined) "_pruned" else ""
    val outputPath = s"/tmp/generated_inline_parser_${messageName.toLowerCase}${suffix}.txt"
    Files.write(Paths.get(outputPath), code.getBytes(StandardCharsets.UTF_8))
    println(s"Generated code saved to: $outputPath\n")
  }

  private def parseCustomSchema(args: Array[String]): Option[StructType] = {
    // Look for --schema argument followed by schema definition
    args.indexOf("--schema") match {
      case -1 => None
      case idx if idx + 1 < args.length =>
        val schemaArg = args(idx + 1)
        // Support predefined pruned schemas
        schemaArg match {
          case "dom_pruned" => Some(createDomPrunedSchema())
          case _ => None
        }
      case _ => None
    }
  }

  private def createDomPrunedSchema(): StructType = {
    // Deeply nested pruned schema: root.children[].children[].children[].tag_name
    val level4Schema = StructType(Seq(
      StructField("tag_name", StringType, nullable = false)
    ))
    val level3Schema = StructType(Seq(
      StructField("children", ArrayType(level4Schema), nullable = true)
    ))
    val level2Schema = StructType(Seq(
      StructField("children", ArrayType(level3Schema), nullable = true)
    ))
    val level1Schema = StructType(Seq(
      StructField("children", ArrayType(level2Schema), nullable = true)
    ))
    StructType(Seq(
      StructField("root", level1Schema, nullable = true)
    ))
  }

  private def printUsage(): Unit = {
    println("\nUsage: sbt \"testOnly ShowGeneratedParser <MessageName> [--schema <schema>] [--compiled]\"")
    println("\nOptions:")
    println("  --schema dom_pruned   Use pruned DOM schema (root.children[].children[].children[].tag_name)")
    println("  --compiled            Show info about the compiled parser class")
    println("\nAvailable test messages:")
    testMessages.keys.toSeq.sorted.foreach(name => println(s"  - $name"))
    println("\nAvailable bench messages:")
    benchMessages.keys.toSeq.sorted.foreach(name => println(s"  - $name"))
    println()
  }
}
