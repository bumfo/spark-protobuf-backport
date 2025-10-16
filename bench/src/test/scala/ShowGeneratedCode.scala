import com.google.protobuf.Descriptors.Descriptor
import fastproto.{InlineParserGenerator, InlineParserToRowGenerator, RecursiveSchemaConverters, StreamWireParser}
import org.apache.spark.sql.types._
import testproto.AllTypesProtos._
import testproto.EdgeCasesProtos._
import testproto.NestedProtos._
import benchmark.SimpleBenchmarkProtos._
import benchmark.ComplexBenchmarkProtos._
import benchmark.ScalarBenchmarkProtos._
import benchmark.DomBenchmarkProtos._
import benchmark.MultiwayTreeProtos._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import java.lang.reflect.Field

object ShowGeneratedCode {

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
    "Tree" -> Tree.getDescriptor,
    "Node" -> Node.getDescriptor,
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

    // Use InlineParserToRowGenerator helper for consistent class name
    val className = InlineParserToRowGenerator.generateClassName(descriptor, schema)

    if (!showCompiled) {
      // Show standalone generated source preview when not in compiled mode
      val generatedSource = InlineParserGenerator.generateParser(className, descriptor, schema)

      println("\n" + "="*80)
      println(s"Generated InlineParser Source for $messageName")
      println(s"Schema: ${if (customSchema.isDefined) "Custom (pruned)" else "Full"}")
      println("="*80)
      println(generatedSource)
      println("="*80 + "\n")

      // Save source to file
      val suffix = if (customSchema.isDefined) "_pruned" else ""
      val sourceOutputPath = s"/tmp/generated_inline_parser_${messageName.toLowerCase}${suffix}.java"
      Files.write(Paths.get(sourceOutputPath), generatedSource.getBytes(StandardCharsets.UTF_8))
      println(s"Generated source saved to: $sourceOutputPath\n")
    } else {
      // Compile and show the actual compiled parser details (ground truth)
      println("\n" + "="*80)
      println(s"Compiled InlineParser Details for $messageName")
      println("="*80)
      println("\nGenerating and compiling parser via InlineParserToRowGenerator...")

      val parser = InlineParserToRowGenerator.generateParser(descriptor, schema)

      // Collect all parsers in the dependency tree
      val allParsers = collectAllParsers(parser)

      println(s"\nTotal parsers generated: ${allParsers.size}")
      println(s"Root parser: ${parser.getClass.getName}")
      println("\n" + "="*80)

      // Show each parser and its generated code
      allParsers.zipWithIndex.foreach { case (p, idx) =>
        val parserClass = p.getClass
        val parserDescriptor = getDescriptorForParser(parserClass.getSimpleName)
        val parserSchema = getSchemaForParser(p)

        println(s"\n[$idx] Parser: ${parserClass.getName}")
        println(s"    Schema hash: ${parserSchema.hashCode()}")

        // Show nested parser fields if any
        val parserFields = parserClass.getDeclaredFields.filter { field =>
          field.getName.startsWith("parser_")
        }

        if (parserFields.nonEmpty) {
          println(s"    Nested parsers (${parserFields.length}):")
          parserFields.foreach { field =>
            field.setAccessible(true)
            val value = field.get(p)
            val valueStr = if (value == null) "null" else value.getClass.getName
            println(s"      ${field.getName}: $valueStr")
          }
        }

        // Generate and show the source code for this parser
        if (parserDescriptor.isDefined) {
          val desc = parserDescriptor.get
          val className = parserClass.getSimpleName
          val generatedCode = InlineParserGenerator.generateParser(className, desc, parserSchema)

          println(s"\n    Generated source code:")
          println("    " + "-"*76)
          generatedCode.split("\n").foreach(line => println(s"    $line"))
          println("    " + "-"*76)
        }

        println()
      }

      // Show instance-level parser graph
      println("\n" + "="*80)
      println("Parser Instance Graph:")
      println("="*80)

      // Build instance ID map first
      val instanceIdMap = buildInstanceIdMap(parser)
      printParserGraph(parser, instanceIdMap)
      println("="*80)

      // Show the root schema structure (skip for recursive schemas to avoid StackOverflowError)
      println("\n" + "="*80)
      println(s"Root Schema structure for $messageName:")
      println("="*80)
      try {
        println(schema.treeString)
      } catch {
        case _: StackOverflowError =>
          println("(Skipped: recursive schema causes StackOverflowError in Spark's treeString)")
      }
      println("="*80 + "\n")
    }
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
          case "dom_pruned_4level" => Some(createDomPrunedSchema4Level())
          case "simple_pruned" => Some(createSimplePrunedSchema())
          case "complex_pruned" => Some(createComplexPrunedSchema())
          case "diff_pruned" => Some(createDiffPrunedSchema())
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

  private def createDomPrunedSchema4Level(): StructType = {
    // Same as dom_pruned, just an alias for clarity
    createDomPrunedSchema()
  }

  private def createSimplePrunedSchema(): StructType = {
    // Single scalar field from middle of SimpleMessage
    StructType(Seq(
      StructField("field_double_055", DoubleType, nullable = false)
    ))
  }

  private def createComplexPrunedSchema(): StructType = {
    // Nested scalar: message_b.nested_data.count
    val nestedDataSchema = StructType(Seq(
      StructField("count", IntegerType, nullable = false)
    ))
    val messageBSchema = StructType(Seq(
      StructField("nested_data", nestedDataSchema, nullable = true)
    ))
    StructType(Seq(
      StructField("message_b", messageBSchema, nullable = true)
    ))
  }

  private def createDiffPrunedSchema(): StructType = {
    // Differential pruning test schema (from TestDifferentialPruning)
    // root.left.right[0].right[0].left.value AND root.right[0].payload
    val level4Schema = StructType(Seq(
      StructField("value", IntegerType, nullable = false)
    ))
    val level3Schema = StructType(Seq(
      StructField("left", level4Schema, nullable = true)
    ))
    val level2Schema = StructType(Seq(
      StructField("right", ArrayType(level3Schema), nullable = true)
    ))
    val level1LeftSchema = StructType(Seq(
      StructField("right", ArrayType(level2Schema), nullable = true)
    ))
    val level1RightSchema = StructType(Seq(
      StructField("payload", StringType, nullable = false)
    ))
    val rootSchema = StructType(Seq(
      StructField("left", level1LeftSchema, nullable = true),
      StructField("right", ArrayType(level1RightSchema), nullable = true)
    ))
    StructType(Seq(
      StructField("root", rootSchema, nullable = true)
    ))
  }

  /**
   * Build a map from parser instances to sequential IDs (0, 1, 2, ...).
   * Traverses all reachable parsers to assign consistent IDs.
   */
  private def buildInstanceIdMap(root: StreamWireParser): Map[Int, Int] = {
    val instanceIds = scala.collection.mutable.Map[Int, Int]()
    val visited = scala.collection.mutable.Set[Int]()
    var nextId = 0

    def visit(parser: StreamWireParser): Unit = {
      val identityHash = System.identityHashCode(parser)
      if (!visited.contains(identityHash)) {
        visited.add(identityHash)
        instanceIds(identityHash) = nextId
        nextId += 1

        // Visit nested parsers
        val parserFields = parser.getClass.getDeclaredFields
          .filter(f => f.getName.startsWith("parser_") && classOf[StreamWireParser].isAssignableFrom(f.getType))

        parserFields.foreach { field =>
          field.setAccessible(true)
          val nestedParser = field.get(parser).asInstanceOf[StreamWireParser]
          if (nestedParser != null) {
            visit(nestedParser)
          }
        }
      }
    }

    visit(root)
    instanceIds.toMap
  }

  /**
   * Print a tree-style graph showing parser instance linkages.
   * Shows how parser instances are connected through their nested parser fields.
   */
  private def printParserGraph(
      root: StreamWireParser,
      instanceIdMap: Map[Int, Int],
      prefix: String = "",
      visited: scala.collection.mutable.Set[Int] = scala.collection.mutable.Set()
  ): Unit = {
    val identityHash = System.identityHashCode(root)
    val instanceId = instanceIdMap(identityHash)
    val className = root.getClass.getName.stripPrefix("fastproto.generated.")
    val schemaHash = getSchemaForParser(root).hashCode()

    // Mark if we've seen this instance before (cycle detection)
    val cycleMarker = if (visited.contains(identityHash)) " [CYCLE]" else ""

    println(s"${prefix}#${instanceId} ${className} (schema: ${schemaHash})${cycleMarker}")

    // Don't traverse cycles
    if (visited.contains(identityHash)) {
      return
    }
    visited.add(identityHash)

    // Find nested parser fields
    val parserFields = root.getClass.getDeclaredFields
      .filter(f => f.getName.startsWith("parser_") && classOf[StreamWireParser].isAssignableFrom(f.getType))
      .sortBy(_.getName)

    parserFields.zipWithIndex.foreach { case (field, idx) =>
      field.setAccessible(true)
      val nestedParser = field.get(root).asInstanceOf[StreamWireParser]

      val isLast = idx == parserFields.length - 1
      val connector = if (isLast) "└─ " else "├─ "
      val childPrefix = prefix + (if (isLast) "   " else "│  ")

      if (nestedParser != null) {
        print(s"${prefix}${connector}${field.getName}: ")
        printParserGraph(nestedParser, instanceIdMap, childPrefix, visited)
      } else {
        println(s"${prefix}${connector}${field.getName}: null")
      }
    }
  }

  /**
   * Recursively collect all UNIQUE parser classes in the dependency tree.
   * Shows one instance per unique class (for deduplication after canonical key optimization).
   */
  private def collectAllParsers(parser: StreamWireParser): Seq[StreamWireParser] = {
    val visitedClasses = scala.collection.mutable.Set[String]()
    val visitedInstances = scala.collection.mutable.Set[Int]() // Track by identity hash
    val result = scala.collection.mutable.ArrayBuffer[StreamWireParser]()

    def visit(p: StreamWireParser): Unit = {
      val instanceId = System.identityHashCode(p)
      if (!visitedInstances.contains(instanceId)) {
        visitedInstances.add(instanceId)

        val className = p.getClass.getName
        if (!visitedClasses.contains(className)) {
          visitedClasses.add(className)
          result += p
        }

        // Find nested parsers
        val parserFields = p.getClass.getDeclaredFields.filter { field =>
          field.getName.startsWith("parser_") && classOf[StreamWireParser].isAssignableFrom(field.getType)
        }

        parserFields.foreach { field =>
          field.setAccessible(true)
          val nestedParser = field.get(p).asInstanceOf[StreamWireParser]
          if (nestedParser != null) {
            visit(nestedParser)
          }
        }
      }
    }

    visit(parser)
    result.toSeq
  }

  /**
   * Extract descriptor from parser class name
   * Format: GeneratedInlineParser_<MessageType>_<CacheKeyHash>
   */
  private def getDescriptorForParser(className: String): Option[Descriptor] = {
    // Extract message type from class name
    // Example: GeneratedInlineParser_DomNode_2671599217 -> DomNode
    val pattern = "GeneratedInlineParser_(.+)_\\d+".r
    className match {
      case pattern(messageType) =>
        // Handle package prefixes (benchmark_DomNode -> DomNode)
        val simpleName = if (messageType.contains("_")) {
          messageType.substring(messageType.lastIndexOf("_") + 1)
        } else {
          messageType
        }

        // Try to find descriptor by simple name first
        val simpleNameMatch = availableMessages.get(simpleName)
        if (simpleNameMatch.isDefined) {
          return simpleNameMatch
        }

        // Try to match by descriptor name or full name
        availableMessages.values.find(_.getName == simpleName)
          .orElse(availableMessages.values.find(_.getFullName.endsWith("." + simpleName)))
          .orElse(availableMessages.values.find(_.getFullName == simpleName))
      case _ => None
    }
  }

  /**
   * Extract schema from parser using reflection
   */
  private def getSchemaForParser(parser: StreamWireParser): StructType = {
    // schema is a val in BufferSharingParser
    val bufferSharingParserClass = Class.forName("fastproto.BufferSharingParser")
    val schemaMethod = bufferSharingParserClass.getMethod("schema")
    schemaMethod.invoke(parser).asInstanceOf[StructType]
  }

  private def printUsage(): Unit = {
    println("\nUsage: sbt \"showGeneratedCode <MessageName> [--schema <schema>] [--compiled]\"")
    println("\nOptions:")
    println("  --schema dom_pruned        Use pruned DOM schema (root.children[].children[].children[].tag_name)")
    println("  --schema simple_pruned     Use pruned SimpleMessage schema (field_double_055)")
    println("  --schema complex_pruned    Use pruned ComplexMessageA schema (message_b.nested_data.count)")
    println("  --compiled                 Show compiled parser details and ALL generated code for dependency tree")
    println("\nAvailable test messages:")
    testMessages.keys.toSeq.sorted.foreach(name => println(s"  - $name"))
    println("\nAvailable bench messages:")
    benchMessages.keys.toSeq.sorted.foreach(name => println(s"  - $name"))
    println("\nExamples:")
    println("  sbt \"showGeneratedCode DomDocument --schema dom_pruned --compiled\"")
    println("  sbt \"showGeneratedCode ScalarArrayMessage\"")
    println("  sbt \"showGeneratedCode ComplexMessageA --schema complex_pruned\"")
    println()
  }
}
