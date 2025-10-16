import org.apache.spark.sql.types._
import fastproto.InlineParserToRowGenerator
import benchmark.MultiwayTreeProtos.{Tree, Node}

object TestDifferentialPruning {
  def main(args: Array[String]): Unit = {
    println("="*80)
    println("Testing differential pruning with same descriptor in multiple fields")
    println("="*80)

    // Create pruned schema for:
    // - root.left.right[0].left.value (Node appears 3 times with different pruning)
    // - root.right[0].payload (Node appears once with different pruning)

    // Level 4: Node with only value field
    val level4Schema = StructType(Seq(
      StructField("value", IntegerType, nullable = false)
    ))

    // Level 3: Node with only left field (points to level 4)
    val level3Schema = StructType(Seq(
      StructField("left", level4Schema, nullable = true)
    ))

    // Level 2: Node with only right[] field (points to level 3)
    val level2Schema = StructType(Seq(
      StructField("right", ArrayType(level3Schema), nullable = true)
    ))

    // Level 1 has TWO Node fields with DIFFERENT pruning:
    // - left: Node with right[] field (points to level 2)
    // - right[]: Node with only payload field
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

    val treeSchema = StructType(Seq(
      StructField("root", rootSchema, nullable = true)
    ))

    println("\nPruned schema:")
    println(treeSchema.treeString)

    // Test: Generate parser for Tree with this pruned schema
    val treeDescriptor = Tree.getDescriptor

    println("\nGenerating parser...")
    try {
      val parser = InlineParserToRowGenerator.generateParser(treeDescriptor, treeSchema)
      println("✓ Parser generated successfully!")
      println(s"  Root parser class: ${parser.getClass.getName}")

      // Examine Tree parser's nested parsers
      val treeParserFields = parser.getClass.getDeclaredFields.filter(_.getName.startsWith("parser_"))
      println(s"\n  Tree parser has ${treeParserFields.length} nested parser(s):")
      treeParserFields.foreach { field =>
        field.setAccessible(true)
        val value = field.get(parser)
        println(s"    ${field.getName}: ${if (value == null) "null" else value.getClass.getName}")
      }

      // The critical test: Does root.Node parser have TWO different nested parsers
      // for left (pruned to right[]) and right[] (pruned to payload)?

      // Get the root.Node parser
      val nodeParserField = treeParserFields.find(_.getName == "parser_root_Node")
      if (nodeParserField.isDefined) {
        nodeParserField.get.setAccessible(true)
        val nodeParser = nodeParserField.get.get(parser)

        println(s"\n  Examining root.Node parser:")
        println(s"    Class: ${nodeParser.getClass.getName}")

        val nodeParserFields = nodeParser.getClass.getDeclaredFields.filter(_.getName.startsWith("parser_"))
        println(s"    Nested parsers: ${nodeParserFields.length}")
        nodeParserFields.foreach { field =>
          field.setAccessible(true)
          val value = field.get(nodeParser)
          println(s"      ${field.getName}: ${if (value == null) "null" else value.getClass.getName}")
        }

        // BUG CHECK: Root.Node has TWO Node fields (left and right[]) with DIFFERENT schemas
        // After fix, there should be TWO nested parser fields: parser_left_Node and parser_right_Node

        if (nodeParserFields.length == 1) {
          println("\n  ⚠️  BUG: Only ONE nested parser found!")
          println("      root.Node has left and right[] fields with different pruning")
          println("      They should have DIFFERENT nested parser instances!")
        } else if (nodeParserFields.length == 2) {
          println(s"\n  ✓ FIXED: Found ${nodeParserFields.length} nested parsers!")
          println("      Differential pruning is working correctly.")
        } else {
          println(s"\n  ? Unexpected: Found ${nodeParserFields.length} nested parsers")
        }
      } else {
        println("\n  ⚠️  ERROR: Could not find parser_root_Node field!")
      }

    } catch {
      case e: Exception =>
        println(s"✗ ERROR: ${e.getClass.getSimpleName}: ${e.getMessage}")
        e.printStackTrace()
    }

    println("\n" + "="*80)
  }
}
