import org.apache.spark.sql.types._
import fastproto.InlineParserToRowGenerator
import benchmark.MultiwayTreeProtos.{Tree, Node}
import java.lang.reflect.Field

object TestBinaryTreeDepth5 {
  def main(args: Array[String]): Unit = {
    println("="*80)
    println("Testing full binary tree with max depth = 5")
    println("="*80)

    // Create schema for full binary tree: depth 5
    // Each level has both left and right fields
    val level5Schema = StructType(Seq(
      StructField("value", IntegerType, nullable = false)
    ))

    val level4Schema = StructType(Seq(
      StructField("left", level5Schema, nullable = true),
      StructField("right", level5Schema, nullable = true)
    ))

    val level3Schema = StructType(Seq(
      StructField("left", level4Schema, nullable = true),
      StructField("right", level4Schema, nullable = true)
    ))

    val level2Schema = StructType(Seq(
      StructField("left", level3Schema, nullable = true),
      StructField("right", level3Schema, nullable = true)
    ))

    val level1Schema = StructType(Seq(
      StructField("left", level2Schema, nullable = true),
      StructField("right", level2Schema, nullable = true)
    ))

    val treeSchema = StructType(Seq(
      StructField("root", level1Schema, nullable = true)
    ))

    println("\nFull binary tree schema (depth 5):")
    println(treeSchema.treeString)

    val treeDescriptor = Tree.getDescriptor

    println("\nGenerating parser...")
    val parser = InlineParserToRowGenerator.generateParser(treeDescriptor, treeSchema)
    println("✓ Parser generated successfully!")

    // Collect all parsers in the dependency tree
    val allParsers = collectAllParsers(parser)

    println(s"\n" + "="*80)
    println(s"Parser Statistics:")
    println("="*80)
    println(s"Total parser instances: ${allParsers.size}")

    // Group by class to see unique classes
    val parsersByClass = allParsers.groupBy(_.getClass.getName)
    println(s"Unique parser classes: ${parsersByClass.size}")

    println(s"\nClass breakdown:")
    parsersByClass.toSeq.sortBy(_._1).foreach { case (className, instances) =>
      val shortName = className.split("\\.").last
      println(s"  $shortName: ${instances.size} instance(s)")
    }

    // Show nested parser fields for each unique class
    println(s"\n" + "="*80)
    println("Parser class details:")
    println("="*80)
    parsersByClass.toSeq.sortBy(_._1).zipWithIndex.foreach { case ((className, instances), idx) =>
      val shortName = className.split("\\.").last
      val parser = instances.head
      val nestedFields = parser.getClass.getDeclaredFields.filter(f => classOf[fastproto.StreamWireParser].isAssignableFrom(f.getType))

      println(s"\n[$idx] Class: $shortName")
      println(s"    Instances: ${instances.size}")
      if (nestedFields.nonEmpty) {
        println(s"    Nested parser fields (${nestedFields.length}):")
        nestedFields.foreach { field =>
          field.setAccessible(true)
          val value = field.get(parser)
          val valueClass = if (value == null) "null" else value.getClass.getSimpleName
          println(s"      ${field.getName}: $valueClass")
        }
      } else {
        println(s"    Nested parser fields: (none - leaf parser)")
      }
    }

    println(s"\n" + "="*80)
  }

  /**
   * Recursively collect all parser instances in the dependency tree.
   */
  private def collectAllParsers(parser: fastproto.StreamWireParser): Seq[fastproto.StreamWireParser] = {
    val visitedInstances = scala.collection.mutable.Set[Int]()
    val result = scala.collection.mutable.ArrayBuffer[fastproto.StreamWireParser]()

    def visit(p: fastproto.StreamWireParser): Unit = {
      val instanceId = System.identityHashCode(p)
      if (!visitedInstances.contains(instanceId)) {
        visitedInstances.add(instanceId)
        result += p

        // Find nested parsers
        val parserFields = p.getClass.getDeclaredFields.filter { field =>
          classOf[fastproto.StreamWireParser].isAssignableFrom(field.getType)
        }

        parserFields.foreach { field =>
          field.setAccessible(true)
          val nestedParser = field.get(p).asInstanceOf[fastproto.StreamWireParser]
          if (nestedParser != null) {
            visit(nestedParser)
          }
        }
      }
    }

    visit(parser)
    result.toSeq
  }
}
