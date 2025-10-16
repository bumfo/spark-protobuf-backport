import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import fastproto.InlineParserToRowGenerator
import benchmark.MultiwayTreeProtos.{Tree, Node}

object TestDifferentialPruning {
  def main(args: Array[String]): Unit = {
    println("="*80)
    println("Testing differential pruning with same descriptor in multiple fields")
    println("="*80)

    // Create test data:
    // - root.left.right[0].left.value = 42
    // - root.right[0].payload = "test"
    // - root.right[1].payload = "data"

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

    // Build test protobuf message matching the schema structure
    val testTree = Tree.newBuilder()
      .setRoot(
        Node.newBuilder()
          .setLeft(
            Node.newBuilder()
              .addRight(
                Node.newBuilder()
                  .addRight(
                    Node.newBuilder()
                      .setLeft(
                        Node.newBuilder()
                          .setValue(42)
                          .build()
                      )
                      .build()
                  )
                  .build()
              )
              .build()
          )
          .addRight(
            Node.newBuilder()
              .setPayload("test")
              .build()
          )
          .addRight(
            Node.newBuilder()
              .setPayload("data")
              .build()
          )
          .build()
      )
      .build()

    val binaryData = testTree.toByteArray

    println("\nGenerating parser...")
    try {
      val parser = InlineParserToRowGenerator.generateParser(treeDescriptor, treeSchema)
      println("✓ Parser generated successfully!")

      // Parse the binary data
      println("\nParsing test data...")
      val row = parser.parse(binaryData)

      println(s"✓ Parsed successfully!")
      println(s"  Root row numFields: ${row.numFields} (expected 1 for 'root' field)")

      // Verify structure
      val rootRow = row.getStruct(0, 2)  // root has 2 fields: left and right[]
      println(s"\n  root.numFields: ${rootRow.numFields} (expected 2 for 'left' and 'right')")

      // Check left path: root.left.right[0].right[0].left.value
      val leftNode = rootRow.getStruct(0, 1)  // left field (index 0)
      println(s"  root.left.numFields: ${leftNode.numFields} (expected 1 for 'right' array)")

      val leftRightArray = leftNode.getArray(0)  // right[] field
      println(s"  root.left.right.length: ${leftRightArray.numElements()} (expected 1)")

      val leftRight0 = leftRightArray.getStruct(0, 1)  // right[0] (level2Schema)
      println(s"  root.left.right[0].numFields: ${leftRight0.numFields} (expected 1 for 'right' array)")

      val leftRight0RightArray = leftRight0.getArray(0)  // right[] field
      println(s"  root.left.right[0].right.length: ${leftRight0RightArray.numElements()} (expected 1)")

      val leftRight0Right0 = leftRight0RightArray.getStruct(0, 1)  // right[0] (level3Schema)
      println(s"  root.left.right[0].right[0].numFields: ${leftRight0Right0.numFields} (expected 1 for 'left')")

      val leftRight0Right0Left = leftRight0Right0.getStruct(0, 1)  // left (level4Schema)
      println(s"  root.left.right[0].right[0].left.numFields: ${leftRight0Right0Left.numFields} (expected 1 for 'value')")

      val value = leftRight0Right0Left.getInt(0)  // value field
      println(s"  root.left.right[0].right[0].left.value: $value (expected 42)")

      // Check right path: root.right[0].payload and root.right[1].payload
      val rightArray = rootRow.getArray(1)  // right[] field (index 1)
      println(s"\n  root.right.length: ${rightArray.numElements()} (expected 2)")

      val right0 = rightArray.getStruct(0, 1)  // right[0]
      println(s"  root.right[0].numFields: ${right0.numFields} (expected 1 for 'payload')")
      val payload0 = right0.getUTF8String(0).toString  // payload field
      println(s"""  root.right[0].payload: "$payload0" (expected "test")""")

      val right1 = rightArray.getStruct(1, 1)  // right[1]
      println(s"  root.right[1].numFields: ${right1.numFields} (expected 1 for 'payload')")
      val payload1 = right1.getUTF8String(0).toString  // payload field
      println(s"""  root.right[1].payload: "$payload1" (expected "data")""")

      // Verify correctness
      println("\n" + "="*80)
      if (row.numFields == 1 &&
          rootRow.numFields == 2 &&
          leftNode.numFields == 1 &&
          leftRightArray.numElements() == 1 &&
          leftRight0.numFields == 1 &&
          leftRight0RightArray.numElements() == 1 &&
          leftRight0Right0.numFields == 1 &&
          leftRight0Right0Left.numFields == 1 &&
          value == 42 &&
          rightArray.numElements() == 2 &&
          right0.numFields == 1 &&
          payload0 == "test" &&
          right1.numFields == 1 &&
          payload1 == "data") {
        println("✅ ALL TESTS PASSED!")
        println("   Differential pruning is working correctly.")
      } else {
        println("❌ TESTS FAILED!")
        println("   Parsed data does not match expected values.")
      }

    } catch {
      case e: Exception =>
        println(s"✗ ERROR: ${e.getClass.getSimpleName}: ${e.getMessage}")
        e.printStackTrace()
    }

    println("\n" + "="*80)
  }
}
