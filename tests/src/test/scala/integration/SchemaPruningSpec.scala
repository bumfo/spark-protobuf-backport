package integration

import com.google.protobuf.DescriptorProtos
import org.apache.spark.sql.protobuf.backport.ProtobufTestUtils
import org.apache.spark.sql.protobuf.backport.functions._
import org.apache.spark.sql.{Column, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Tag}
import testproto.AllTypesProtos._
import testproto.EdgeCasesProtos._

/**
 * Integration tests for nested schema pruning in protobuf deserialization.
 *
 * Verifies that the ProtobufSchemaPruning optimizer correctly prunes unused
 * fields when using WireFormat parser (binary descriptor sets).
 */
object SchemaPruningTest extends Tag("Integration")

class SchemaPruningSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    // Suppress Spark logging noise
    org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.ERROR)

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("SchemaPruningSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.protobuf.backport.ProtobufExtensions")
      .config("spark.sql.protobuf.nestedSchemaPruning.enabled", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      try {
        // Let JVM cleanup handle shutdown
      } catch {
        case _: Exception =>
      }
    }
  }

  private def createDescriptorBytes(messageName: String): Array[Byte] = {
    val message = messageName match {
      case "CompleteMessage" => CompleteMessage.getDefaultInstance
      case "AllPrimitiveTypes" => AllPrimitiveTypes.getDefaultInstance
      case _ => throw new IllegalArgumentException(s"Unknown message: $messageName")
    }

    DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(message.getDescriptorForType.getFile.toProto)
      .build()
      .toByteArray
  }

  "Schema pruning" should "work with nested struct field access" taggedAs SchemaPruningTest in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data with nested structure
    val message = CompleteMessage.newBuilder()
      .setPrimitives(AllPrimitiveTypes.newBuilder()
        .setInt32Field(42)
        .setStringField("test")
        .setInt64Field(100L)
        .build())
      .setId("test_id")
      .setVersion(1)
      .build()

    val binary = message.toByteArray
    val df = Seq(binary).toDF("data")

    // Only select primitives.int32_field - other fields should be pruned
    val descBytes = createDescriptorBytes("CompleteMessage")
    val result = df
      .select(from_protobuf($"data", "CompleteMessage", descBytes).as("proto"))
      .select($"proto.primitives.int32_field")
      .collect()

    result.length shouldBe 1
    result(0).getInt(0) shouldBe 42
  }

  it should "preserve correctness when pruning is disabled" taggedAs SchemaPruningTest in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Disable pruning
    spark.conf.set("spark.sql.protobuf.nestedSchemaPruning.enabled", "false")

    try {
      val message = CompleteMessage.newBuilder()
        .setPrimitives(AllPrimitiveTypes.newBuilder()
          .setInt32Field(42)
          .setStringField("test")
          .build())
        .setId("test_id")
        .build()

      val binary = message.toByteArray
      val df = Seq(binary).toDF("data")

      val descBytes = createDescriptorBytes("CompleteMessage")
      val result = df
        .select(from_protobuf($"data", "CompleteMessage", descBytes).as("proto"))
        .select($"proto.primitives.int32_field")
        .collect()

      result.length shouldBe 1
      result(0).getInt(0) shouldBe 42
    } finally {
      // Re-enable pruning for other tests
      spark.conf.set("spark.sql.protobuf.nestedSchemaPruning.enabled", "true")
    }
  }

  it should "handle multiple field accesses from same struct" taggedAs SchemaPruningTest in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    val message = CompleteMessage.newBuilder()
      .setPrimitives(AllPrimitiveTypes.newBuilder()
        .setInt32Field(42)
        .setStringField("test")
        .setInt64Field(100L)
        .build())
      .setId("test_id")
      .build()

    val binary = message.toByteArray
    val df = Seq(binary).toDF("data")

    val descBytes = createDescriptorBytes("CompleteMessage")
    val result = df
      .select(from_protobuf($"data", "CompleteMessage", descBytes).as("proto"))
      .select($"proto.primitives.int32_field", $"proto.primitives.string_field")
      .collect()

    result.length shouldBe 1
    result(0).getInt(0) shouldBe 42
    result(0).getString(1) shouldBe "test"
  }

  it should "work with top-level field selection" taggedAs SchemaPruningTest in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    val message = AllPrimitiveTypes.newBuilder()
      .setInt32Field(100)
      .setInt64Field(200L)
      .setStringField("test")
      .build()

    val binary = message.toByteArray
    val df = Seq(binary).toDF("data")

    // Only select int32_field - other fields should be pruned
    val descBytes = createDescriptorBytes("AllPrimitiveTypes")
    val result = df
      .select(from_protobuf($"data", "AllPrimitiveTypes", descBytes).as("proto"))
      .select($"proto.int32_field")
      .collect()

    result.length shouldBe 1
    result(0).getInt(0) shouldBe 100
  }

  it should "handle entire struct selection without pruning" taggedAs SchemaPruningTest in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    val message = AllPrimitiveTypes.newBuilder()
      .setInt32Field(100)
      .setStringField("test")
      .build()

    val binary = message.toByteArray
    val df = Seq(binary).toDF("data")

    // Select entire struct - no pruning should occur
    val descBytes = createDescriptorBytes("AllPrimitiveTypes")
    val result = df
      .select(from_protobuf($"data", "AllPrimitiveTypes", descBytes).as("proto"))
      .select($"proto")
      .collect()

    result.length shouldBe 1
    val row = result(0).getStruct(0)
    row.getInt(row.fieldIndex("int32_field")) shouldBe 100
    row.getString(row.fieldIndex("string_field")) shouldBe "test"
  }

  it should "preserve fields referenced in filter conditions" taggedAs SchemaPruningTest in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data with nested structure
    val message1 = CompleteMessage.newBuilder()
      .setPrimitives(AllPrimitiveTypes.newBuilder()
        .setInt32Field(42)
        .setStringField("test")
        .setInt64Field(100L)
        .build())
      .setId("id1")
      .setVersion(1)
      .build()

    val message2 = CompleteMessage.newBuilder()
      .setPrimitives(AllPrimitiveTypes.newBuilder()
        .setInt32Field(10)
        .setStringField("test2")
        .setInt64Field(200L)
        .build())
      .setId("id2")
      .setVersion(2)
      .build()

    val df = Seq(message1.toByteArray, message2.toByteArray).toDF("data")

    // Filter on primitives.int32_field but only select id
    // Both fields must be preserved in the pruned schema
    val descBytes = createDescriptorBytes("CompleteMessage")
    val result = df
      .select(from_protobuf($"data", "CompleteMessage", descBytes).as("proto"))
      .filter($"proto.primitives.int32_field" > 20)
      .select($"proto.id")
      .collect()

    // Should only return message1 (int32_field = 42 > 20)
    result.length shouldBe 1
    result(0).getString(0) shouldBe "id1"
  }

  it should "preserve field ordinals when selecting non-first fields" taggedAs SchemaPruningTest in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data with string_field at ordinal 13 in the original schema
    val message = AllPrimitiveTypes.newBuilder()
      .setInt32Field(100)
      .setInt64Field(200L)
      .setUint32Field(300)
      .setUint64Field(400L)
      .setSint32Field(500)
      .setSint64Field(600L)
      .setFixed32Field(700)
      .setFixed64Field(800L)
      .setSfixed32Field(900)
      .setSfixed64Field(1000L)
      .setFloatField(1.5f)
      .setDoubleField(2.5)
      .setBoolField(true)
      .setStringField("test_string")  // field 14, ordinal 13
      .build()

    val binary = message.toByteArray

    // Use parallelized data to prevent constant folding optimization
    // This ensures the query actually runs through the full execution path
    val df = spark.sparkContext.parallelize(Seq(binary)).toDF("data")

    // Only select string_field (ordinal 13 in original schema)
    // If ordinals are not preserved, this will fail with IndexOutOfBoundsException
    // because the pruned schema would have string_field at ordinal 0
    val descBytes = createDescriptorBytes("AllPrimitiveTypes")
    val query = df
      .select(from_protobuf($"data", "AllPrimitiveTypes", descBytes).as("proto"))
      .select($"proto.string_field")

    // query.explain(true)

    val result = query
      .collect()

    result.length shouldBe 1
    result(0).getString(0) shouldBe "test_string"
  }

  it should "actually prune schema in optimized plan" taggedAs SchemaPruningTest in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    val message = AllPrimitiveTypes.newBuilder()
      .setInt32Field(100)
      .setInt64Field(200L)
      .setStringField("test")
      .setBoolField(true)
      .setDoubleField(3.14)
      .build()

    val binary = message.toByteArray
    val df = spark.sparkContext.parallelize(Seq(binary)).toDF("data")

    val descBytes = createDescriptorBytes("AllPrimitiveTypes")
    val query = df
      .select(from_protobuf($"data", "AllPrimitiveTypes", descBytes).as("proto"))
      .select($"proto.string_field")

    // Inspect optimized plan to verify schema pruning occurred
    val optimizedPlan = query.queryExecution.optimizedPlan
    val protobufExprs = ProtobufTestUtils.collectProtobufExpressions(optimizedPlan)

    // Should find at least one ProtobufDataToCatalyst expression
    protobufExprs should not be empty
    val exprInfo = protobufExprs.head

    // Verify schema was pruned (requiredSchema should be Some(...))
    exprInfo.prunedSchema shouldBe defined
    val prunedSchema = exprInfo.prunedSchema.get

    // Should have only 1 field (string_field) instead of all 14+ fields
    prunedSchema.fields.length shouldBe 1
    prunedSchema.fields(0).name shouldBe "string_field"

    // The pruned schema should be much smaller than the full AllPrimitiveTypes schema
    // AllPrimitiveTypes has 14+ primitive fields in the proto definition
    prunedSchema.fields.length should be < 5  // Significantly pruned

    // Verify correctness still works
    val result = query.collect()
    result.length shouldBe 1
    result(0).getString(0) shouldBe "test"
  }
}
