import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Tag}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.protobuf.backport.functions._

/**
 * Integration tests for shaded protobuf environment.
 *
 * This test suite runs in a fully shaded environment where ALL protobuf classes
 * are under org.sparkproject.spark_protobuf.protobuf.* namespace.
 *
 * NOTE: This test class does NOT import any protobuf classes directly.
 * All protobuf-dependent code is in ShadedTestHelper (src/main/scala) which
 * gets compiled and shaded into the assembly JAR.
 *
 * Tests verify:
 * - Shaded protobuf runtime works correctly
 * - from_protobuf works with shaded messages
 * - Serialization across executors with shaded classes
 * - Binary descriptor sets work in shaded environment
 */
object ShadedIntegrationTest extends Tag("Integration")

class ShadedIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    // Suppress Spark logging noise
    org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.ERROR)

    spark = SparkSession.builder()
      .appName("ShadedProtobufTest")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  behavior of "Shaded protobuf integration"

  it should "verify shaded protobuf environment without unshaded classes" taggedAs ShadedIntegrationTest in {
    // Check if shaded protobuf is available
    val shadedName = "org.sparkproject.spark_protobuf.protobuf." + "Message"
    val shadedClass = Class.forName(shadedName)
    val packageName = shadedClass.getPackage.getName

    info(s"Found shaded protobuf package: $packageName")
    packageName should be("org.sparkproject.spark_protobuf.protobuf")

    // Verify unshaded protobuf is NOT available
    // Construct name dynamically to avoid shading rewriting the literal
    val unshadedName = Seq("com", "google", "protobuf", "Message").mkString(".")
    val unshadedException = intercept[ClassNotFoundException] {
      Class.forName(unshadedName)
    }

    info(s"✓ Unshaded protobuf correctly excluded from classpath: ${unshadedException.getMessage}")
  }

  it should "parse shaded protobuf messages with from_protobuf" taggedAs ShadedIntegrationTest in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test message using helper (no protobuf imports in test code)
    val messageBytes = ShadedTestHelper.createTestMessage(
      id = 42,
      name = "shaded-test",
      values = Seq(1, 2, 3),
      nestedField = Some("nested-value")
    )

    val descriptorBytes = ShadedTestHelper.createDescriptorBytes()
    val messageName = ShadedTestHelper.getMessageName()

    // Create DataFrame with binary data
    val data = (1 to 100).map(_ => messageBytes)
    val df = spark.createDataset(data).repartition(4).toDF("data")

    // Parse using from_protobuf with binary descriptor
    val parsedDf = df.select(
      from_protobuf($"data", messageName, descriptorBytes).as("proto")
    )

    val results = parsedDf.collect()
    results.length should be(100)
    results.foreach { row =>
      val proto = row.getAs[Row]("proto")
      proto.getAs[Int]("id") should be(42)
      proto.getAs[String]("name") should be("shaded-test")
      proto.getAs[Seq[Int]]("values") should be(Seq(1, 2, 3))

      val nested = proto.getAs[Row]("nested")
      nested.getAs[String]("field") should be("nested-value")
    }
  }

  it should "serialize shaded classes across executor boundaries" taggedAs ShadedIntegrationTest in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create message using helper
    val messageBytes = ShadedTestHelper.createTestMessage(
      id = 99,
      name = "distributed",
      values = Seq.empty,
      nestedField = None
    )

    val descriptorBytes = ShadedTestHelper.createDescriptorBytes()
    val messageName = ShadedTestHelper.getMessageName()

    // Create large dataset across multiple partitions
    val data = (1 to 10000).map(_ => messageBytes)
    val df = spark.createDataset(data).repartition(8).toDF("data")

    // Parse across all executors
    val parsedDf = df.select(
      from_protobuf($"data", messageName, descriptorBytes).as("proto")
    )

    // Verify all rows parsed correctly
    val count = parsedDf.select($"proto.id").distinct().count()
    count should be(1) // All should be 99

    parsedDf.count() should be(10000)

    // Verify specific values
    val sample = parsedDf.select($"proto.id", $"proto.name").first()
    sample.getAs[Int](0) should be(99)
    sample.getAs[String](1) should be("distributed")
  }

  it should "work with binary descriptor sets in shaded environment" taggedAs ShadedIntegrationTest in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Test that binary descriptors work correctly with shaded runtime
    val messageBytes = ShadedTestHelper.createTestMessage(
      id = 123,
      name = "",
      values = Seq(10, 20, 30),
      nestedField = None
    )

    val descriptorBytes = ShadedTestHelper.createDescriptorBytes()
    val messageName = ShadedTestHelper.getMessageName()

    val data = (1 to 1000).map(_ => messageBytes)
    val df = spark.createDataset(data).repartition(4).toDF("data")

    // Use message name only (forces descriptor-based parsing)
    val parsedDf = df.select(
      from_protobuf($"data", messageName, descriptorBytes).as("proto")
    )

    val results = parsedDf.collect()
    results.length should be(1000)

    results.foreach { row =>
      val proto = row.getAs[Row]("proto")
      proto.getAs[Int]("id") should be(123)
      proto.getAs[Seq[Int]]("values") should be(Seq(10, 20, 30))
    }
  }

  it should "parse with InlineParser in shaded environment" taggedAs ShadedIntegrationTest in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test message using helper
    val messageBytes = ShadedTestHelper.createTestMessage(
      id = 42,
      name = "inline-parser-test",
      values = Seq(1, 2, 3),
      nestedField = Some("nested-value")
    )

    // Create large dataset across multiple partitions to test serialization
    val rowCount = 1000
    val data = (1 to rowCount).map(_ => messageBytes)
    val df = spark.createDataset(data).repartition(4).toDF("data")

    // Parse using InlineParser (via helper that creates InlineParserExpression)
    val parsedDf = df.select(
      ShadedTestHelper.createInlineParserColumn($"data").as("proto")
    )

    // Verify all rows parsed successfully
    val count = parsedDf.count()
    count should be(rowCount)

    val results = parsedDf.collect()
    results.length should be(rowCount)

    // Verify correctness
    results.foreach { row =>
      val proto = row.getAs[Row]("proto")
      proto.getAs[Int]("id") should be(42)
      proto.getAs[String]("name") should be("inline-parser-test")
      proto.getAs[Seq[Int]]("values") should be(Seq(1, 2, 3))

      val nested = proto.getAs[Row]("nested")
      nested.getAs[String]("field") should be("nested-value")
    }
  }

  it should "serialize InlineParser across executor boundaries in shaded environment" taggedAs ShadedIntegrationTest in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test messages with varied content
    val messages = (1 to 100).map { i =>
      ShadedTestHelper.createTestMessage(
        id = i,
        name = s"msg_$i",
        values = Seq(i, i * 2, i * 3),
        nestedField = if (i % 2 == 0) Some(s"nested_$i") else None
      )
    }

    val df = spark.createDataset(messages).repartition(8).toDF("data")

    // Parse using InlineParser across all executors
    val parsedDf = df.select(
      ShadedTestHelper.createInlineParserColumn($"data").as("proto")
    )

    // Verify all rows parsed correctly
    val count = parsedDf.select($"proto.id").distinct().count()
    count should be(100) // All IDs should be distinct

    parsedDf.count() should be(100)

    // Verify specific values across partitions
    val orderedResults = parsedDf.select($"proto.id", $"proto.name", $"proto.values")
      .orderBy($"proto.id")
      .collect()

    orderedResults.zipWithIndex.foreach { case (row, idx) =>
      val expectedId = idx + 1
      row.getAs[Int]("id") should be(expectedId)
      row.getAs[String]("name") should be(s"msg_$expectedId")
      row.getAs[Seq[Int]]("values") should be(Seq(expectedId, expectedId * 2, expectedId * 3))
    }
  }
}
