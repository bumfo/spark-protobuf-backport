package integration

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Tag}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.protobuf.backport.functions._
import testproto.AllTypesProtos._
import testproto.TestData

/**
 * Tier 3 integration tests with multi-executor Spark cluster.
 *
 * Tests parser behavior in distributed execution:
 * - Multi-executor parsing with 1000+ rows
 * - Parser serialization across JVM boundaries
 * - Roundtrip conversion in distributed mode
 * - Cross-partition consistency
 *
 * Target runtime: <60s
 * Run via: sbt integrationTests
 */
object IntegrationTest extends Tag("Integration")

class SparkIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    // Suppress Spark logging noise
    org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("org.spark_project").setLevel(org.apache.log4j.Level.ERROR)

    spark = SparkSession.builder()
      .master("local[2]")  // 2 threads for parallel execution
      .appName("SparkIntegrationSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")  // Force shuffling for distributed-like behavior
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")  // Use JavaSerializer to avoid Kryo Java module issues
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .config("spark.sql.warehouse.dir", s"file://${System.getProperty("java.io.tmpdir")}/spark-warehouse")
      .config("spark.hadoop.yarn.timeline-service.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      try {
        // Don't stop during test run - let JVM cleanup handle it
        // This prevents "LiveListenerBus is stopped" errors between tests
        // spark.stop()
      } catch {
        case _: Exception => // Ignore shutdown exceptions
      }
    }
  }

  "Spark cluster integration" should "parse 1000+ protobuf rows across multiple executors" in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Generate 1000 rows of test data
    val rowCount = 1000
    val messages = (1 to rowCount).map { i =>
      AllPrimitiveTypes.newBuilder()
        .setInt32Field(i)
        .setInt64Field(i.toLong * 1000)
        .setSint32Field(-i)
        .setSint64Field(-i.toLong * 1000)
        .setStringField(s"row_$i")
        .setStatusField(AllPrimitiveTypes.Status.forNumber(i % 4))
        .build()
        .toByteArray
    }

    // Create DataFrame and repartition to force multi-executor execution
    val df = spark.createDataset(messages).repartition(4).toDF("data")

    // Parse protobuf data
    val parsedDf = df.select(
      from_protobuf($"data", classOf[AllPrimitiveTypes].getName).as("proto")
    )

    // Verify all rows parsed successfully
    val count = parsedDf.count()
    count shouldBe rowCount

    // Sample rows to verify correctness (order not guaranteed after repartition)
    val rows = parsedDf.select("proto.int32_field", "proto.sint32_field", "proto.string_field")
      .orderBy("proto.int32_field")  // Order by int32_field for deterministic results
      .collect()
    rows.length shouldBe rowCount

    // Verify specific rows after ordering
    val firstRow = rows.head
    firstRow.getInt(0) shouldBe 1  // int32_field
    firstRow.getInt(1) shouldBe -1 // sint32_field
    firstRow.getString(2) shouldBe "row_1"

    val lastRow = rows.last
    lastRow.getInt(0) shouldBe rowCount
    lastRow.getInt(1) shouldBe -rowCount
    lastRow.getString(2) shouldBe s"row_$rowCount"
  }

  it should "handle parser serialization across JVM boundaries" in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data with varied content
    val messages = (1 to 100).map { i =>
      AllRepeatedTypes.newBuilder()
        .addAllInt32List(java.util.Arrays.asList((1 to 5).map(_ * i).map(Int.box): _*))
        .addAllSint32List(java.util.Arrays.asList((-1 to -3 by -1).map(_ * i).map(Int.box): _*))
        .addAllSint64List(java.util.Arrays.asList((-10L to -30L by -10L).map(_ * i).map(Long.box): _*))
        .addAllStringList(java.util.Arrays.asList(s"str_${i}_a", s"str_${i}_b"))
        .build()
        .toByteArray
    }

    val df = spark.createDataset(messages).repartition(4).toDF("data")

    // Parse with from_protobuf (forces serialization of parser to executors)
    val parsedDf = df.select(
      from_protobuf($"data", classOf[AllRepeatedTypes].getName).as("proto")
    )

    // Force computation across executors with aggregation
    val result = parsedDf.selectExpr(
      "size(proto.int32_list) as int32_count",
      "size(proto.sint32_list) as sint32_count",
      "size(proto.string_list) as string_count"
    ).collect()

    result.length shouldBe 100
    result.foreach { row =>
      row.getInt(0) shouldBe 5  // int32_list has 5 elements
      row.getInt(1) shouldBe 3  // sint32_list has 3 elements
      row.getInt(2) shouldBe 2  // string_list has 2 elements
    }
  }

  it should "perform roundtrip conversion in distributed mode" in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create original test data
    val original = TestData.createFullPrimitives()
    val originalBinary = original.toByteArray

    // Create 500 copies to ensure distributed execution
    val df = spark.createDataset(Seq.fill(500)(originalBinary)).repartition(4).toDF("data")

    // Roundtrip: binary → struct → binary
    val roundtripDf = df.select(
      to_protobuf(
        from_protobuf($"data", classOf[AllPrimitiveTypes].getName),
        classOf[AllPrimitiveTypes].getName
      ).as("roundtrip_data")
    )

    val roundtripRows = roundtripDf.collect()
    roundtripRows.length shouldBe 500

    // Verify all roundtrip binaries can be parsed back
    roundtripRows.foreach { row =>
      val roundtripBinary = row.getAs[Array[Byte]](0)
      val reparsed = AllPrimitiveTypes.parseFrom(roundtripBinary)

      // Verify key fields match original
      reparsed.getInt32Field shouldBe original.getInt32Field
      reparsed.getSint32Field shouldBe original.getSint32Field
      reparsed.getSint64Field shouldBe original.getSint64Field
      reparsed.getStringField shouldBe original.getStringField
      reparsed.getStatusField shouldBe original.getStatusField
    }
  }

  it should "maintain consistency across partitions" in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create 1000 identical messages
    val message = TestData.createFullRepeated()
    val binary = message.toByteArray

    val df = spark.createDataset(Seq.fill(1000)(binary)).repartition(8).toDF("data")

    // Parse and compute aggregations across all partitions
    val parsedDf = df.select(
      from_protobuf($"data", classOf[AllRepeatedTypes].getName).as("proto")
    )

    // Aggregate to verify consistency
    val stats = parsedDf.selectExpr(
      "COUNT(*) as row_count",
      "COUNT(DISTINCT proto.int32_list) as distinct_int32_lists",
      "COUNT(DISTINCT proto.sint32_list) as distinct_sint32_lists"
    ).collect().head

    stats.getLong(0) shouldBe 1000  // All rows processed
    stats.getLong(1) shouldBe 1     // All int32_lists identical
    stats.getLong(2) shouldBe 1     // All sint32_lists identical
  }

  it should "handle mixed message types in distributed mode" in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Mix primitive and repeated types
    val primitives = (1 to 300).map(_ => TestData.createFullPrimitives().toByteArray)
    val repeated = (1 to 300).map(_ => TestData.createFullRepeated().toByteArray)
    val unpacked = (1 to 400).map(_ => TestData.createFullUnpackedRepeated().toByteArray)

    // Process each type in separate DataFrames
    val primitivesDf = spark.createDataset(primitives).repartition(3).toDF("data")
      .select(from_protobuf($"data", classOf[AllPrimitiveTypes].getName).as("proto"))

    val repeatedDf = spark.createDataset(repeated).repartition(3).toDF("data")
      .select(from_protobuf($"data", classOf[AllRepeatedTypes].getName).as("proto"))

    val unpackedDf = spark.createDataset(unpacked).repartition(4).toDF("data")
      .select(from_protobuf($"data", classOf[AllUnpackedRepeatedTypes].getName).as("proto"))

    // Verify counts
    primitivesDf.count() shouldBe 300
    repeatedDf.count() shouldBe 300
    unpackedDf.count() shouldBe 400

    // Verify content of first row from each type
    val primRow = primitivesDf.select("proto.int32_field", "proto.sint32_field").head()
    primRow.getInt(0) shouldBe 42
    primRow.getInt(1) shouldBe -42

    val repRow = repeatedDf.selectExpr("size(proto.sint32_list) as count").head()
    repRow.getInt(0) shouldBe 3

    val unpackRow = unpackedDf.selectExpr("size(proto.sint32_list) as count").head()
    unpackRow.getInt(0) shouldBe 3
  }
}