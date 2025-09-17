package benchmark

import com.google.protobuf.ByteString

/**
 * Deterministic test data generator for benchmark protobuf messages.
 *
 * Provides consistent, reproducible test data for both simple and complex
 * message schemas without any randomness.
 */
object TestDataGenerator {

  /**
   * Create a deterministic SimpleMessage with all 120 fields populated.
   * Uses fixed, predictable values for consistent benchmarking.
   */
  def createSimpleMessage(): SimpleBenchmarkProtos.SimpleMessage = {
    val builder = SimpleBenchmarkProtos.SimpleMessage.newBuilder()

    // Set scalar int32 fields with deterministic values
    builder.setFieldInt32001(100)
    builder.setFieldInt32002(200)
    builder.setFieldInt32003(300)
    builder.setFieldInt32004(400)
    builder.setFieldInt32005(500)
    builder.setFieldInt32006(600)
    builder.setFieldInt32007(700)
    builder.setFieldInt32008(800)
    builder.setFieldInt32009(900)
    builder.setFieldInt32010(1000)
    builder.setFieldInt32011(1100)
    builder.setFieldInt32012(1200)
    builder.setFieldInt32013(1300)
    builder.setFieldInt32014(1400)
    builder.setFieldInt32015(1500)
    builder.setFieldInt32016(1600)
    builder.setFieldInt32017(1700)
    builder.setFieldInt32018(1800)
    builder.setFieldInt32019(1900)
    builder.setFieldInt32020(2000)

    // Set int64 fields with deterministic values
    (21 to 40).foreach { i =>
       val method = builder.getClass.getMethod(s"setFieldInt64${"%03d".format(i)}", classOf[Long])
      method.invoke(builder, Long.box(i.toLong * 1000000L))
    }

    // Set float fields with deterministic values
    (41 to 50).foreach { i =>
      val method = builder.getClass.getMethod(s"setFieldFloat${"%03d".format(i)}", classOf[Float])
      method.invoke(builder, Float.box(i.toFloat + 0.5f))
    }

    // Set double fields with deterministic values
    (51 to 60).foreach { i =>
      val method = builder.getClass.getMethod(s"setFieldDouble${"%03d".format(i)}", classOf[Double])
      method.invoke(builder, Double.box(i.toDouble + 0.25))
    }

    // Set boolean fields with deterministic pattern
    (61 to 70).foreach { i =>
      val method = builder.getClass.getMethod(s"setFieldBool${"%03d".format(i)}", classOf[Boolean])
      method.invoke(builder, Boolean.box(i % 2 == 0))
    }

    // Set string fields with deterministic values
    (71 to 80).foreach { i =>
      val method = builder.getClass.getMethod(s"setFieldString${"%03d".format(i)}", classOf[String])
      method.invoke(builder, s"test_string_field_$i")
    }

    // Set bytes fields with deterministic values
    (81 to 85).foreach { i =>
      val method = builder.getClass.getMethod(s"setFieldBytes${"%03d".format(i)}", classOf[ByteString])
      val bytes = s"test_bytes_$i".getBytes("UTF-8")
      method.invoke(builder, ByteString.copyFrom(bytes))
    }

    // Add repeated fields with deterministic values
    // Large repeated int32 field to test packed field parsing with array resizing (100+ elements)
    (1 to 120).foreach(i => builder.addRepeatedInt32086(i * 10))
    (91 to 100).foreach(i => builder.addRepeatedInt64091(i.toLong * 100000L))
    (1 to 15).foreach(i => builder.addRepeatedFloat096(i.toFloat + 0.1f))
    (101 to 110).foreach(i => builder.addRepeatedDouble101(i.toDouble + 0.01))
    (106 to 125).foreach(i => builder.addRepeatedBool106(i % 2 == 1))
    (1 to 12).foreach(i => builder.addRepeatedString111(s"repeated_string_$i"))
    (1 to 8).foreach(i => builder.addRepeatedBytes116(ByteString.copyFromUtf8(s"repeated_bytes_$i")))

    builder.build()
  }

  /**
   * Create a deterministic ComplexMessageA with nested structures.
   * Uses fixed values to ensure reproducible test results.
   */
  def createComplexMessage(): ComplexBenchmarkProtos.ComplexMessageA = {
    val builderA = ComplexBenchmarkProtos.ComplexMessageA.newBuilder()
      .setId(42)
      .setName("test_message_a")
      .setValue(123.456)
      .setActive(true)
      .setData(ByteString.copyFromUtf8("deterministic_data_a"))
      .setTimestamp(1640995200000L) // Fixed timestamp: 2022-01-01 00:00:00 UTC
      .setRatio(3.14159f)
      .setDescription("Deterministic complex message A for testing")
      .setMetadata(ByteString.copyFromUtf8("metadata_a"))

    // Add deterministic repeated fields to A
    (1 to 3).foreach(i => builderA.addNumbers(i * 100))
    builderA.addTags("performance")
    builderA.addTags("test")
    builderA.addTags("protobuf")
    (1 to 3).foreach(i => builderA.addMeasurements(i.toDouble * 2.5))
    builderA.addFlags(true)
    builderA.addFlags(false)
    builderA.addFlags(true)
    (15 to 17).foreach(i => builderA.addTimestamps(1640995200000L + i * 3600000L))
    (1 to 3).foreach(i => builderA.addRatios(i.toFloat * 0.33f))
    (1 to 2).foreach(i => builderA.addChunks(ByteString.copyFromUtf8(s"chunk_$i")))

    // Create nested message B with deterministic values
    val builderB = ComplexBenchmarkProtos.ComplexMessageB.newBuilder()
      .setIdentifier(9876543210L)
      .setLabel("test_message_b")
      .setScore(87.65f)
      .setEnabled(true)
      .setPayload(ByteString.copyFromUtf8("deterministic_payload_b"))
      .setPriority(5)
      .setWeight(250.75)
      .setComment("Nested message B with deterministic data")
      .setBinaryData(ByteString.copyFromUtf8("binary_data_b"))

    // Add deterministic repeated fields to B
    (21 to 23).foreach(i => builderB.addCodes(i.toLong * 1000L))
    builderB.addCategories("category_alpha")
    builderB.addCategories("category_beta")
    (1 to 3).foreach(i => builderB.addScores(i.toFloat * 12.34f))
    builderB.addStatuses(true)
    builderB.addStatuses(false)
    (31 to 33).foreach(i => builderB.addPriorities(i))
    (1 to 3).foreach(i => builderB.addWeights(i.toDouble * 50.0))
    builderB.addComments("comment_one")
    builderB.addComments("comment_two")

    // Add nested data with deterministic values
    val nestedData = ComplexBenchmarkProtos.NestedData.newBuilder()
      .setKey("deterministic_key")
      .setValue("deterministic_value")
      .setCount(777)
      .setAverage(555.333)
      .setValid(true)
      .addKeys("key1")
      .addKeys("key2")
      .addValues("value1")
      .addValues("value2")
      .addCounts(10)
      .addCounts(20)
      .addAverages(11.11)
      .addAverages(22.22)
      .addValidations(true)
      .addValidations(false)
      .build()

    builderB.setNestedData(nestedData)
    builderB.addNestedDataList(nestedData) // Reuse the same nested data

    builderA.setMessageB(builderB.build())
    builderA.addNestedMessages(builderB.build()) // Add to repeated field as well

    builderA.build()
  }

  /**
   * Get deterministic binary data for SimpleMessage.
   */
  def getSimpleBinary: Array[Byte] = createSimpleMessage().toByteArray

  /**
   * Get deterministic binary data for ComplexMessage.
   */
  def getComplexBinary: Array[Byte] = createComplexMessage().toByteArray
}