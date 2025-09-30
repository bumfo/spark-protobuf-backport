package testproto

import com.google.protobuf.ByteString
import testproto.AllTypesProtos._
import testproto.NestedProtos._
import testproto.MapsProtos._
import testproto.EdgeCasesProtos._

import scala.util.Random
import scala.collection.JavaConverters._

/**
 * Factory methods for creating test protobuf messages.
 * Provides both deterministic (for unit tests) and random (for property tests) data.
 */
object TestData {

  private val deterministicSeed = 42L

  // ===== AllPrimitiveTypes =====

  def createFullPrimitives(): AllPrimitiveTypes = {
    AllPrimitiveTypes.newBuilder()
      .setInt32Field(42)
      .setInt64Field(12345678901L)
      .setUint32Field(-1)  // 0xFFFFFFFF in unsigned representation
      .setUint64Field(Long.MaxValue)
      .setSint32Field(-42)                    // ZigZag encoded
      .setSint64Field(-12345678901L)          // ZigZag encoded
      .setFixed32Field(100)
      .setFixed64Field(1000L)
      .setSfixed32Field(-100)
      .setSfixed64Field(-1000L)
      .setFloatField(3.14f)
      .setDoubleField(2.71828)
      .setBoolField(true)
      .setStringField("test_string")
      .setBytesField(ByteString.copyFrom(Array[Byte](1, 2, 3, 4, 5)))
      .setStatusField(AllPrimitiveTypes.Status.ACTIVE)
      .build()
  }

  def createSparsePrimitives(): AllPrimitiveTypes = {
    AllPrimitiveTypes.newBuilder()
      .setInt32Field(10)
      .setStringField("sparse")
      // Only 2 fields set, rest are defaults
      .build()
  }

  def createEmptyPrimitives(): AllPrimitiveTypes = {
    AllPrimitiveTypes.newBuilder().build()
  }

  // ===== AllRepeatedTypes =====

  def createFullRepeated(): AllRepeatedTypes = {
    AllRepeatedTypes.newBuilder()
      .addAllInt32List(List(1, 2, 3, 4, 5).map(Int.box).asJava)
      .addAllInt64List(List(10L, 20L, 30L).map(Long.box).asJava)
      .addAllUint32List(List(100, 200, 300).map(Int.box).asJava)
      .addAllUint64List(List(1000L, 2000L, 3000L).map(Long.box).asJava)
      .addAllSint32List(List(-1, -2, -3).map(Int.box).asJava)       // Critical: ZigZag
      .addAllSint64List(List(-10L, -20L, -30L).map(Long.box).asJava) // Critical: ZigZag
      .addAllFixed32List(List(100, 200).map(Int.box).asJava)
      .addAllFixed64List(List(1000L, 2000L).map(Long.box).asJava)
      .addAllSfixed32List(List(-100, -200).map(Int.box).asJava)
      .addAllSfixed64List(List(-1000L, -2000L).map(Long.box).asJava)
      .addAllFloatList(List(1.1f, 2.2f, 3.3f, 4.4f).map(Float.box).asJava)
      .addAllDoubleList(List(1.11, 2.22).map(Double.box).asJava)
      .addAllBoolList(List(true, false, true).map(Boolean.box).asJava)
      .addAllStringList(List("a", "b", "c").asJava)
      .addAllBytesList(List(
        ByteString.copyFrom(Array[Byte](1, 2)),
        ByteString.copyFrom(Array[Byte](3, 4))
      ).asJava)
      .addAllStatusList(List(
        AllPrimitiveTypes.Status.ACTIVE,
        AllPrimitiveTypes.Status.INACTIVE
      ).asJava)
      .build()
  }

  def createEmptyRepeated(): AllRepeatedTypes = {
    AllRepeatedTypes.newBuilder().build()
  }

  // ===== Unpacked Repeated Fields (proto3 [packed = false]) =====

  def createFullUnpackedRepeated(): AllUnpackedRepeatedTypes = {
    AllUnpackedRepeatedTypes.newBuilder()
      .addAllInt32List(List(1, 2, 3, 4, 5).map(Int.box).asJava)
      .addAllInt64List(List(10L, 20L, 30L).map(Long.box).asJava)
      .addAllUint32List(List(-1, -2).map(Int.box).asJava) // Max uint32 values as signed
      .addAllUint64List(List(Long.MaxValue, Long.MaxValue - 1).map(Long.box).asJava)
      .addAllSint32List(List(-1, -2, -3).map(Int.box).asJava) // CRITICAL: ZigZag unpacked
      .addAllSint64List(List(-10L, -20L, -30L).map(Long.box).asJava) // CRITICAL: ZigZag unpacked
      .addAllFixed32List(List(100, 200).map(Int.box).asJava)
      .addAllFixed64List(List(1000L, 2000L).map(Long.box).asJava)
      .addAllSfixed32List(List(-100, -200).map(Int.box).asJava)
      .addAllSfixed64List(List(-1000L, -2000L).map(Long.box).asJava)
      .addAllFloatList(List(1.1f, 2.2f, 3.3f, 4.4f).map(Float.box).asJava)
      .addAllDoubleList(List(1.11, 2.22).map(Double.box).asJava)
      .addAllBoolList(List(true, false, true).map(Boolean.box).asJava)
      .addAllStringList(List("a", "b", "c").asJava)
      .addAllBytesList(List(
        ByteString.copyFrom(Array[Byte](1, 2)),
        ByteString.copyFrom(Array[Byte](3, 4))
      ).asJava)
      .addAllStatusList(List(
        AllPrimitiveTypes.Status.ACTIVE,
        AllPrimitiveTypes.Status.INACTIVE
      ).asJava)
      .build()
  }

  def createEmptyUnpackedRepeated(): AllUnpackedRepeatedTypes = {
    AllUnpackedRepeatedTypes.newBuilder().build()
  }

  // ===== Complete Message =====

  def createCompleteMessage(): CompleteMessage = {
    CompleteMessage.newBuilder()
      .setPrimitives(createFullPrimitives())
      .setRepeated(createFullRepeated())
      .setId("test_id_123")
      .setVersion(1)
      .build()
  }

  // ===== Random Data Generation =====

  def generateRandomPrimitives(seed: Long = deterministicSeed): AllPrimitiveTypes = {
    val rand = new Random(seed)
    AllPrimitiveTypes.newBuilder()
      .setInt32Field(rand.nextInt())
      .setInt64Field(rand.nextLong())
      .setUint32Field(rand.nextInt())
      .setUint64Field(rand.nextLong())
      .setSint32Field(rand.nextInt())
      .setSint64Field(rand.nextLong())
      .setFixed32Field(rand.nextInt())
      .setFixed64Field(rand.nextLong())
      .setSfixed32Field(rand.nextInt())
      .setSfixed64Field(rand.nextLong())
      .setFloatField(rand.nextFloat())
      .setDoubleField(rand.nextDouble())
      .setBoolField(rand.nextBoolean())
      .setStringField(rand.alphanumeric.take(20).mkString)
      .setBytesField(ByteString.copyFrom({
        val bytes = new Array[Byte](10)
        rand.nextBytes(bytes)
        bytes
      }))
      .setStatusField(AllPrimitiveTypes.Status.forNumber(rand.nextInt(4)))
      .build()
  }

  def generateRandomDataset(count: Int): Seq[AllPrimitiveTypes] = {
    (0 until count).map(i => generateRandomPrimitives(deterministicSeed + i))
  }

  // ===== Nested Messages =====

  def createNestedMessage(): Nested = {
    Nested.newBuilder()
      .setName("parent")
      .setInner(Nested.Inner.newBuilder()
        .setValue(42)
        .setDescription("inner")
        .setDeep(Nested.Inner.DeepInner.newBuilder()
          .setFlag(true)
          .setScore(3.14)
          .setData(ByteString.copyFrom(Array[Byte](1, 2, 3)))
          .build())
        .build())
      .addInnerList(Nested.Inner.newBuilder()
        .setValue(100)
        .setDescription("list_item")
        .build())
      .build()
  }

  def createRecursiveMessage(depth: Int): Recursive = {
    def build(d: Int): Recursive.Builder = {
      val builder = Recursive.newBuilder()
        .setId(s"node_$d")
        .setDepth(d)
      if (d > 0) {
        builder.setChild(build(d - 1).build())
      }
      builder
    }
    build(depth).build()
  }

  def createMutualRecursion(): MutualA = {
    MutualA.newBuilder()
      .setName("a1")
      .setValueA(42)
      .setBField(MutualB.newBuilder()
        .setLabel("b1")
        .setValueB(3.14)
        .setAField(MutualA.newBuilder()
          .setName("a2")
          .setValueA(100)
          .build())
        .build())
      .build()
  }

  // ===== Maps =====

  def createMapMessage(): WithMaps = {
    WithMaps.newBuilder()
      .putStringToInt("one", 1)
      .putStringToInt("two", 2)
      .putIntToString(1, "one")
      .putIntToString(2, "two")
      .putStringToDouble("pi", 3.14)
      .putInt64ToBool(100L, true)
      .putIntToBytes(1, ByteString.copyFrom(Array[Byte](1, 2, 3)))
      .putStringToMessage("msg1", WithMaps.Value.newBuilder()
        .setData("data1")
        .setCount(10)
        .addTags("tag1")
        .addTags("tag2")
        .build())
      .putIntToMessage(1, WithMaps.Value.newBuilder()
        .setData("data_int")
        .setCount(20)
        .build())
      .build()
  }

  // ===== Edge Cases =====

  def createEmptyMessage(): EmptyMessage = EmptyMessage.newBuilder().build()

  def createSingleFieldMessage(): SingleField = {
    SingleField.newBuilder().setOnlyField("value").build()
  }

  def createSparseMessage(): Sparse = {
    Sparse.newBuilder()
      .setField1(42)
      .setField2("sparse_value")
      // field3, field4, field5, field6, nested omitted
      .build()
  }

  def createDefaults(): Defaults = {
    Defaults.newBuilder().build() // All fields use defaults
  }

  def createOneof(): WithOneof = {
    WithOneof.newBuilder()
      .setName("oneof_test")
      .setStringChoice("string_value")  // Set string choice
      .build()
  }

  def createOneofWithMessage(): WithOneof = {
    WithOneof.newBuilder()
      .setName("oneof_message")
      .setMessageChoice(WithOneof.SubMessage.newBuilder()
        .setValue("nested_value")
        .build())
      .build()
  }
}