package testproto

import com.google.protobuf.ByteString
import org.scalacheck.Gen
import testproto.AllTypesProtos._

import scala.collection.JavaConverters._

/**
 * ScalaCheck generators for property-based testing.
 * Generates random valid protobuf messages for testing parser invariants.
 */
object Generators {

  // ===== Primitive Generators =====

  def genInt32: Gen[Int] = Gen.chooseNum(Int.MinValue, Int.MaxValue)
  def genInt64: Gen[Long] = Gen.chooseNum(Long.MinValue, Long.MaxValue)
  def genUint32: Gen[Int] = genInt32  // Represented as Int in protobuf Java API
  def genUint64: Gen[Long] = genInt64 // Represented as Long in protobuf Java API
  def genSint32: Gen[Int] = genInt32  // Full range including negative
  def genSint64: Gen[Long] = genInt64 // Full range including negative
  def genFixed32: Gen[Int] = genInt32
  def genFixed64: Gen[Long] = genInt64
  def genSfixed32: Gen[Int] = genInt32
  def genSfixed64: Gen[Long] = genInt64
  def genFloat: Gen[Float] = Gen.chooseNum(-1000f, 1000f)
  def genDouble: Gen[Double] = Gen.chooseNum(-1000.0, 1000.0)
  def genBool: Gen[Boolean] = Gen.oneOf(true, false)
  def genString: Gen[String] = Gen.alphaNumStr.map(s => if (s.isEmpty) "a" else s.take(50))
  def genBytes: Gen[ByteString] = Gen.listOfN(10, Gen.chooseNum(0, 255).map(_.toByte))
    .map(bytes => ByteString.copyFrom(bytes.toArray))
  def genStatus: Gen[AllPrimitiveTypes.Status] = Gen.oneOf(
    AllPrimitiveTypes.Status.UNKNOWN,
    AllPrimitiveTypes.Status.ACTIVE,
    AllPrimitiveTypes.Status.INACTIVE,
    AllPrimitiveTypes.Status.PENDING
  )

  // ===== Message Generators =====

  /**
   * Generate AllPrimitiveTypes with all fields populated.
   */
  def genFullPrimitives: Gen[AllPrimitiveTypes] = for {
    int32 <- genInt32
    int64 <- genInt64
    uint32 <- genUint32
    uint64 <- genUint64
    sint32 <- genSint32
    sint64 <- genSint64
    fixed32 <- genFixed32
    fixed64 <- genFixed64
    sfixed32 <- genSfixed32
    sfixed64 <- genSfixed64
    float <- genFloat
    double <- genDouble
    bool <- genBool
    string <- genString
    bytes <- genBytes
    status <- genStatus
  } yield AllPrimitiveTypes.newBuilder()
    .setInt32Field(int32)
    .setInt64Field(int64)
    .setUint32Field(uint32)
    .setUint64Field(uint64)
    .setSint32Field(sint32)
    .setSint64Field(sint64)
    .setFixed32Field(fixed32)
    .setFixed64Field(fixed64)
    .setSfixed32Field(sfixed32)
    .setSfixed64Field(sfixed64)
    .setFloatField(float)
    .setDoubleField(double)
    .setBoolField(bool)
    .setStringField(string)
    .setBytesField(bytes)
    .setStatusField(status)
    .build()

  /**
   * Generate AllPrimitiveTypes with randomly omitted fields (sparse).
   */
  def genSparsePrimitives: Gen[AllPrimitiveTypes] = for {
    int32Opt <- Gen.option(genInt32)
    int64Opt <- Gen.option(genInt64)
    uint32Opt <- Gen.option(genUint32)
    uint64Opt <- Gen.option(genUint64)
    sint32Opt <- Gen.option(genSint32)
    sint64Opt <- Gen.option(genSint64)
    stringOpt <- Gen.option(genString)
    boolOpt <- Gen.option(genBool)
  } yield {
    val builder = AllPrimitiveTypes.newBuilder()
    int32Opt.foreach(builder.setInt32Field)
    int64Opt.foreach(builder.setInt64Field)
    uint32Opt.foreach(builder.setUint32Field)
    uint64Opt.foreach(builder.setUint64Field)
    sint32Opt.foreach(builder.setSint32Field)
    sint64Opt.foreach(builder.setSint64Field)
    stringOpt.foreach(builder.setStringField)
    boolOpt.foreach(builder.setBoolField)
    builder.build()
  }

  /**
   * Generate AllRepeatedTypes with populated lists (packed encoding).
   */
  def genFullRepeated: Gen[AllRepeatedTypes] = for {
    int32List <- Gen.listOfN(5, genInt32)
    sint32List <- Gen.listOfN(3, genSint32)
    sint64List <- Gen.listOfN(3, genSint64)
    floatList <- Gen.listOfN(4, genFloat)
    stringList <- Gen.listOfN(3, genString)
  } yield AllRepeatedTypes.newBuilder()
    .addAllInt32List(int32List.map(Int.box).asJava)
    .addAllSint32List(sint32List.map(Int.box).asJava)
    .addAllSint64List(sint64List.map(Long.box).asJava)
    .addAllFloatList(floatList.map(Float.box).asJava)
    .addAllStringList(stringList.asJava)
    .build()

  /**
   * Generate AllUnpackedRepeatedTypes with populated lists (unpacked encoding).
   */
  def genFullUnpackedRepeated: Gen[AllUnpackedRepeatedTypes] = for {
    int32List <- Gen.listOfN(5, genInt32)
    sint32List <- Gen.listOfN(3, genSint32)
    sint64List <- Gen.listOfN(3, genSint64)
    floatList <- Gen.listOfN(4, genFloat)
    stringList <- Gen.listOfN(3, genString)
  } yield AllUnpackedRepeatedTypes.newBuilder()
    .addAllInt32List(int32List.map(Int.box).asJava)
    .addAllSint32List(sint32List.map(Int.box).asJava)
    .addAllSint64List(sint64List.map(Long.box).asJava)
    .addAllFloatList(floatList.map(Float.box).asJava)
    .addAllStringList(stringList.asJava)
    .build()

  /**
   * Generate any AllPrimitiveTypes message (full or sparse).
   */
  def genAnyPrimitives: Gen[AllPrimitiveTypes] = Gen.oneOf(genFullPrimitives, genSparsePrimitives)

  /**
   * Generate CompleteMessage.
   */
  def genCompleteMessage: Gen[CompleteMessage] = for {
    primitives <- genFullPrimitives
    repeated <- genFullRepeated
    id <- genString
    version <- Gen.chooseNum(1, 100)
  } yield CompleteMessage.newBuilder()
    .setPrimitives(primitives)
    .setRepeated(repeated)
    .setId(id)
    .setVersion(version)
    .build()
}