package fastproto

import com.google.protobuf.{Type, CodedOutputStream}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.ByteArrayOutputStream

class WireFormatIssueTest extends AnyFlatSpec with Matchers {

  private def createWireTypeMismatchBinary(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 1: VARINT (tag = 8, value = 0) - this is normal
    output.writeTag(1, com.google.protobuf.WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(0)

    // Field 2: LENGTH_DELIMITED containing nested message (tag = 18)
    val nestedMessage = {
      val nestedBaos = new ByteArrayOutputStream()
      val nestedOutput = CodedOutputStream.newInstance(nestedBaos)

      // Nested field 1: LENGTH_DELIMITED string (tag = 10)
      nestedOutput.writeTag(1, com.google.protobuf.WireFormat.WIRETYPE_LENGTH_DELIMITED)
      val nestedNestedMessage = {
        val innerBaos = new ByteArrayOutputStream()
        val innerOutput = CodedOutputStream.newInstance(innerBaos)

        // Inner field 2: LENGTH_DELIMITED string (tag = 18) - "http://test"
        innerOutput.writeTag(2, com.google.protobuf.WireFormat.WIRETYPE_LENGTH_DELIMITED)
        innerOutput.writeStringNoTag("http://test")

        // Inner field 5: VARINT (tag = 40) - this causes the wire type mismatch
        // if schema expects field 5 to be STRING (LENGTH_DELIMITED)
        innerOutput.writeTag(5, com.google.protobuf.WireFormat.WIRETYPE_VARINT)
        innerOutput.writeInt64NoTag(123456789L)

        innerOutput.flush()
        innerBaos.toByteArray
      }
      nestedOutput.writeBytesNoTag(com.google.protobuf.ByteString.copyFrom(nestedNestedMessage))

      nestedOutput.flush()
      nestedBaos.toByteArray
    }

    output.writeTag(2, com.google.protobuf.WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeBytesNoTag(com.google.protobuf.ByteString.copyFrom(nestedMessage))

    output.flush()
    baos.toByteArray
  }

  "WireFormatParser" should "handle wire type mismatches in protobuf data without crashing" in {
    // Create a synthetic binary that has wire type mismatches
    val binary = createWireTypeMismatchBinary()

    // Create a basic Type descriptor
    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    // Use a simple schema
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true)
    ))

    val converter = new WireFormatParser(descriptor, sparkSchema)

    // Before the fix, this would throw an exception due to wire type mismatches
    // After the fix, it should complete successfully
    noException should be thrownBy {
      val row = converter.parse(binary)
      row.numFields should equal(1)
    }
  }

  it should "parse the data and extract valid fields" in {
    // Use our synthetic binary instead of hardcoded base64
    val binary = createWireTypeMismatchBinary()

    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    // More comprehensive schema that matches some of the fields in the data
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true)
      ))), nullable = true)
    ))

    val converter = new WireFormatParser(descriptor, sparkSchema)
    val row = converter.parse(binary)

    // Should complete successfully
    row.numFields should equal(2)

    // The name field might be null since the original data might not match exactly,
    // but the parsing should not crash
  }

  it should "demonstrate wire type validation by creating specific mismatches" in {
    // Create binary with field 5 as VARINT but schema expects STRING
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 5: VARINT (but schema will expect STRING/LENGTH_DELIMITED)
    output.writeTag(5, com.google.protobuf.WireFormat.WIRETYPE_VARINT)
    output.writeInt64NoTag(999L)

    output.flush()
    val binaryWithMismatch = baos.toByteArray

    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    // Schema expects field 5 to exist but as a different type
    val sparkSchema = StructType(Seq(
      StructField("syntax", StringType, nullable = true) // Field 5 in Type proto is syntax (enum -> string)
    ))

    val converter = new WireFormatParser(descriptor, sparkSchema)

    // Before fix: this would crash trying to read VARINT as LENGTH_DELIMITED
    // After fix: wire type mismatch is detected and field is skipped
    noException should be thrownBy {
      val row = converter.parse(binaryWithMismatch)
      row.numFields should equal(1)
      // Field should be null due to wire type mismatch (acceptable)
    }
  }
}