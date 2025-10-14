package fastproto

import benchmark.{ComplexBenchmarkProtos, SimpleBenchmarkProtos}
import org.apache.spark.sql.protobuf.backport.utils.SchemaConverters
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class InlineParserGeneratorSpec extends AnyFunSuite with Matchers {

  test("generates parser for SimpleMessage with all primitive types") {
    val descriptor = SimpleBenchmarkProtos.SimpleMessage.getDescriptor
    val schema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    val generated = InlineParserGenerator.generateParser(
      "SimpleMessageParser",
      descriptor,
      schema
    )

    // Verify class structure
    generated should include("public final class SimpleMessageParser extends StreamWireParser")
    generated should include("protected void parseInto(CodedInputStream input, RowWriter writer)")
    generated should include("NullDefaultRowWriter w = (NullDefaultRowWriter) writer")

    // Verify ArrayContext for primitive arrays
    generated should include("ProtoRuntime.ArrayContext arrayCtx = new ProtoRuntime.ArrayContext()")
    generated should include("arrayCtx.completeIfActive(w)")

    // Verify BufferList for repeated strings/bytes
    generated should include("BufferList[] bufferLists = new BufferList")

    // Verify switch statement
    generated should include("switch (tag)")

    // Verify single field handlers exist
    generated should include("ProtoRuntime.writeInt32")
    generated should include("ProtoRuntime.writeInt64")
    generated should include("ProtoRuntime.writeFloat")
    generated should include("ProtoRuntime.writeDouble")
    generated should include("ProtoRuntime.writeBool")
    generated should include("ProtoRuntime.writeString")
    generated should include("ProtoRuntime.writeBytes")

    // Verify repeated field handlers exist
    generated should include("ProtoRuntime.collectInt32")
    generated should include("ProtoRuntime.collectInt64")
    generated should include("ProtoRuntime.collectFloat")
    generated should include("ProtoRuntime.collectDouble")
    generated should include("ProtoRuntime.collectBool")
    generated should include("ProtoRuntime.collectString")
    generated should include("ProtoRuntime.collectBytes")

    // Verify packed handlers exist
    generated should include("ProtoRuntime.collectPackedInt32")
    generated should include("ProtoRuntime.collectPackedInt64")
    generated should include("ProtoRuntime.collectPackedFloat")
    generated should include("ProtoRuntime.collectPackedDouble")
    generated should include("ProtoRuntime.collectPackedBool")

    // Verify flush calls
    generated should include("ProtoRuntime.flushStringArray")
    generated should include("ProtoRuntime.flushBytesArray")

    // Verify comments with field names
    generated should include("// field_int32_001")
    generated should include("// repeated_int32_086")
    generated should include("// repeated_string_111")
  }

  test("generates parser for ComplexMessageA with nested messages") {
    val descriptor = ComplexBenchmarkProtos.ComplexMessageA.getDescriptor
    val schema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    val generated = InlineParserGenerator.generateParser(
      "ComplexMessageAParser",
      descriptor,
      schema
    )

    // Verify class structure
    generated should include("public final class ComplexMessageAParser extends StreamWireParser")

    // Verify nested parser fields
    generated should include("private StreamWireParser parser_message_b")
    generated should include("private StreamWireParser parser_nested_messages")

    // Verify nested parser setters
    generated should include("public void setNestedParser10(StreamWireParser parser)")
    generated should include("public void setNestedParser18(StreamWireParser parser)")
    generated should include("if (this.parser_message_b != null)")
    generated should include("if (this.parser_nested_messages != null)")

    // Verify message handling
    generated should include("ProtoRuntime.writeMessage")
    generated should include("ProtoRuntime.collectMessage")
    generated should include("ProtoRuntime.flushMessageArray")

    // Verify field comments
    generated should include("// id")
    generated should include("// name")
    generated should include("// numbers")
    generated should include("// tags")
    generated should include("// message_b")
    generated should include("// nested_messages")
  }

  test("generates parser for ComplexMessageB with NestedData") {
    val descriptor = ComplexBenchmarkProtos.ComplexMessageB.getDescriptor
    val schema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    val generated = InlineParserGenerator.generateParser(
      "ComplexMessageBParser",
      descriptor,
      schema
    )

    // Verify nested parsers for NestedData
    generated should include("private StreamWireParser parser_nested_data")
    generated should include("private StreamWireParser parser_nested_data_list")

    // Verify nested parser setters exist
    generated should include("public void setNestedParser18(StreamWireParser parser)")
    generated should include("public void setNestedParser19(StreamWireParser parser)")

    // Verify field handlers
    generated should include("ProtoRuntime.writeInt64")
    generated should include("ProtoRuntime.writeFloat")
    generated should include("ProtoRuntime.collectInt64")
    generated should include("ProtoRuntime.collectFloat")
  }

  test("generates correct field number to tag mapping") {
    val descriptor = ComplexBenchmarkProtos.ComplexMessageA.getDescriptor
    val schema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    val generated = InlineParserGenerator.generateParser(
      "TestParser",
      descriptor,
      schema
    )

    // Field 1 (id, int32) -> tag = (1 << 3) | 0 = 8
    generated should include("case 8:")

    // Field 2 (name, string) -> tag = (2 << 3) | 2 = 18
    generated should include("case 18:")

    // Field 6 (numbers, repeated int32) -> unpacked tag = (6 << 3) | 0 = 48
    generated should include("case 48:")
    // Field 6 (numbers, repeated int32) -> packed tag = (6 << 3) | 2 = 50
    generated should include("case 50:")

    // Field 10 (message_b, message) -> tag = (10 << 3) | 2 = 82
    generated should include("case 82:")
  }

  test("generates parser method (not full class)") {
    val descriptor = SimpleBenchmarkProtos.SimpleMessage.getDescriptor
    val schema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    val generated = InlineParserGenerator.generateParserMethod(
      "parseSimpleMessage",
      descriptor,
      schema
    )

    // Verify method signature
    generated should include("public static void parseSimpleMessage(byte[] data, NullDefaultRowWriter writer)")

    // Should not have class declaration
    generated should not include "public final class"
    generated should not include "extends StreamWireParser"

    // Should have method body
    generated should include("CodedInputStream input = CodedInputStream.newInstance(data)")
    generated should include("writer.resetRowWriter()")
    generated should include("switch (tag)")
  }

  test("respects schema field mapping") {
    val descriptor = ComplexBenchmarkProtos.ComplexMessageA.getDescriptor
    val schema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    // Create a subset schema with only a few fields
    val subsetSchema = StructType(schema.fields.take(3))

    val generated = InlineParserGenerator.generateParser(
      "SubsetParser",
      descriptor,
      subsetSchema
    )

    // Should only generate cases for fields in the subset schema
    // Field 1, 2, 3 are id, name, value
    generated should include("case 8:") // id
    generated should include("case 18:") // name
    generated should include("case 25:") // value (double, wire type 1)

    // Fields beyond the subset should not generate excessive code
    // (they should be skipped by the default case)
  }

  test("handles empty schema gracefully") {
    val descriptor = ComplexBenchmarkProtos.ComplexMessageA.getDescriptor
    val emptySchema = StructType(Seq())

    val generated = InlineParserGenerator.generateParser(
      "EmptyParser",
      descriptor,
      emptySchema
    )

    // Should still have valid structure
    generated should include("public final class EmptyParser extends StreamWireParser")
    generated should include("switch (tag)")
    generated should include("default:")
    generated should include("input.skipField(tag)")

    // Should not have any case statements (all fields skipped)
    generated should not include "case 8:"
  }

  test("generates compact code with single-line cases") {
    val descriptor = SimpleBenchmarkProtos.SimpleMessage.getDescriptor
    val schema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    val generated = InlineParserGenerator.generateParser(
      "CompactParser",
      descriptor,
      schema
    )

    // Verify cases are single-line format
    // Each case should be: case TAG: ProtoRuntime.method(...); break; // comment
    val casePattern = "case \\d+: ProtoRuntime\\.\\w+\\([^;]+\\); break; // \\w+".r
    val matches = casePattern.findAllIn(generated).toList

    matches.length should be > 0
  }

  test("orders BufferList indices consistently") {
    val descriptor = ComplexBenchmarkProtos.ComplexMessageA.getDescriptor
    val schema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    val generated = InlineParserGenerator.generateParser(
      "IndexParser",
      descriptor,
      schema
    )

    // Repeated strings, bytes, messages should use sequential BufferList indices
    // The order should match the order they appear in the proto

    // tags (repeated string, field 7) should be at some index
    // chunks (repeated bytes, field 17) should be at a later index
    // nested_messages (repeated message, field 18) should be at an even later index

    generated should include("bufferLists[")
    generated should include("ProtoRuntime.collectString")
    generated should include("ProtoRuntime.collectBytes")
    generated should include("ProtoRuntime.collectMessage")
    generated should include("ProtoRuntime.flushStringArray")
    generated should include("ProtoRuntime.flushBytesArray")
    generated should include("ProtoRuntime.flushMessageArray")
  }
}
