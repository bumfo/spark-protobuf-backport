/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fastproto

import com.google.protobuf.Descriptors.Descriptor
import org.apache.spark.sql.protobuf.backport.utils.SchemaConverters
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import benchmark.ComplexBenchmarkProtos

class InlineParserGeneratorSpec extends AnyFlatSpec with Matchers {

  "InlineParserGenerator" should "generate valid parser code for simple messages" in {
    // Use the SimpleMessage from benchmarks
    val descriptor = ComplexBenchmarkProtos.SimpleMessage.getDescriptor
    val schema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    val generatedCode = InlineParserGenerator.generateParser(
      "GeneratedSimpleParser",
      descriptor,
      schema
    )

    // Check that code contains expected elements
    generatedCode should include ("public final class GeneratedSimpleParser")
    generatedCode should include ("extends StreamWireParser")
    generatedCode should include ("ProtoRuntime.ArrayContext arrayCtx")
    generatedCode should include ("switch (tag)")

    // Check for field-specific cases
    generatedCode should include ("case 8:") // id field (tag 8 = field 1, varint)
    generatedCode should include ("ProtoRuntime.writeInt32")
    generatedCode should include ("case 18:") // name field (tag 18 = field 2, length-delimited)
    generatedCode should include ("ProtoRuntime.writeString")

    println("Generated code for SimpleMessage:")
    println(generatedCode)
  }

  it should "generate code for complex nested messages" in {
    val descriptor = ComplexBenchmarkProtos.ComplexMessageA.getDescriptor
    val schema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    val generatedCode = InlineParserGenerator.generateParser(
      "GeneratedComplexParser",
      descriptor,
      schema
    )

    // Check for repeated field handling
    generatedCode should include ("BufferList[]")
    generatedCode should include ("ProtoRuntime.collectInt32") // repeated numbers
    generatedCode should include ("ProtoRuntime.collectPackedInt32") // packed variant
    generatedCode should include ("ProtoRuntime.collectString") // repeated tags
    generatedCode should include ("ProtoRuntime.collectMessage") // repeated nested_messages

    // Check for nested message handling
    generatedCode should include ("parser_message_b")
    generatedCode should include ("setNestedParser10")

    // Check for flush operations
    generatedCode should include ("ProtoRuntime.flushStringArray")
    generatedCode should include ("ProtoRuntime.flushMessageArray")

    println("\nGenerated code size for ComplexMessageA: " + generatedCode.lines.size + " lines")
  }

  it should "generate parser method (not full class)" in {
    val descriptor = ComplexBenchmarkProtos.SimpleMessage.getDescriptor
    val schema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    val generatedMethod = InlineParserGenerator.generateParserMethod(
      "parseSimpleMessage",
      descriptor,
      schema
    )

    // Check method signature
    generatedMethod should include ("public static void parseSimpleMessage")
    generatedMethod should include ("byte[] data")
    generatedMethod should include ("NullDefaultRowWriter writer")

    // Check initialization
    generatedMethod should include ("CodedInputStream.newInstance(data)")
    generatedMethod should include ("writer.resetRowWriter()")

    println("\nGenerated method for SimpleMessage:")
    println(generatedMethod)
  }

  it should "handle messages with all primitive types" in {
    val descriptor = ComplexBenchmarkProtos.AllPrimitives.getDescriptor
    val schema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    val generatedCode = InlineParserGenerator.generateParser(
      "GeneratedAllPrimitivesParser",
      descriptor,
      schema
    )

    // Check for various primitive types
    generatedCode should include ("ProtoRuntime.writeInt32")
    generatedCode should include ("ProtoRuntime.writeInt64")
    generatedCode should include ("ProtoRuntime.writeFloat")
    generatedCode should include ("ProtoRuntime.writeDouble")
    generatedCode should include ("ProtoRuntime.writeBool")

    // Check for signed variants
    generatedCode should include ("ProtoRuntime.writeSInt32")
    generatedCode should include ("ProtoRuntime.writeSInt64")

    // Check for fixed-size variants
    generatedCode should include ("ProtoRuntime.writeFixed32")
    generatedCode should include ("ProtoRuntime.writeFixed64")

    println("\nGenerated code for AllPrimitives includes all type handlers")
  }

  it should "correctly map field numbers to ordinals" in {
    val descriptor = ComplexBenchmarkProtos.ComplexMessageB.getDescriptor
    val schema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    val generatedCode = InlineParserGenerator.generateParser(
      "GeneratedMessageBParser",
      descriptor,
      schema
    )

    // MessageB has fields with specific field numbers
    // identifier = 1, label = 2, score = 3, etc.
    generatedCode should include ("case 8:") // field 1 (identifier)
    generatedCode should include ("case 18:") // field 2 (label)
    generatedCode should include ("case 29:") // field 3 (score) - float is fixed32

    println("\nField mapping verified for ComplexMessageB")
  }
}