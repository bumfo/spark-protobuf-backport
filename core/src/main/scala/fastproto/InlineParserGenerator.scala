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

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.WireFormat
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

/**
 * Generates compact inline parsers for protobuf messages that integrate with Spark's
 * columnar execution engine.
 *
 * The generated code uses a switch statement for efficient dispatch (JIT compiles
 * to jump table) and calls minimal static methods from ProtoRuntime. This approach:
 * - Minimizes generated code size (~50-100 lines vs 300+ for current approach)
 * - Leverages existing optimized writers (NullDefaultRowWriter, PrimitiveArrayWriter)
 * - Enables JIT inlining of small static methods
 * - Avoids virtual method calls in hot paths
 */
object InlineParserGenerator {

  /**
   * Generate a complete parser class for a protobuf message.
   */
  def generateParser(
      className: String,
      descriptor: Descriptor,
      schema: StructType): String = {
    val fields = descriptor.getFields.asScala.toSeq
    val fieldMapping = buildFieldMapping(descriptor, schema)

    // Separate fields by type
    val repeatedPrimitives = fields.filter(f => f.isRepeated && isPrimitive(f))
    val repeatedStrings = fields.filter(f => f.isRepeated && isString(f))
    val repeatedBytes = fields.filter(f => f.isRepeated && isBytes(f))
    val repeatedMessages = fields.filter(f => f.isRepeated && isMessage(f))

    // All repeated variable-length fields (strings, bytes, messages)
    val repeatedVarLength = repeatedStrings ++ repeatedBytes ++ repeatedMessages

    // Build nested parsers map
    val nestedParsers = fields
      .filter(f => isMessage(f))
      .map(f => f.getNumber -> s"parser_${f.getName}")
      .toMap

    s"""
    |package fastproto.generated;
    |import com.google.protobuf.CodedInputStream;
    |import fastproto.*;
    |import java.io.IOException;
    |
    |public final class $className extends StreamWireParser {
    |
    |  ${generateNestedParserFields(nestedParsers)}
    |
    |  public $className(org.apache.spark.sql.types.StructType schema) {
    |    super(schema);
    |  }
    |
    |  @Override
    |  protected void parseInto(CodedInputStream input, RowWriter writer) throws IOException {
    |    // Cast to NullDefaultRowWriter for optimized null handling
    |    NullDefaultRowWriter w = (NullDefaultRowWriter) writer;
    |
    |    // Single array context for all primitive arrays
    |    ProtoRuntime.ArrayContext arrayCtx = new ProtoRuntime.ArrayContext();
    |
    |    // BufferLists for repeated variable-length fields
    |    ${if (repeatedVarLength.nonEmpty) s"BufferList[] bufferLists = new BufferList[${repeatedVarLength.size}];" else ""}
    |
    |    // Parse loop with switch for JIT optimization
    |    while (!input.isAtEnd()) {
    |      int tag = input.readTag();
    |
    |      switch (tag) {
    |        ${generateSwitchCases(fields, fieldMapping, repeatedVarLength)}
    |        default:
    |          input.skipField(tag);
    |          break;
    |      }
    |    }
    |
    |    // Complete any pending primitive array
    |    arrayCtx.completeIfActive(w);
    |
    |    // Flush all collected arrays
    |    ${generateFlushCode(repeatedVarLength, fieldMapping, nestedParsers)}
    |  }
    |
    |  ${generateNestedParserSetters(nestedParsers)}
    |}
    """.stripMargin
  }

  /**
   * Generate code for a single parser method (not a full class).
   */
  def generateParserMethod(
      methodName: String,
      descriptor: Descriptor,
      schema: StructType): String = {
    val fields = descriptor.getFields.asScala.toSeq
    val fieldMapping = buildFieldMapping(descriptor, schema)

    // Separate fields by type
    val repeatedVarLength = fields.filter(f =>
      f.isRepeated && (isString(f) || isBytes(f) || isMessage(f)))

    val nestedParsers = fields
      .filter(f => isMessage(f))
      .map(f => f.getNumber -> s"${methodName}_${f.getName}")
      .toMap

    s"""
    |public static void $methodName(byte[] data, NullDefaultRowWriter writer) throws IOException {
    |  CodedInputStream input = CodedInputStream.newInstance(data);
    |  ProtoRuntime.ArrayContext arrayCtx = new ProtoRuntime.ArrayContext();
    |  ${if (repeatedVarLength.nonEmpty) s"BufferList[] bufferLists = new BufferList[${repeatedVarLength.size}];" else ""}
    |
    |  writer.resetRowWriter();
    |
    |  while (!input.isAtEnd()) {
    |    int tag = input.readTag();
    |    switch (tag) {
    |      ${generateSwitchCases(fields, fieldMapping, repeatedVarLength)}
    |      default: input.skipField(tag); break;
    |    }
    |  }
    |
    |  arrayCtx.completeIfActive(writer);
    |  ${generateFlushCode(repeatedVarLength, fieldMapping, nestedParsers)}
    |}
    """.stripMargin
  }

  private def buildFieldMapping(descriptor: Descriptor, schema: StructType): Map[Int, Int] = {
    val schemaFields = schema.fields.map(_.name.toLowerCase).zipWithIndex.toMap
    descriptor.getFields.asScala.map { field =>
      val ordinal = schemaFields.get(field.getName.toLowerCase).getOrElse(-1)
      field.getNumber -> ordinal
    }.filter(_._2 >= 0).toMap
  }

  private def generateSwitchCases(
      fields: Seq[FieldDescriptor],
      fieldMapping: Map[Int, Int],
      repeatedVarLength: Seq[FieldDescriptor]): String = {

    val varLengthIndices = repeatedVarLength.zipWithIndex.map { case (f, i) =>
      f.getNumber -> i
    }.toMap

    fields.flatMap { field =>
      val fieldNumber = field.getNumber
      val ordinal = fieldMapping.getOrElse(fieldNumber, -1)

      if (ordinal < 0) {
        // Field not in schema - skip
        Nil
      } else if (field.isRepeated) {
        // Repeated field - generate both unpacked and packed cases
        generateRepeatedCases(field, ordinal, varLengthIndices.get(fieldNumber))
      } else {
        // Single field
        List(generateSingleCase(field, ordinal))
      }
    }.mkString("\n        ")
  }

  private def generateSingleCase(field: FieldDescriptor, ordinal: Int): String = {
    val tag = makeTag(field.getNumber, getWireType(field))
    val methodCall = field.getType match {
      case FieldDescriptor.Type.INT32 => s"ProtoRuntime.writeInt32(input, w, $ordinal);"
      case FieldDescriptor.Type.INT64 => s"ProtoRuntime.writeInt64(input, w, $ordinal);"
      case FieldDescriptor.Type.UINT32 => s"ProtoRuntime.writeUInt32(input, w, $ordinal);"
      case FieldDescriptor.Type.UINT64 => s"ProtoRuntime.writeUInt64(input, w, $ordinal);"
      case FieldDescriptor.Type.SINT32 => s"ProtoRuntime.writeSInt32(input, w, $ordinal);"
      case FieldDescriptor.Type.SINT64 => s"ProtoRuntime.writeSInt64(input, w, $ordinal);"
      case FieldDescriptor.Type.FIXED32 => s"ProtoRuntime.writeFixed32(input, w, $ordinal);"
      case FieldDescriptor.Type.FIXED64 => s"ProtoRuntime.writeFixed64(input, w, $ordinal);"
      case FieldDescriptor.Type.SFIXED32 => s"ProtoRuntime.writeSFixed32(input, w, $ordinal);"
      case FieldDescriptor.Type.SFIXED64 => s"ProtoRuntime.writeSFixed64(input, w, $ordinal);"
      case FieldDescriptor.Type.FLOAT => s"ProtoRuntime.writeFloat(input, w, $ordinal);"
      case FieldDescriptor.Type.DOUBLE => s"ProtoRuntime.writeDouble(input, w, $ordinal);"
      case FieldDescriptor.Type.BOOL => s"ProtoRuntime.writeBool(input, w, $ordinal);"
      case FieldDescriptor.Type.ENUM => s"ProtoRuntime.writeEnum(input, w, $ordinal);"
      case FieldDescriptor.Type.STRING => s"ProtoRuntime.writeString(input, w, $ordinal, arrayCtx);"
      case FieldDescriptor.Type.BYTES => s"ProtoRuntime.writeBytes(input, w, $ordinal, arrayCtx);"
      case FieldDescriptor.Type.MESSAGE =>
        s"ProtoRuntime.writeMessage(input, w, $ordinal, arrayCtx, parser_${field.getName});"
      case FieldDescriptor.Type.GROUP =>
        throw new UnsupportedOperationException("GROUP type is deprecated and not supported")
    }

    s"case $tag: $methodCall break; // ${field.getName}"
  }

  private def generateRepeatedCases(
      field: FieldDescriptor,
      ordinal: Int,
      bufferListIndex: Option[Int]): List[String] = {

    val unpackedTag = makeTag(field.getNumber, getWireType(field))

    field.getType match {
      case FieldDescriptor.Type.STRING =>
        val idx = bufferListIndex.get
        List(s"case $unpackedTag: ProtoRuntime.collectString(input, bufferLists, $idx, arrayCtx, w); break; // ${field.getName}[]")

      case FieldDescriptor.Type.BYTES =>
        val idx = bufferListIndex.get
        List(s"case $unpackedTag: ProtoRuntime.collectBytes(input, bufferLists, $idx, arrayCtx, w); break; // ${field.getName}[]")

      case FieldDescriptor.Type.MESSAGE =>
        val idx = bufferListIndex.get
        List(s"case $unpackedTag: ProtoRuntime.collectMessage(input, bufferLists, $idx, arrayCtx, w); break; // ${field.getName}[]")

      case FieldDescriptor.Type.GROUP =>
        throw new UnsupportedOperationException("GROUP type is deprecated and not supported")

      case primitiveType if isPackable(field) =>
        val packedTag = makeTag(field.getNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED)
        val typeName = primitiveType.toString.split("_").map(_.toLowerCase.capitalize).mkString
        val unpackedMethod = s"ProtoRuntime.collect$typeName(input, arrayCtx, w, $ordinal);"
        val packedMethod = s"ProtoRuntime.collectPacked$typeName(input, arrayCtx, w, $ordinal);"

        List(
          s"case $unpackedTag: $unpackedMethod break; // ${field.getName}[]",
          s"case $packedTag: $packedMethod break; // ${field.getName}[] packed"
        )

      case primitiveType =>
        val typeName = primitiveType.toString.split("_").map(_.toLowerCase.capitalize).mkString
        List(s"case $unpackedTag: ProtoRuntime.collect$typeName(input, arrayCtx, w, $ordinal); break; // ${field.getName}[]")
    }
  }

  private def generateFlushCode(
      repeatedVarLength: Seq[FieldDescriptor],
      fieldMapping: Map[Int, Int],
      nestedParsers: Map[Int, String]): String = {

    if (repeatedVarLength.isEmpty) return ""

    repeatedVarLength.zipWithIndex.map { case (field, idx) =>
      val ordinal = fieldMapping(field.getNumber)
      field.getType match {
        case FieldDescriptor.Type.STRING =>
          s"ProtoRuntime.flushStringArray(bufferLists[$idx], $ordinal, w);"
        case FieldDescriptor.Type.BYTES =>
          s"ProtoRuntime.flushBytesArray(bufferLists[$idx], $ordinal, w);"
        case FieldDescriptor.Type.MESSAGE =>
          val parser = nestedParsers(field.getNumber)
          s"ProtoRuntime.flushMessageArray(bufferLists[$idx], $ordinal, $parser, w);"
        case _ => ""
      }
    }.mkString("\n    ")
  }

  private def generateNestedParserFields(nestedParsers: Map[Int, String]): String = {
    if (nestedParsers.isEmpty) return ""
    nestedParsers.values.map { name =>
      s"private StreamWireParser $name;"
    }.mkString("\n  ")
  }

  private def generateNestedParserSetters(nestedParsers: Map[Int, String]): String = {
    if (nestedParsers.isEmpty) return ""
    nestedParsers.map { case (fieldNum, name) =>
      s"""
      |public void setNestedParser$fieldNum(StreamWireParser parser) {
      |  if (this.$name != null) throw new IllegalStateException("Parser $fieldNum already set");
      |  this.$name = parser;
      |}
      """.stripMargin
    }.mkString("\n")
  }

  private def makeTag(fieldNumber: Int, wireType: Int): Int = {
    // WireFormat.makeTag is package-private, so compute manually
    // tag = (field_number << 3) | wire_type
    (fieldNumber << 3) | wireType
  }

  private def getWireType(field: FieldDescriptor): Int = {
    import FieldDescriptor.Type._
    field.getType match {
      case INT32 | INT64 | UINT32 | UINT64 | SINT32 | SINT64 | ENUM | BOOL =>
        WireFormat.WIRETYPE_VARINT
      case FIXED32 | SFIXED32 =>
        WireFormat.WIRETYPE_FIXED32
      case FIXED64 | SFIXED64 =>
        WireFormat.WIRETYPE_FIXED64
      case FLOAT =>
        WireFormat.WIRETYPE_FIXED32
      case DOUBLE =>
        WireFormat.WIRETYPE_FIXED64
      case STRING | BYTES | MESSAGE =>
        WireFormat.WIRETYPE_LENGTH_DELIMITED
      case GROUP =>
        WireFormat.WIRETYPE_START_GROUP
    }
  }

  private def isPrimitive(field: FieldDescriptor): Boolean = {
    import FieldDescriptor.Type._
    field.getType match {
      case STRING | BYTES | MESSAGE | GROUP => false
      case _ => true
    }
  }

  private def isString(field: FieldDescriptor): Boolean = {
    field.getType == FieldDescriptor.Type.STRING
  }

  private def isBytes(field: FieldDescriptor): Boolean = {
    field.getType == FieldDescriptor.Type.BYTES
  }

  private def isMessage(field: FieldDescriptor): Boolean = {
    field.getType == FieldDescriptor.Type.MESSAGE
  }

  private def isPackable(field: FieldDescriptor): Boolean = {
    import FieldDescriptor.Type._
    field.getType match {
      case INT32 | INT64 | UINT32 | UINT64 | SINT32 | SINT64 |
           FIXED32 | FIXED64 | SFIXED32 | SFIXED64 |
           FLOAT | DOUBLE | BOOL | ENUM => true
      case _ => false
    }
  }
}