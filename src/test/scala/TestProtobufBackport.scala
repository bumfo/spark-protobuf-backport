/*
 * Minimal test harness for the spark‑protobuf‑backport project.
 *
 * This program exercises the backported Protobuf functions using both a
 * compiled Java class and a descriptor file.  Rather than relying on a
 * custom schema compiled from an external .proto file, it uses one of
 * the standard messages shipped with the Protobuf runtime: `google.protobuf.Type`.
 * The `Type` message contains a string field and a repeated nested message
 * (`fields`), which allows us to verify that complex structures such as
 * arrays of structs are handled correctly by the backported functions.
 *
 * To execute the test locally:
 *
 *  1. Compile the backport project and assemble the shaded jar.
 *  2. Run this program on a Spark driver, for example using
 *     `spark-submit --class TestProtobufBackport --master local[*] ...`.
 *
 * The test constructs a `Type` message with two nested `Field` entries,
 * serializes it to bytes, and uses the backported `from_protobuf` function in
 * three forms: via a compiled class reference, via a descriptor file, and
 * via a binary descriptor set.  It asserts that the resulting rows match
 * the original data.  If all assertions pass "All tests passed" will be
 * printed to stdout.
 */

package org.apache.spark.sql.protobuf.backport

import com.google.protobuf._
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

object TestProtobufBackport {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("TestProtobufBackport")
      .getOrCreate()
    import spark.implicits._

    // Construct a Type message using the standard protobuf class.  The Type message
    // has a string field "name" and a repeated nested message field "fields".
    // Here we create a Type called "TestType" with two nested Field entries.
    val field1 = Field.newBuilder()
      .setName("strField")
      .setNumber(1)
      .setKind(Field.Kind.TYPE_STRING)
      .setCardinality(Field.Cardinality.CARDINALITY_OPTIONAL)
      .build()
    val field2 = Field.newBuilder()
      .setName("intField")
      .setNumber(2)
      .setKind(Field.Kind.TYPE_INT32)
      .setCardinality(Field.Cardinality.CARDINALITY_OPTIONAL)
      .build()
    val typ = Type.newBuilder()
      .setName("TestType")
      .addFields(field1)
      .addFields(field2)
      .build()
    val df = Seq(typ.toByteArray).toDF("data")

    // Use the compiled Java class for schema inference.  The resulting struct
    // contains fields corresponding to the Type message ("name", "fields", etc.).
    val structDf = df.select(
      functions.from_protobuf($"data", classOf[Type].getName).as("struct"),
    )
    // Extract the name and fields from the struct.  The first column is the name,
    // the second column is an array of structs representing the nested Field messages.
    val row1 = structDf.select("struct.name", "struct.fields").head()
    val typeName1 = row1.getString(0)
    // Each element of the "fields" array is a Row; extract the field names.
    val fieldNames1 = row1.getSeq[org.apache.spark.sql.Row](1).map(_.getAs[String]("name"))
    assert(typeName1 == "TestType", s"Expected TestType but got $typeName1")
    assert(fieldNames1 == Seq("strField", "intField"), s"Expected (Seq(strField,intField)) but got $fieldNames1")

    // Generate a descriptor file for Type and broadcast it.  We add only the
    // FileDescriptorProto for Type itself because its dependencies (e.g. google.api annotations)
    // are part of the standard descriptor set included in the Protobuf library.  Write it
    // to a temporary file and register it with Spark so executors can retrieve it.
    val descSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(Type.getDescriptor.getFile.toProto)
      .addFile(AnyProto.getDescriptor.getFile.toProto)
      .addFile(SourceContextProto.getDescriptor.getFile.toProto)
      .addFile(ApiProto.getDescriptor.getFile.toProto)
      // .addFile(OptionProto.getDescriptor.getFile.toProto)
      .build()
    val tempDesc = java.io.File.createTempFile("type_descriptor", ".desc")
    java.nio.file.Files.write(tempDesc.toPath, descSet.toByteArray)
    spark.sparkContext.addFile(tempDesc.getAbsolutePath)
    val fileName = tempDesc.getName
    val structDf2 = df.select(
      functions.from_protobuf(
        $"data",
        "google.protobuf.Type",
        SparkFiles.get(fileName),
      ).as("struct"),
    )
    val row2 = structDf2.select("struct.name", "struct.fields").head()
    val typeName2 = row2.getString(0)
    val fieldNames2 = row2.getSeq[org.apache.spark.sql.Row](1).map(_.getAs[String]("name"))
    assert(typeName2 == "TestType", s"Expected TestType but got $typeName2")
    assert(fieldNames2 == Seq("strField", "intField"), s"Expected (Seq(strField,intField)) but got $fieldNames2")

    // Use the binary descriptor set directly.  Pass the descriptor set bytes to the
    // function to avoid file I/O on executors.
    val descBytes = descSet.toByteArray
    val structDf3 = df.select(
      functions.from_protobuf($"data", "google.protobuf.Type", descBytes).as("struct"),
    )
    val row3 = structDf3.select("struct.name", "struct.fields").head()
    val typeName3 = row3.getString(0)
    val fieldNames3 = row3.getSeq[org.apache.spark.sql.Row](1).map(_.getAs[String]("name"))
    assert(typeName3 == "TestType", s"Expected TestType but got $typeName3")
    assert(fieldNames3 == Seq("strField", "intField"), s"Expected (Seq(strField,intField)) but got $fieldNames3")

    println("All tests passed")
    spark.stop()
  }
}
