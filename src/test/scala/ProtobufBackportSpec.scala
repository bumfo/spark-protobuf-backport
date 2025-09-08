package org.apache.spark.sql.protobuf.backport

import com.google.protobuf._
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ProtobufBackportSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  
  override def beforeAll(): Unit = {
    // Set log level before creating SparkSession to suppress startup INFO messages
    org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("org.spark_project").setLevel(org.apache.log4j.Level.ERROR)
    
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("ProtobufBackportSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
      .config("spark.hadoop.yarn.timeline-service.enabled", "false")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
      .getOrCreate()
    
    // Set log level to ERROR to suppress INFO messages
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      try {
        spark.stop()
        Thread.sleep(100)
      } catch {
        case _: Exception => // Ignore shutdown exceptions
      }
    }
  }

  "Protobuf backport" should "convert protobuf binary to Catalyst using compiled class" in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    
    // Construct a Type message using the standard protobuf class
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

    // Use the compiled Java class for schema inference
    val structDf = df.select(
      functions.from_protobuf($"data", classOf[Type].getName).as("struct"),
    )
    
    val row = structDf.select("struct.name", "struct.fields").head()
    val typeName = row.getString(0)
    val fieldNames = row.getSeq[org.apache.spark.sql.Row](1).map(_.getAs[String]("name"))
    
    typeName should be ("TestType")
    fieldNames should be (Seq("strField", "intField"))
  }

  it should "convert protobuf binary to Catalyst using descriptor file" in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    
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

    // Generate a descriptor file for Type and broadcast it
    val descSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(Type.getDescriptor.getFile.toProto)
      .addFile(AnyProto.getDescriptor.getFile.toProto)
      .addFile(SourceContextProto.getDescriptor.getFile.toProto)
      .addFile(ApiProto.getDescriptor.getFile.toProto)
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
    
    typeName2 should be ("TestType")
    fieldNames2 should be (Seq("strField", "intField"))
  }

  it should "convert protobuf binary to Catalyst using binary descriptor set" in {
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    
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

    // Use the binary descriptor set directly
    val descSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(Type.getDescriptor.getFile.toProto)
      .addFile(AnyProto.getDescriptor.getFile.toProto)
      .addFile(SourceContextProto.getDescriptor.getFile.toProto)
      .addFile(ApiProto.getDescriptor.getFile.toProto)
      .build()
    val descBytes = descSet.toByteArray
    
    val structDf3 = df.select(
      functions.from_protobuf($"data", "google.protobuf.Type", descBytes).as("struct"),
    )
    
    val row3 = structDf3.select("struct.name", "struct.fields").head()
    val typeName3 = row3.getString(0)
    val fieldNames3 = row3.getSeq[org.apache.spark.sql.Row](1).map(_.getAs[String]("name"))
    
    typeName3 should be ("TestType")
    fieldNames3 should be (Seq("strField", "intField"))
  }
}