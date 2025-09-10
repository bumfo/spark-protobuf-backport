package fastproto

import com.google.protobuf._
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.unsafe.types.UTF8String

class WireFormatConverterSpec extends AnyFlatSpec with Matchers {

  "WireFormatConverter" should "convert simple protobuf messages" in {
    // Create a simple Type message (which is available in protobuf-java)
    val typeMsg = Type.newBuilder()
      .setName("test_type")
      .addFields(Field.newBuilder()
        .setName("field1")
        .setNumber(1)
        .setKind(Field.Kind.TYPE_STRING)
        .build())
      .build()
    
    val binary = typeMsg.toByteArray
    val descriptor = typeMsg.getDescriptorForType
    
    // Create a simplified Spark schema that matches some fields
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("number", IntegerType, nullable = true)
      ))), nullable = true)
    ))
    
    val converter = new WireFormatConverter(descriptor, sparkSchema)
    
    // Test that converter doesn't crash
    noException should be thrownBy {
      converter.convert(binary)
    }
    
    // Test schema access
    converter.schema should equal(sparkSchema)
  }

  it should "handle empty messages" in {
    val typeMsg = Type.newBuilder().build()
    val binary = typeMsg.toByteArray
    val descriptor = typeMsg.getDescriptorForType
    
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true)
    ))
    
    val converter = new WireFormatConverter(descriptor, sparkSchema)
    
    noException should be thrownBy {
      val row = converter.convert(binary)
      row.numFields should be >= 0
    }
  }

  it should "handle unknown fields gracefully" in {
    // Create a message with more fields than our schema expects
    val typeMsg = Type.newBuilder()
      .setName("test")
      .setSourceContext(SourceContext.newBuilder().setFileName("test.proto").build())
      .build()
    
    val binary = typeMsg.toByteArray
    val descriptor = typeMsg.getDescriptorForType
    
    // Schema that only includes the name field
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true)
    ))
    
    val converter = new WireFormatConverter(descriptor, sparkSchema)
    
    noException should be thrownBy {
      val row = converter.convert(binary)
      row.numFields should equal(1)
    }
  }
}