package fastproto

import com.google.protobuf.Descriptors.{Descriptor, FileDescriptor}
import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import org.apache.spark.sql.types._

import java.io.{File, FileInputStream, PrintWriter}
import java.nio.file.StandardCopyOption

/**
 * Code generator that uses WireFormatToRowGenerator to create optimized converter implementations
 * for protobuf messages defined in descriptor files. Supports both SBT task mode
 * and command-line usage.
 *
 * SBT Task Mode (no args):
 * Uses google.protobuf.Type for demonstration. Shows generated code for parsing
 * protobuf wire format directly to Spark SQL rows with performance optimizations.
 *
 * Command-line Mode (2 args):
 *   GenerateWireFormatTask <descriptor-file> <output-directory>
 */
object GenerateWireFormatTask {

  def main(args: Array[String]): Unit = {
    args.length match {
      case 0 => runSbtTaskMode()
      case 2 => runCommandLineMode(args(0), args(1))
      case _ =>
        println("Usage:")
        println("  No args: SBT task mode for google.protobuf.Type demo")
        println("  GenerateWireFormatTask <descriptor-file> <output-directory>")
        sys.exit(1)
    }
  }

  private def runSbtTaskMode(): Unit = {
    val baseDir = new File(".").getCanonicalFile
    val targetDir = new File(baseDir, "target/wireformat-generated")
    targetDir.mkdirs()

    println(s"Base directory: ${baseDir}")
    println(s"Target directory: ${targetDir}")

    // Use google.protobuf.Type as an example message
    val typeMsg = com.google.protobuf.Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    println(s"Generating WireFormat converter code for: ${descriptor.getFullName}")

    // Create a sample Spark schema that matches some Type fields
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("kind", StringType, nullable = true),
        StructField("cardinality", StringType, nullable = true),
        StructField("number", IntegerType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("type_url", StringType, nullable = true),
        StructField("oneof_index", IntegerType, nullable = true),
        StructField("packed", BooleanType, nullable = true),
        StructField("options", ArrayType(StructType(Seq(
          StructField("name", StringType, nullable = true),
          StructField("value", StructType(Seq(
            StructField("type_url", StringType, nullable = true),
            StructField("value", BinaryType, nullable = true)
          )), nullable = true)
        ))), nullable = true),
        StructField("json_name", StringType, nullable = true),
        StructField("default_value", StringType, nullable = true)
      ))), nullable = true),
      StructField("oneofs", ArrayType(StringType), nullable = true),
      StructField("options", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("value", StructType(Seq(
          StructField("type_url", StringType, nullable = true),
          StructField("value", BinaryType, nullable = true)
        )), nullable = true)
      ))), nullable = true),
      StructField("source_context", StructType(Seq(
        StructField("file_name", StringType, nullable = true)
      )), nullable = true),
      StructField("syntax", StringType, nullable = true)
    ))

    // Generate converter code
    println("Generating WireFormat converter code using WireFormatToRowGenerator")
    generateConverterForMessage(descriptor, sparkSchema, targetDir)

    // Also generate a simplified version for comparison
    val simpleSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("syntax", StringType, nullable = true)
    ))

    println("Generating simplified WireFormat converter for comparison")
    generateConverterForMessage(descriptor, simpleSchema, targetDir, "Simple")

    println(s"WireFormat code generation completed. Output directory: ${targetDir}")
  }

  private def runCommandLineMode(descriptorPath: String, outputPath: String): Unit = {
    val descriptorFile = new File(descriptorPath)
    val outputDir = new File(outputPath)

    if (!descriptorFile.exists()) {
      println(s"Descriptor file does not exist: ${descriptorFile}")
      sys.exit(1)
    }

    try {
      generateFromDescriptor(descriptorFile, outputDir)
      println(s"Generated WireFormat converter code in: ${outputDir}")
    } catch {
      case e: Exception =>
        println(s"Code generation failed: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    }
  }

  private def generateFromDescriptor(descriptorFile: File, outputDir: File): Unit = {
    outputDir.mkdirs()

    // Load the descriptor set
    val fis = new FileInputStream(descriptorFile)
    try {
      val descriptorSet = FileDescriptorSet.parseFrom(fis)

      // Build file descriptors
      import scala.collection.JavaConverters._
      val fileDescriptors = descriptorSet.getFileList.asScala.map { fileDescriptorProto =>
        FileDescriptor.buildFrom(fileDescriptorProto, Array.empty[FileDescriptor])
      }

      // Generate code for each message type
      fileDescriptors.foreach { fileDescriptor =>
        fileDescriptor.getMessageTypes.forEach { messageDescriptor =>
          // Create a basic schema for all fields
          val basicSchema = createBasicSchemaForDescriptor(messageDescriptor)
          generateConverterForMessage(messageDescriptor, basicSchema, outputDir)
        }
      }
    } finally {
      fis.close()
    }
  }

  private def generateConverterForMessage(
      descriptor: Descriptor,
      schema: StructType,
      outputDir: File,
      suffix: String = ""): Unit = {

    val messageName = descriptor.getName
    val packageName = descriptor.getFile.getOptions.getJavaPackage
    val fullClassName = if (packageName.nonEmpty) s"${packageName}.${messageName}" else messageName

    val className = s"${messageName}${suffix}WireFormatConverter"
    println(s"Generating WireFormat converter for message: ${fullClassName} -> ${className}")

    // Generate the converter code using WireFormatToRowGenerator's source code generation method
    val code = generateWireFormatConverterSource(className, descriptor, schema)

    // Write the generated code to a file
    val outputFile = new File(outputDir, s"${className}.java")
    val writer = new PrintWriter(outputFile)
    try {
      writer.write(code.toString())
      println(s"Generated WireFormat converter: ${outputFile}")
    } finally {
      writer.close()
    }

    // Also write the descriptor and schema information
    writeWireFormatInfo(descriptor, schema, outputDir, s"${messageName}${suffix}")
  }

  private def generateWireFormatConverterSource(
      className: String,
      descriptor: Descriptor,
      schema: StructType): StringBuilder = {

    // Use reflection to access the private generateSourceCode method from WireFormatToRowGenerator
    try {
      val generatorClass = WireFormatToRowGenerator.getClass
      val generateSourceCodeMethod = generatorClass.getDeclaredMethod(
        "generateSourceCode",
        classOf[String],
        classOf[com.google.protobuf.Descriptors.Descriptor],
        classOf[StructType]
      )
      generateSourceCodeMethod.setAccessible(true)

      val sourceCode = generateSourceCodeMethod.invoke(
        WireFormatToRowGenerator,
        className,
        descriptor,
        schema
      ).asInstanceOf[StringBuilder]

      sourceCode
    } catch {
      case e: Exception =>
        println(s"Warning: Could not access generateSourceCode method: ${e.getMessage}")
        // Return a template showing what would be generated
        createExampleWireFormatSource(className, descriptor, schema)
    }
  }

  private def createExampleWireFormatSource(
      className: String,
      descriptor: Descriptor,
      schema: StructType): StringBuilder = {

    val code = new StringBuilder

    code ++= s"""// This is an example of what WireFormatToRowGenerator would generate
// for message: ${descriptor.getFullName}
//
// The actual generated code would be produced by calling:
// WireFormatToRowGenerator.generateConverter(descriptor, schema)

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import fastproto.AbstractWireFormatConverter;

public final class $className extends AbstractWireFormatConverter {

  // Field ordinal constants for compile-time optimization
"""

    // Generate field constants
    import scala.collection.JavaConverters._
    descriptor.getFields.asScala.foreach { field =>
      try {
        val ordinal = schema.fieldIndex(field.getName)
        val wireType = getWireTypeForField(field)
        code ++= s"  private static final int FIELD_${field.getNumber}_ORDINAL = $ordinal;\n"
        code ++= s"  private static final int FIELD_${field.getNumber}_WIRE_TYPE = $wireType;\n"
      } catch {
        case _: IllegalArgumentException => // Field not in schema
      }
    }

    code ++= s"""
  public $className(StructType schema) {
    super(schema);
  }

  @Override
  protected void writeData(byte[] binary, UnsafeRowWriter writer) {
    CodedInputStream input = CodedInputStream.newInstance(binary);

    try {
      while (!input.isAtEnd()) {
        int tag = input.readTag();
        int fieldNumber = WireFormat.getTagFieldNumber(tag);
        int wireType = WireFormat.getTagWireType(tag);

        // Generated field parsing with inlined type information
        // This eliminates runtime type checking and branching
"""

    // Generate sample field parsing
    val schemaFields = descriptor.getFields.asScala
      .filter(field => schema.fieldNames.contains(field.getName))
      .take(3) // Show first 3 fields as example

    schemaFields.zipWithIndex.foreach { case (field, index) =>
      val ifClause = if (index == 0) "if" else "} else if"
      code ++= s"""        $ifClause (fieldNumber == ${field.getNumber}) {
          // Parse ${field.getName} (${field.getType})
          if (wireType == FIELD_${field.getNumber}_WIRE_TYPE) {
            // Inlined parsing logic for ${field.getType}
            ${generateParsingLogicExample(field)}
          } else {
            input.skipField(tag);
          }
"""
    }

    if (schemaFields.nonEmpty) {
      code ++= "        } else {\n"
    }

    code ++= """          // Unknown field - skip
          input.skipField(tag);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse wire format", e);
    }
  }

  // Generated helper methods would appear here for:
  // - Enum value lookups with switch statements
  // - Repeated field array writing
  // - Nested message processing
}

// Key Performance Optimizations in Generated Code:
// 1. Field ordinals and wire types are constants (no runtime lookup)
// 2. Field parsing is inlined (no method calls or switch statements)
// 3. Type-specific parsing eliminates runtime type checking
// 4. Branch prediction friendly ordered field dispatch
// 5. Primitive arrays used to avoid boxing overhead
"""

    code
  }

  private def generateParsingLogicExample(field: com.google.protobuf.Descriptors.FieldDescriptor): String = {
    import com.google.protobuf.Descriptors.FieldDescriptor
    field.getType match {
      case FieldDescriptor.Type.STRING =>
        s"writer.write(FIELD_${field.getNumber}_ORDINAL, UTF8String.fromBytes(input.readBytes().toByteArray()));"
      case FieldDescriptor.Type.INT32 =>
        s"writer.write(FIELD_${field.getNumber}_ORDINAL, input.readInt32());"
      case FieldDescriptor.Type.ENUM =>
        s"writer.write(FIELD_${field.getNumber}_ORDINAL, UTF8String.fromString(getEnumName${field.getNumber}(input.readEnum())));"
      case FieldDescriptor.Type.MESSAGE =>
        s"writeMessage(input.readBytes().toByteArray(), ${field.getNumber}, FIELD_${field.getNumber}_ORDINAL, writer);"
      case _ =>
        s"// ${field.getType} parsing logic would be generated here"
    }
  }

  private def getWireTypeForField(field: com.google.protobuf.Descriptors.FieldDescriptor): Int = {
    import com.google.protobuf.Descriptors.FieldDescriptor
    import com.google.protobuf.WireFormat

    field.getType match {
      case FieldDescriptor.Type.DOUBLE => WireFormat.WIRETYPE_FIXED64
      case FieldDescriptor.Type.FLOAT => WireFormat.WIRETYPE_FIXED32
      case FieldDescriptor.Type.INT64 | FieldDescriptor.Type.UINT64 |
           FieldDescriptor.Type.INT32 | FieldDescriptor.Type.UINT32 |
           FieldDescriptor.Type.BOOL | FieldDescriptor.Type.ENUM |
           FieldDescriptor.Type.SINT32 | FieldDescriptor.Type.SINT64 => WireFormat.WIRETYPE_VARINT
      case FieldDescriptor.Type.FIXED64 | FieldDescriptor.Type.SFIXED64 => WireFormat.WIRETYPE_FIXED64
      case FieldDescriptor.Type.FIXED32 | FieldDescriptor.Type.SFIXED32 => WireFormat.WIRETYPE_FIXED32
      case FieldDescriptor.Type.STRING | FieldDescriptor.Type.BYTES |
           FieldDescriptor.Type.MESSAGE => WireFormat.WIRETYPE_LENGTH_DELIMITED
      case _ => WireFormat.WIRETYPE_LENGTH_DELIMITED
    }
  }

  private def writeWireFormatInfo(
      descriptor: Descriptor,
      schema: StructType,
      outputDir: File,
      messageName: String): Unit = {

    val infoFile = new File(outputDir, s"${messageName}WireFormat_info.txt")
    val writer = new PrintWriter(infoFile)
    try {
      writer.println("WireFormat Code Generator Analysis")
      writer.println("="*50)
      writer.println(s"Message: ${descriptor.getFullName}")
      writer.println(s"Package: ${descriptor.getFile.getOptions.getJavaPackage}")
      writer.println()

      writer.println("Protobuf Fields:")
      descriptor.getFields.forEach { field =>
        val wireType = getWireTypeForField(field)
        writer.println(s"  ${field.getNumber}. ${field.getName}: ${field.getType} " +
          s"(wire type: $wireType, repeated: ${field.isRepeated})")
      }

      writer.println()
      writer.println("Target Spark Schema:")
      schema.fields.zipWithIndex.foreach { case (field, index) =>
        writer.println(s"  $index. ${field.name}: ${field.dataType} (nullable: ${field.nullable})")
      }

      writer.println()
      writer.println("Performance Optimizations:")
      writer.println("- Field ordinals pre-computed as constants")
      writer.println("- Wire types validated at compile time")
      writer.println("- Type-specific parsing logic inlined")
      writer.println("- Branch prediction friendly field dispatch")
      writer.println("- No runtime type checking or method dispatch")

      println(s"Generated info: ${infoFile}")
    } finally {
      writer.close()
    }
  }

  private def createBasicSchemaForDescriptor(descriptor: Descriptor): StructType = {
    import scala.collection.JavaConverters._

    val fields = descriptor.getFields.asScala.map { field => // Handle all fields, not just 5
      val dataType = field.getType match {
        case com.google.protobuf.Descriptors.FieldDescriptor.Type.STRING => StringType
        case com.google.protobuf.Descriptors.FieldDescriptor.Type.INT32 |
             com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT32 => IntegerType
        case com.google.protobuf.Descriptors.FieldDescriptor.Type.INT64 |
             com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT64 => LongType
        case com.google.protobuf.Descriptors.FieldDescriptor.Type.FLOAT => FloatType
        case com.google.protobuf.Descriptors.FieldDescriptor.Type.DOUBLE => DoubleType
        case com.google.protobuf.Descriptors.FieldDescriptor.Type.BOOL => BooleanType
        case com.google.protobuf.Descriptors.FieldDescriptor.Type.BYTES => BinaryType
        case com.google.protobuf.Descriptors.FieldDescriptor.Type.ENUM => StringType
        case com.google.protobuf.Descriptors.FieldDescriptor.Type.MESSAGE =>
          // Create a simple nested structure for messages
          StructType(Seq(StructField("data", StringType, nullable = true)))
        case _ => StringType
      }

      val finalType = if (field.isRepeated) ArrayType(dataType) else dataType
      StructField(field.getName, finalType, nullable = true)
    }

    StructType(fields.toSeq)
  }
}