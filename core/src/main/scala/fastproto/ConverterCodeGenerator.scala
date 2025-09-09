package fastproto

import com.google.protobuf.Descriptors.{Descriptor, FileDescriptor}
import com.google.protobuf.DescriptorProtos.FileDescriptorSet

import java.io.{File, FileInputStream, PrintWriter}
import java.nio.file.{Files, Paths}

/**
 * Code generator that uses ProtoToRowGenerator to create converter implementations
 * for protobuf messages defined in descriptor files. This is primarily for testing
 * and demonstration purposes.
 */
object ConverterCodeGenerator {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: ConverterCodeGenerator <descriptor-file> <output-directory>")
      sys.exit(1)
    }

    val descriptorFile = new File(args(0))
    val outputDir = new File(args(1))

    if (!descriptorFile.exists()) {
      println(s"Descriptor file does not exist: ${descriptorFile}")
      sys.exit(1)
    }

    outputDir.mkdirs()

    try {
      generateConverterCode(descriptorFile, outputDir)
      println(s"Generated converter code in: ${outputDir}")
    } catch {
      case e: Exception =>
        println(s"Code generation failed: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    }
  }

  private def generateConverterCode(descriptorFile: File, outputDir: File): Unit = {
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
          generateConverterForMessage(messageDescriptor, outputDir)
        }
      }
    } finally {
      fis.close()
    }
  }

  private def generateConverterForMessage(descriptor: Descriptor, outputDir: File): Unit = {
    val messageName = descriptor.getName
    val packageName = descriptor.getFile.getOptions.getJavaPackage
    val fullClassName = if (packageName.nonEmpty) s"${packageName}.${messageName}" else messageName
    
    println(s"Generating converter for message: ${fullClassName}")

    // Count distinct nested message types (same logic as createConverterGraph)
    import scala.collection.JavaConverters._
    val nestedMessageFields = descriptor.getFields.asScala.filter(
      _.getJavaType == com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE
    )
    val distinctNestedTypes = nestedMessageFields.map(_.getMessageType.getFullName).toSet
    val nestedMessageCount = distinctNestedTypes.size
    
    // Generate the converter code using ProtoToRowGenerator's source code generation method
    val className = s"${messageName}Converter"
    val code = ProtoToRowGenerator.generateConverterSourceCode(
      className, 
      descriptor, 
      classOf[com.google.protobuf.Message], // Placeholder class since we're only generating code
      nestedMessageCount
    )

    // Write the generated code to a file
    val outputFile = new File(outputDir, s"${className}.java")
    val writer = new PrintWriter(outputFile)
    try {
      writer.write(code.toString())
      println(s"Generated: ${outputFile}")
    } finally {
      writer.close()
    }

    // Also write the descriptor information
    writeDescriptorInfo(descriptor, outputDir, messageName)
  }

  private def writeDescriptorInfo(descriptor: Descriptor, outputDir: File, messageName: String): Unit = {
    val infoFile = new File(outputDir, s"${messageName}_info.txt")
    val writer = new PrintWriter(infoFile)
    try {
      writer.println(s"Message: ${descriptor.getFullName}")
      writer.println(s"Package: ${descriptor.getFile.getOptions.getJavaPackage}")
      writer.println(s"Fields:")
      
      descriptor.getFields.forEach { field =>
        writer.println(s"  - ${field.getName}: ${field.getJavaType} (repeated: ${field.isRepeated})")
      }
      
      println(s"Generated info: ${infoFile}")
    } finally {
      writer.close()
    }
  }
}