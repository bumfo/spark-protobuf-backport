package fastproto

import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import com.google.protobuf.Descriptors.{Descriptor, FileDescriptor}

import java.io.{File, FileInputStream, PrintWriter}
import java.nio.file.StandardCopyOption

/**
 * Code generator that uses ProtoToRowGenerator to create parser implementations
 * for protobuf messages defined in descriptor files. Supports both SBT task mode
 * and command-line usage.
 * 
 * SBT Task Mode (no args): 
 * Uses hardcoded paths for nested.proto testing. Prerequisites:
 *   cd core/src/test/resources && protoc --descriptor_set_out=nested.desc --include_imports nested.proto
 *
 * Command-line Mode (2 args):
 *   generateParsersTask <descriptor-file> <output-directory>
 */
object GenerateParsersTask {
  
  def main(args: Array[String]): Unit = {
    args.length match {
      case 0 => runSbtTaskMode()
      case 2 => runCommandLineMode(args(0), args(1))
      case _ => 
        println("Usage:")
        println("  No args: SBT task mode for nested.proto")
        println("  generateParsersTask <descriptor-file> <output-directory>")
        sys.exit(1)
    }
  }
  
  private def runSbtTaskMode(): Unit = {
    val baseDir = new File(".").getCanonicalFile
    val resourceDir = new File(baseDir, "src/test/resources")  // We're already in core/
    val targetDir = new File(baseDir, "target/test-generated")
    val descriptorFile = new File(resourceDir, "nested.desc") // Use pre-compiled descriptor
    
    println(s"Base directory: ${baseDir}")
    println(s"Resource directory: ${resourceDir}")
    println(s"Using pre-compiled descriptor: ${descriptorFile}")
    println(s"Descriptor exists: ${descriptorFile.exists()}")
    
    if (!descriptorFile.exists()) {
      println("Descriptor file not found. Please run: protoc --descriptor_set_out=nested.desc --include_imports nested.proto")
      return
    }
    
    // Copy descriptor to target directory for reference
    val targetDescriptor = new File(targetDir, "nested.desc")
    java.nio.file.Files.copy(descriptorFile.toPath, targetDescriptor.toPath, 
      StandardCopyOption.REPLACE_EXISTING)
    
    // Generate parser code
    println(s"Generating parser code using ProtoToRowGenerator")
    generateFromDescriptor(descriptorFile, targetDir)
    
    println(s"Code generation completed. Output directory: ${targetDir}")
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
      println(s"Generated parser code in: ${outputDir}")
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
          generateParserForMessage(messageDescriptor, outputDir)
        }
      }
    } finally {
      fis.close()
    }
  }

  private def generateParserForMessage(descriptor: Descriptor, outputDir: File): Unit = {
    val messageName = descriptor.getName
    val packageName = descriptor.getFile.getOptions.getJavaPackage
    val fullClassName = if (packageName.nonEmpty) s"${packageName}.${messageName}" else messageName
    
    println(s"Generating parser for message: ${fullClassName}")

    // Count distinct nested message types (same logic as createParserGraph)
    import scala.collection.JavaConverters._
    val nestedMessageFields = descriptor.getFields.asScala.filter(
      _.getJavaType == com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE
    )
    val distinctNestedTypes = nestedMessageFields.map(_.getMessageType.getFullName).toSet
    val nestedMessageCount = distinctNestedTypes.size
    
    // Generate the parser code using ProtoToRowGenerator's source code generation method
    val className = s"${messageName}Parser"
    val code = ProtoToRowGenerator.generateParserSourceCode(
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