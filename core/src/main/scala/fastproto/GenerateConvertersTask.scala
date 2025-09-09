package fastproto

import java.io.File
import scala.sys.process._

/**
 * Simple task runner that compiles protobuf to descriptor and generates converter code.
 * This avoids complex SBT task syntax issues.
 */
object GenerateConvertersTask {
  
  def main(args: Array[String]): Unit = {
    val baseDir = new File(".").getCanonicalFile
    val resourceDir = new File(baseDir, "src/test/resources")  // We're already in core/
    val targetDir = new File(baseDir, "target/test-generated")
    val descriptorFile = new File(resourceDir, "nested.desc") // Use pre-compiled descriptor
    
    // Ensure target directory exists
    targetDir.mkdirs()
    
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
      java.nio.file.StandardCopyOption.REPLACE_EXISTING)
    
    // Generate converter code using ProtoToRowGenerator
    println(s"Generating converter code using ProtoToRowGenerator")
    ConverterCodeGenerator.main(Array(descriptorFile.toString, targetDir.toString))
    
    println(s"Code generation completed. Output directory: ${targetDir}")
  }
}