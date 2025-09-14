package benchmark

import com.google.protobuf.DescriptorProtos

import java.io.File
import java.nio.file.Files

/**
 * Utility to generate WireFormat converter code for our SimpleMessage benchmark proto.
 * This helps us see exactly what code is generated for our 120-field schema.
 */
object GenerateSimpleProtoCode {

  def main(args: Array[String]): Unit = {
    generateWireFormatCode()
  }

  // Step 1: Create a descriptor file for SimpleMessage
  def createSimpleDescriptorFile(): File = {
    println("Creating descriptor file for SimpleMessage...")

    val simpleMessage = SimpleBenchmarkProtos.SimpleMessage.newBuilder().build()
    val descriptor = simpleMessage.getDescriptorForType

    println(s"Message: ${descriptor.getFullName}")
    println(s"Fields: ${descriptor.getFields.size()}")

    // Create FileDescriptorSet
    val descriptorSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(descriptor.getFile.toProto)
      .build()

    val tempFile = File.createTempFile("simple_benchmark", ".desc")
    Files.write(tempFile.toPath, descriptorSet.toByteArray)

    println(s"Created descriptor file: ${tempFile.getAbsolutePath}")
    tempFile
  }

  // Step 2: Generate the code
  def generateWireFormatCode(): Unit = {
    val descriptorFile = createSimpleDescriptorFile()
    val outputDir = new File("target/simple_wireformat_generated")
    outputDir.mkdirs()

    println(s"Generating WireFormat code to: ${outputDir.getAbsolutePath}")

    try {
      // Call the GenerateWireFormatTask with our descriptor
      fastproto.GenerateWireFormatTask.main(Array(descriptorFile.getAbsolutePath, outputDir.getAbsolutePath))

      println("\n" + "=" * 60)
      println("Generated files:")
      val files = Option(outputDir.listFiles()).getOrElse(Array.empty)
      files.foreach { file =>
        println(s"  ${file.getName}")
      }

      // Show the main generated Java file
      val javaFiles = files.filter(_.getName.endsWith(".java"))
      if (javaFiles.nonEmpty) {
        val mainJavaFile = javaFiles.head
        println(s"\n" + "=" * 60)
        println(s"Generated Java code in ${mainJavaFile.getName}:")
        println("=" * 60)
        val content = scala.io.Source.fromFile(mainJavaFile).mkString
        val maxLength = 5000
        println(content.take(maxLength))
        if (content.length > maxLength) {
          println(s"\n... (showing first $maxLength characters of ${content.length} total)")
          println(s"Full file: ${mainJavaFile.getAbsolutePath}")
        }

        // Also show the info file
        val infoFiles = files.filter(_.getName.endsWith("_info.txt"))
        if (infoFiles.nonEmpty) {
          val infoFile = infoFiles.head
          println(s"\n" + "=" * 60)
          println(s"Generated info from ${infoFile.getName}:")
          println("=" * 60)
          val infoContent = scala.io.Source.fromFile(infoFile).mkString
          println(infoContent.take(2000))
        }
      }

    } catch {
      case e: Exception =>
        println(s"Error generating code: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      descriptorFile.delete() // Clean up temp file
    }
  }
}