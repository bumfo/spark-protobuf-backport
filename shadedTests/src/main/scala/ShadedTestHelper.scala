import com.google.protobuf.DescriptorProtos
import shadedtest.ShadedTest.ShadedTestMessage

/**
 * Helper class for shaded integration tests.
 *
 * This class uses protobuf classes (com.google.protobuf.*) which will be shaded
 * to org.sparkproject.spark_protobuf.protobuf.* by the assembly process.
 *
 * The public interface uses only basic types (Array[Byte], Int, String) so test
 * code doesn't need to import protobuf classes directly.
 */
object ShadedTestHelper {

  /**
   * Create a test message with given parameters.
   * Returns serialized binary protobuf data.
   */
  def createTestMessage(id: Int, name: String, values: Seq[Int], nestedField: Option[String]): Array[Byte] = {
    val builder = ShadedTestMessage.newBuilder()
      .setId(id)
      .setName(name)

    values.foreach(builder.addValues)

    nestedField.foreach { field =>
      builder.setNested(
        ShadedTestMessage.NestedMessage.newBuilder().setField(field)
      )
    }

    builder.build().toByteArray
  }

  /**
   * Create binary descriptor set for ShadedTestMessage.
   * Returns serialized FileDescriptorSet.
   */
  def createDescriptorBytes(): Array[Byte] = {
    val message = ShadedTestMessage.getDefaultInstance
    val descriptor = message.getDescriptorForType

    DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(descriptor.getFile.toProto)
      .build()
      .toByteArray
  }

  /**
   * Get the message name for use in from_protobuf.
   */
  def getMessageName(): String = "ShadedTestMessage"

  /**
   * Verify that protobuf classes are shaded.
   * Returns (true, package_name) if shaded, (false, error_message) otherwise.
   *
   * Note: Constructs class names dynamically to prevent shading process from
   * rewriting the string literals.
   */
  def verifyShadedProtobuf(): (Boolean, String) = {
    try {
      // Check if shaded protobuf is available
      val shadedName = "org.sparkproject.spark_protobuf.protobuf." + "Message"
      val shadedClass = Class.forName(shadedName)
      val packageName = shadedClass.getPackage.getName

      // Verify unshaded protobuf is NOT available
      // Construct name dynamically to avoid shading rewriting the literal
      try {
        val unshadedName = Seq("com", "google", "protobuf", "Message").mkString(".")
        val unshadedClass = Class.forName(unshadedName)
        val location = unshadedClass.getProtectionDomain.getCodeSource.getLocation
        (false, s"Unshaded $unshadedName found at: $location")
      } catch {
        case _: ClassNotFoundException =>
          (true, s"✓ Shaded protobuf package: $packageName, unshaded classes excluded from classpath")
      }
    } catch {
      case _: ClassNotFoundException =>
        (false, "Shaded protobuf classes not found")
    }
  }
}
