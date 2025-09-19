package benchmark

import com.google.protobuf.Descriptors
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * Custom schema converter for benchmarks that handles recursive message types.
 *
 * Standard SchemaConverters.toSqlType() fails on recursive protobuf messages because
 * Spark SQL cannot represent infinite recursive types. This converter detects
 * recursive fields during traversal and mocks them as BinaryType to avoid
 * infinite type definitions.
 */
object RecursiveSchemaConverters {

  /**
   * Convert protobuf descriptor to Spark SQL type with recursion detection and mocking.
   *
   * When a recursive field is detected (e.g., DomNode.children referring back to DomNode),
   * it's replaced with BinaryType (or ArrayType(BinaryType) for repeated fields).
   * This allows the schema to be finite while still testing recursive parsing performance.
   *
   * @param descriptor the protobuf message descriptor
   * @return Spark SQL StructType with recursive fields mocked
   */
  def toSqlTypeWithRecursionMocking(descriptor: Descriptor): DataType = {
    val visitedTypes = mutable.Set[String]()
    convertMessageType(descriptor, visitedTypes)
  }

  /**
   * Convert a message type to StructType, tracking visited types to detect recursion.
   */
  private def convertMessageType(
      descriptor: Descriptor,
      visitedTypes: mutable.Set[String]): StructType = {

    // Mark this type as being visited to detect cycles
    val typeName = descriptor.getFullName
    val wasAlreadyVisited = visitedTypes.contains(typeName)
    visitedTypes += typeName

    val fields = descriptor.getFields.asScala.map { field =>
      val sparkType = convertFieldType(field, visitedTypes, wasAlreadyVisited)
      StructField(field.getName, sparkType, nullable = true)
    }.toSeq

    // Clean up: remove from visited set when done (for clean recursion detection)
    if (!wasAlreadyVisited) {
      visitedTypes -= typeName
    }

    StructType(fields)
  }

  /**
   * Convert a single field type, handling recursion detection.
   */
  private def convertFieldType(
      field: FieldDescriptor,
      visitedTypes: mutable.Set[String],
      parentWasVisited: Boolean): DataType = {

    val baseType = field.getJavaType match {
      case FieldDescriptor.JavaType.MESSAGE =>
        val messageDescriptor = field.getMessageType
        val messageTypeName = messageDescriptor.getFullName

        // Check for recursion: if we're already processing this type, mock it
        if (visitedTypes.contains(messageTypeName)) {
          // RECURSION DETECTED! Mock with BinaryType
          BinaryType
        } else {
          // Not recursive yet, continue normal conversion
          convertMessageType(messageDescriptor, visitedTypes.clone())
        }

      case FieldDescriptor.JavaType.INT =>
        IntegerType
      case FieldDescriptor.JavaType.LONG =>
        LongType
      case FieldDescriptor.JavaType.FLOAT =>
        FloatType
      case FieldDescriptor.JavaType.DOUBLE =>
        DoubleType
      case FieldDescriptor.JavaType.BOOLEAN =>
        BooleanType
      case FieldDescriptor.JavaType.STRING =>
        StringType
      case FieldDescriptor.JavaType.BYTE_STRING =>
        BinaryType
      case FieldDescriptor.JavaType.ENUM =>
        StringType // Represent enums as strings
    }

    // Handle repeated fields
    if (field.isRepeated) {
      // Handle map fields - for compatibility with WireFormatParser,
      // treat maps as ArrayType[StructType] rather than MapType
      if (field.isMapField) {
        val mapEntryDescriptor = field.getMessageType
        val keyField = mapEntryDescriptor.findFieldByName("key")
        val valueField = mapEntryDescriptor.findFieldByName("value")

        val keyType = convertFieldType(keyField, visitedTypes, parentWasVisited)
        val valueType = convertFieldType(valueField, visitedTypes, parentWasVisited)

        // Create a synthetic struct type for map entries to be compatible with parsers
        val mapEntryStruct = StructType(Seq(
          StructField("key", keyType, nullable = false),
          StructField("value", valueType, nullable = true)
        ))
        ArrayType(mapEntryStruct)
      } else {
        ArrayType(baseType)
      }
    } else {
      baseType
    }
  }

  /**
   * Get information about which fields were mocked due to recursion.
   * Useful for debugging and understanding schema transformations.
   */
  def getMockedFields(descriptor: Descriptor): Set[String] = {
    val visitedTypes = mutable.Set[String]()
    val mockedFields = mutable.Set[String]()
    collectMockedFields(descriptor, visitedTypes, mockedFields, "")
    mockedFields.toSet
  }

  private def collectMockedFields(
      descriptor: Descriptor,
      visitedTypes: mutable.Set[String],
      mockedFields: mutable.Set[String],
      pathPrefix: String): Unit = {

    val typeName = descriptor.getFullName
    visitedTypes += typeName

    descriptor.getFields.asScala.foreach { field =>
      val fieldPath = if (pathPrefix.isEmpty) field.getName else s"$pathPrefix.${field.getName}"

      if (field.getJavaType == FieldDescriptor.JavaType.MESSAGE) {
        val messageDescriptor = field.getMessageType
        val messageTypeName = messageDescriptor.getFullName

        if (visitedTypes.contains(messageTypeName)) {
          mockedFields += fieldPath
        } else {
          collectMockedFields(messageDescriptor, visitedTypes.clone(), mockedFields, fieldPath)
        }
      }
    }

    visitedTypes -= typeName
  }

  /**
   * Print schema information for debugging.
   */
  def printSchemaInfo(descriptor: Descriptor): Unit = {
    val schema = toSqlTypeWithRecursionMocking(descriptor)
    val mockedFields = getMockedFields(descriptor)

    println(s"Schema for ${descriptor.getFullName}:")
    println(schema.prettyJson)
    println(s"\nMocked recursive fields: ${mockedFields.mkString(", ")}")
  }
}