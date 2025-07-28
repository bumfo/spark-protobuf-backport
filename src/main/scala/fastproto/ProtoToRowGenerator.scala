package fastproto

import scala.collection.JavaConverters._

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.Message

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow

/**
 * Factory object for generating [[RowConverter]] instances using simple reflection.
 * Given a Protobuf [[Descriptor]] and the corresponding compiled message class,
 * this object builds a Spark SQL schema and a converter that extracts each
 * primitive field from the message using its accessor methods.  Nested messages
 * and repeated fields are not fully expanded and are emitted as `null`
 * placeholders.  This approach avoids using DynamicMessage and instead relies
 * on the compiled Java class.
 */
object ProtoToRowGenerator {

  /**
   * Compute the accessor method suffix for a given field descriptor.  This
   * converts snake_case names into CamelCase, capitalising each segment.  For
   * example, a field named "source_context" yields "SourceContext".
   */
  private def accessorName(fd: FieldDescriptor): String = {
    val name = fd.getName
    name.split("_").iterator.map { part =>
      if (part.isEmpty) "" else part.substring(0, 1).toUpperCase + part.substring(1)
    }.mkString("")
  }

  /**
   * Convert a Protobuf field descriptor into a Spark SQL [[DataType]].
   * Nested messages are handled recursively.  Repeated fields become
   * [[ArrayType]] and Protobuf map entry types are translated into
   * [[MapType]].  Primitive wrapper types (e.g. IntValue) are treated
   * according to their contained primitive.
   */
  private def fieldToDataType(fd: FieldDescriptor): DataType = {
    import JavaType._
    // Handle Protobuf map entries: repeated message types with mapEntry option.
    if (fd.isRepeated && fd.getType == FieldDescriptor.Type.MESSAGE && fd.getMessageType.getOptions.hasMapEntry) {
      // Build a StructType for the map entry (with key and value fields) and wrap it in an ArrayType.
      val entryType = buildStructType(fd.getMessageType)
      ArrayType(entryType, containsNull = false)
    } else if (fd.isRepeated) {
      // Repeated (array) field
      val elementType = fd.getJavaType match {
        case INT     => IntegerType
        case LONG    => LongType
        case FLOAT   => FloatType
        case DOUBLE  => DoubleType
        case BOOLEAN => BooleanType
        case STRING  => StringType
        case BYTE_STRING => BinaryType
        case ENUM    => StringType
        case MESSAGE => buildStructType(fd.getMessageType)
      }
      ArrayType(elementType, containsNull = false)
    } else {
      fd.getJavaType match {
        case INT     => IntegerType
        case LONG    => LongType
        case FLOAT   => FloatType
        case DOUBLE  => DoubleType
        case BOOLEAN => BooleanType
        case STRING  => StringType
        case BYTE_STRING => BinaryType
        case ENUM    => StringType
        case MESSAGE => buildStructType(fd.getMessageType)
      }
    }
  }

  /**
   * Recursively build a Spark SQL [[StructType]] corresponding to the structure
   * of a Protobuf message.  Primitive fields are mapped to appropriate
   * Catalyst types; repeated fields become [[ArrayType]] and Protobuf map
   * entries become [[ArrayType]] of structs with key and value fields.  Nested
   * message types are converted into nested [[StructType]]s.
   */
  private def buildStructType(descriptor: Descriptor): StructType = {
    val fields = descriptor.getFields().asScala.map { fd =>
      val dt = fieldToDataType(fd)
      // Proto3 fields are optional by default; mark field nullable unless explicitly required
      val nullable = !fd.isRequired
      StructField(fd.getName, dt, nullable)
    }
    StructType(fields.toArray)
  }

  /**
   * Generate a simple [[RowConverter]] for the given Protobuf message type.
   *
   * The returned converter uses reflection to call the appropriate getter
   * method on the compiled message class for each field defined in the
   * descriptor.  Primitive types are returned as their underlying JVM types.
   * Nested messages and repeated fields are emitted as `null` to match the
   * behaviour of Spark's permissive reader.  Map fields are treated as
   * repeated message types with mapEntry options and also return `null`.
   *
   * @param descriptor the Protobuf descriptor describing the message schema
   * @param messageClass the compiled Protobuf Java class
   * @tparam T the concrete type of the message
   * @return a [[RowConverter]] capable of converting the message into an
   *         [[org.apache.spark.sql.catalyst.InternalRow]]
   */
  def generateConverter[T <: Message](descriptor: Descriptor, messageClass: Class[T]): RowConverter[T] = {
    val schema: StructType = buildStructType(descriptor)
    new RowConverter[T] {
      override def schema: StructType = schema
      override def convert(message: T): InternalRow = {
        val values: Seq[Any] = descriptor.getFields.asScala.map { fd =>
          try {
            val accessor = accessorName(fd)
            val value = if (fd.isRepeated) {
              // repeated fields: call getXList to return java.util.List; we don't support nested arrays yet
              val m = messageClass.getMethod(s"get${accessor}List")
              m.invoke(message)
            } else {
              val m = messageClass.getMethod(s"get${accessor}")
              m.invoke(message)
            }
            if (value == null) {
              null
            } else {
              fd.getJavaType match {
                case JavaType.INT     => value.asInstanceOf[java.lang.Integer].intValue()
                case JavaType.LONG    => value.asInstanceOf[java.lang.Long].longValue()
                case JavaType.FLOAT   => value.asInstanceOf[java.lang.Float].floatValue()
                case JavaType.DOUBLE  => value.asInstanceOf[java.lang.Double].doubleValue()
                case JavaType.BOOLEAN => value.asInstanceOf[java.lang.Boolean].booleanValue()
                case JavaType.STRING  => value.toString
                case JavaType.BYTE_STRING => value.asInstanceOf[com.google.protobuf.ByteString].toByteArray
                case JavaType.ENUM    => value.toString
                case JavaType.MESSAGE => null // nested messages not expanded
              }
            }
          } catch {
            case _: Throwable => null
          }
        }
        InternalRow.fromSeq(values)
      }
    }
  }
}
