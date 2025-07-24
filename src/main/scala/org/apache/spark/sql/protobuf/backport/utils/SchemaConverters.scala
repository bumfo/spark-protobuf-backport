/*
 * Backport of Spark 3.4's SchemaConverters to Spark 3.2.1.
 *
 * Provides conversion from Protobuf descriptors to Spark SQL schemas,
 * including handling of recursive message types, maps, arrays, timestamps
 * and special well‑known Protobuf types.  This implementation mirrors the
 * Spark 3.4 code while adjusting references for Spark 3.2.x compatibility.
 */

package org.apache.spark.sql.protobuf.backport.utils

import scala.collection.JavaConverters._

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

import org.apache.spark.sql.protobuf.backport.shims.QueryCompilationErrors

/**
 * A collection of helper methods to convert Protobuf schemas to Spark SQL schemas.
 */
@DeveloperApi
object SchemaConverters extends Logging {

  /** Internal wrapper for SQL data type and nullability. */
  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
   * Converts a Protobuf schema to a corresponding Spark SQL schema.
   *
   * @param descriptor       the Protobuf descriptor
   * @param protobufOptions  options controlling recursion depth
   */
  def toSqlType(
      descriptor: Descriptor,
      protobufOptions: ProtobufOptions = ProtobufOptions(Map.empty)): SchemaType = {
    toSqlTypeHelper(descriptor, protobufOptions)
  }

  private def toSqlTypeHelper(
      descriptor: Descriptor,
      protobufOptions: ProtobufOptions): SchemaType = {
    SchemaType(
      StructType(descriptor.getFields.asScala.flatMap(structFieldFor(_, Map(descriptor.getFullName -> 1), protobufOptions)).toArray),
      nullable = true)
  }

  /**
   * existingRecordNames: Map[String, Int] used to track the depth of recursive fields and to ensure that the
   * conversion of the Protobuf message to a Spark SQL StructType object does not exceed the maximum
   * recursive depth specified by the recursiveFieldMaxDepth option.  A return of None implies the
   * field has reached the maximum allowed recursive depth and should be dropped.
   */
  def structFieldFor(
      fd: FieldDescriptor,
      existingRecordNames: Map[String, Int],
      protobufOptions: ProtobufOptions): Option[StructField] = {
    import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
    val dataType: Option[DataType] = fd.getJavaType match {
      case INT => Some(IntegerType)
      case LONG => Some(LongType)
      case FLOAT => Some(FloatType)
      case DOUBLE => Some(DoubleType)
      case BOOLEAN => Some(BooleanType)
      case STRING => Some(StringType)
      case BYTE_STRING => Some(BinaryType)
      case ENUM => Some(StringType)
      case MESSAGE if (fd.getMessageType.getName == "Duration" && fd.getMessageType.getFields.size() == 2 &&
        fd.getMessageType.getFields.get(0).getName.equals("seconds") &&
        fd.getMessageType.getFields.get(1).getName.equals("nanos")) =>
        // In Spark 3.2.x there is no defaultConcreteType method on DayTimeIntervalType.  Use the
        // canonical DAY to SECOND interval type directly by constructing a new instance with
        // appropriate start and end fields.  The numeric constants DAY and SECOND are defined on
        // DayTimeIntervalType companion object.
        Some(DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.SECOND))
      case MESSAGE if (fd.getMessageType.getName == "Timestamp" && fd.getMessageType.getFields.size() == 2 && fd.getMessageType.getFields.get(0).getName.equals("seconds") && fd.getMessageType.getFields.get(1).getName.equals("nanos")) =>
        Some(TimestampType)
      case MESSAGE if fd.isRepeated && fd.getMessageType.getOptions.hasMapEntry =>
        var keyType: Option[DataType] = None
        var valueType: Option[DataType] = None
        fd.getMessageType.getFields.forEach { field =>
          field.getName match {
            case "key" =>
              keyType = structFieldFor(field, existingRecordNames, protobufOptions).map(_.dataType)
            case "value" =>
              valueType = structFieldFor(field, existingRecordNames, protobufOptions).map(_.dataType)
            case _ =>
          }
        }
        (keyType, valueType) match {
          case (None, _) =>
            log.info(s"Dropping map field ${fd.getFullName}. Key reached max recursive depth.")
            None
          case (_, None) =>
            log.info(s"Dropping map field ${fd.getFullName}. Value reached max recursive depth.")
            None
          case (Some(kt), Some(vt)) => Some(MapType(kt, vt, valueContainsNull = false))
        }
      case MESSAGE =>
        val recordName = fd.getMessageType.getFullName
        val recursiveDepth = existingRecordNames.getOrElse(recordName, 0)
        val recursiveFieldMaxDepth = protobufOptions.recursiveFieldMaxDepth
        if (existingRecordNames.contains(recordName) && (recursiveFieldMaxDepth <= 0 || recursiveFieldMaxDepth > 10)) {
          throw QueryCompilationErrors.foundRecursionInProtobufSchema(fd.toString)
        } else if (existingRecordNames.contains(recordName) && recursiveDepth >= recursiveFieldMaxDepth) {
          log.info(s"The field ${fd.getFullName} of type $recordName is dropped at recursive depth $recursiveDepth")
          None
        } else {
          val newRecordNames = existingRecordNames + (recordName -> (recursiveDepth + 1))
          val fields = fd.getMessageType.getFields.asScala.flatMap(structFieldFor(_, newRecordNames, protobufOptions)).toSeq
          fields match {
            case Nil =>
              log.info(s"Dropping ${fd.getFullName} as it does not have any fields left likely due to recursive depth limit.")
              None
            case fds => Some(StructType(fds))
          }
        }
      case other =>
        throw QueryCompilationErrors.protobufTypeUnsupportedYetError(other.toString)
    }
    dataType.map {
      case dt: MapType => StructField(fd.getName, dt)
      case dt if fd.isRepeated => StructField(fd.getName, ArrayType(dt, containsNull = false))
      case dt => StructField(fd.getName, dt, nullable = !fd.isRequired)
    }
  }
}