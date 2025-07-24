/*
 * Backport of Spark 3.4's ProtobufSerializer to Spark 3.2.1.
 *
 * Converts Catalyst rows into Protobuf DynamicMessage instances based on
 * descriptor metadata.  Handles primitive types, arrays, maps, structs,
 * enums, timestamps and intervals.  See the upstream Spark implementation
 * for additional context.
 */

package org.apache.spark.sql.protobuf.backport

import scala.collection.JavaConverters._

import com.google.protobuf.{Duration, DynamicMessage, Timestamp}
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.catalyst.util.IntervalStringStyles.ANSI_STYLE
import org.apache.spark.sql.types._

import org.apache.spark.sql.protobuf.backport.utils.ProtobufUtils
import org.apache.spark.sql.protobuf.backport.utils.ProtobufUtils.{toFieldStr, ProtoMatchedField}
import org.apache.spark.sql.protobuf.backport.shims.QueryCompilationErrors

/**
 * A serializer to serialize data in Catalyst format to data in Protobuf
 * format.  This backport adapts Spark 3.4's implementation to work with
 * Spark 3.2.x.
 *
 * @param rootCatalystType The Catalyst type describing the root message.
 * @param rootDescriptor   The Protobuf descriptor for the root message.
 * @param nullable         Whether the input value may be null.
 */
private[backport] class ProtobufSerializer(
    rootCatalystType: DataType,
    rootDescriptor: Descriptor,
    nullable: Boolean)
    extends Logging {

  /** Serialize a Catalyst value into a Protobuf DynamicMessage or null. */
  def serialize(catalystData: Any): Any = {
    converter.apply(catalystData)
  }

  // Build the top level converter for the root Catalyst type.  The converter
  // returns a DynamicMessage or null.  If the root field is nullable we
  // wrap the converter in a null check.
  private val converter: Any => Any = {
    val baseConverter = try {
      rootCatalystType match {
        case st: StructType =>
          newStructConverter(st, rootDescriptor, Nil, Nil).asInstanceOf[Any => Any]
      }
    } catch {
      case ise: AnalysisException =>
        throw QueryCompilationErrors.cannotConvertSqlTypeToProtobufError(
          rootDescriptor.getName,
          rootCatalystType,
          ise)
    }
    if (nullable) {
      (data: Any) => if (data == null) null else baseConverter.apply(data)
    } else {
      baseConverter
    }
  }

  // Type alias for nested converters used within structs.
  private type Converter = (SpecializedGetters, Int) => Any

  // Build a converter for a specific field.  The converter reads the field
  // value from a Catalyst row (via SpecializedGetters) and constructs the
  // corresponding Protobuf representation.
  private def newConverter(
      catalystType: DataType,
      fieldDescriptor: FieldDescriptor,
      catalystPath: Seq[String],
      protoPath: Seq[String]): Converter = {
    (catalystType, fieldDescriptor.getJavaType) match {
      case (NullType, _) => (getter, ordinal) => null
      case (BooleanType, BOOLEAN) => (getter, ordinal) => getter.getBoolean(ordinal)
      case (ByteType, INT) => (getter, ordinal) => getter.getByte(ordinal).toInt
      case (ShortType, INT) => (getter, ordinal) => getter.getShort(ordinal).toInt
      case (IntegerType, INT) => (getter, ordinal) => getter.getInt(ordinal)
      case (LongType, LONG) => (getter, ordinal) => getter.getLong(ordinal)
      case (FloatType, FLOAT) => (getter, ordinal) => getter.getFloat(ordinal)
      case (DoubleType, DOUBLE) => (getter, ordinal) => getter.getDouble(ordinal)
      case (StringType, ENUM) =>
        val enumSymbols: Set[String] = fieldDescriptor.getEnumType.getValues.asScala.map(_.toString).toSet
        (getter, ordinal) => {
          val data = getter.getUTF8String(ordinal).toString
          if (!enumSymbols.contains(data)) {
            throw QueryCompilationErrors.cannotConvertCatalystTypeToProtobufEnumTypeError(
              catalystPath,
              toFieldStr(protoPath),
              data,
              enumSymbols.mkString("\"", "\", \"", "\""))
          }
          fieldDescriptor.getEnumType.findValueByName(data)
        }
      case (StringType, STRING) => (getter, ordinal) => String.valueOf(getter.getUTF8String(ordinal))
      case (BinaryType, BYTE_STRING) => (getter, ordinal) => getter.getBinary(ordinal)
      case (DateType, INT) => (getter, ordinal) => getter.getInt(ordinal)
      case (TimestampType, MESSAGE) => (getter, ordinal) => {
        val millis = DateTimeUtils.microsToMillis(getter.getLong(ordinal))
        Timestamp.newBuilder()
          .setSeconds((millis / 1000))
          .setNanos(((millis % 1000) * 1000000).toInt)
          .build()
      }
      case (ArrayType(et, containsNull), _) =>
        val elementConverter = newConverter(et, fieldDescriptor, catalystPath :+ "element", protoPath :+ "element")
        (getter, ordinal) => {
          val arrayData = getter.getArray(ordinal)
          val len = arrayData.numElements()
          val result = new Array[Any](len)
          var i = 0
          while (i < len) {
            if (containsNull && arrayData.isNullAt(i)) {
              result(i) = null
            } else {
              result(i) = elementConverter(arrayData, i)
            }
            i += 1
          }
          // Protobuf writer expects a Java Collection, so convert the array to a java.util.List
          java.util.Arrays.asList(result: _*)
        }
      case (st: StructType, MESSAGE) =>
        val structConverter = newStructConverter(st, fieldDescriptor.getMessageType, catalystPath, protoPath)
        val numFields = st.length
        (getter, ordinal) => structConverter(getter.getStruct(ordinal, numFields))
      case (MapType(kt, vt, valueContainsNull), MESSAGE) =>
        var keyField: FieldDescriptor = null
        var valueField: FieldDescriptor = null
        fieldDescriptor.getMessageType.getFields.asScala.foreach { field =>
          field.getName match {
            case "key" => keyField = field
            case "value" => valueField = field
            case _ =>
          }
        }
        val keyConverter = newConverter(kt, keyField, catalystPath :+ "key", protoPath :+ "key")
        val valueConverter = newConverter(vt, valueField, catalystPath :+ "value", protoPath :+ "value")
        (getter, ordinal) => {
          val mapData = getter.getMap(ordinal)
          val len = mapData.numElements()
          val list = new java.util.ArrayList[DynamicMessage]()
          val keyArray = mapData.keyArray()
          val valueArray = mapData.valueArray()
          var i = 0
          while (i < len) {
            val result = DynamicMessage.newBuilder(fieldDescriptor.getMessageType)
            if (valueContainsNull && valueArray.isNullAt(i)) {
              result.setField(keyField, keyConverter(keyArray, i))
              result.setField(valueField, valueField.getDefaultValue)
            } else {
              result.setField(keyField, keyConverter(keyArray, i))
              result.setField(valueField, valueConverter(valueArray, i))
            }
            list.add(result.build())
            i += 1
          }
          list
        }
      case (DayTimeIntervalType(startField, endField), MESSAGE) => (getter, ordinal) => {
        val dayTimeIntervalString = IntervalUtils.toDayTimeIntervalString(getter.getLong(ordinal), ANSI_STYLE, startField, endField)
        val calendarInterval = IntervalUtils.fromIntervalString(dayTimeIntervalString)
        val millis = DateTimeUtils.microsToMillis(calendarInterval.microseconds)
        val duration = Duration.newBuilder()
          .setSeconds((millis / 1000))
          .setNanos(((millis % 1000) * 1000000).toInt)
        // Normalize negative zero values
        if (duration.getSeconds < 0 && duration.getNanos > 0) {
          duration.setSeconds(duration.getSeconds + 1)
          duration.setNanos(duration.getNanos - 1000000000)
        } else if (duration.getSeconds > 0 && duration.getNanos < 0) {
          duration.setSeconds(duration.getSeconds - 1)
          duration.setNanos(duration.getNanos + 1000000000)
        }
        duration.build()
      }
      case _ =>
        throw QueryCompilationErrors.cannotConvertCatalystTypeToProtobufTypeError(
          catalystPath,
          toFieldStr(protoPath),
          catalystType,
          s"${fieldDescriptor} ${fieldDescriptor.toProto.getLabel} ${fieldDescriptor.getJavaType} ${fieldDescriptor.getType}")
    }
  }

  // Build a converter for a struct.  The converter takes an InternalRow and
  // constructs a DynamicMessage.  It validates that required fields are present.
  private def newStructConverter(
      catalystStruct: StructType,
      descriptor: Descriptor,
      catalystPath: Seq[String],
      protoPath: Seq[String]): InternalRow => DynamicMessage = {
    val protoSchemaHelper = new ProtobufUtils.ProtoSchemaHelper(descriptor, catalystStruct, protoPath, catalystPath)
    protoSchemaHelper.validateNoExtraCatalystFields(ignoreNullable = false)
    protoSchemaHelper.validateNoExtraRequiredProtoFields()
    val (protoIndices, fieldConverters: Array[Converter]) = protoSchemaHelper.matchedFields
      .map { case ProtoMatchedField(catalystField, _, protoField) =>
        val converter = newConverter(catalystField.dataType, protoField, catalystPath :+ catalystField.name, protoPath :+ protoField.getName)
        (protoField, converter)
      }
      .toArray
      .unzip
    val numFields = catalystStruct.length
    row: InternalRow => {
      val result = DynamicMessage.newBuilder(descriptor)
      var i = 0
      while (i < numFields) {
        if (row.isNullAt(i)) {
          if (!protoIndices(i).isRepeated && protoIndices(i).getJavaType != FieldDescriptor.JavaType.MESSAGE && protoIndices(i).isRequired) {
            // Required primitive fields must be set to their default value.
            result.setField(protoIndices(i), protoIndices(i).getDefaultValue)
          }
        } else {
          result.setField(protoIndices(i), fieldConverters(i).apply(row, i))
        }
        i += 1
      }
      result.build()
    }
  }
}