/*
 * Backport of Spark 3.4's ProtobufDeserializer to Spark 3.2.1.
 *
 * This class converts a Protobuf message into a Catalyst InternalRow using
 * recursive writers for primitive, array, map and record types.  The
 * implementation mirrors that in Spark 3.4 but has been adapted to work
 * against Spark 3.2's APIs and ships as part of this backport.
 */

package org.apache.spark.sql.protobuf.backport

import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import com.google.protobuf.Descriptors._
import com.google.protobuf.{ByteString, DynamicMessage, Message}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.protobuf.backport.shims.{NoopFilters, QueryCompilationErrors, StructFilters}
import org.apache.spark.sql.protobuf.backport.utils.ProtobufUtils
import org.apache.spark.sql.protobuf.backport.utils.ProtobufUtils.{ProtoMatchedField, toFieldStr}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.util.concurrent.TimeUnit

/**
 * Internal helper that deserializes Protobuf messages into Catalyst rows.
 *
 * @param rootDescriptor   The root Protobuf descriptor describing the message.
 * @param rootCatalystType The root Catalyst type expected for the message.
 * @param filters          Predicate pushdown filters; currently unused and
 *                         provided for API completeness.
 */
private[backport] class ProtobufDeserializer(
    rootDescriptor: Descriptor,
    rootCatalystType: DataType,
    filters: StructFilters) {

  def this(rootDescriptor: Descriptor, rootCatalystType: DataType) =
    this(rootDescriptor, rootCatalystType, new NoopFilters)

  // Top level converter that produces an InternalRow or None if the row should be skipped.
  private val converter: Any => Option[InternalRow] = try {
    rootCatalystType match {
      case st: StructType if st.isEmpty => (_: Any) => Some(InternalRow.empty)
      case st: StructType =>
        val resultRow = new SpecificInternalRow(st.map(_.dataType))
        val fieldUpdater = new RowUpdater(resultRow)
        val applyFilters = filters.skipRow(resultRow, _)
        val writer = getRecordWriter(rootDescriptor, st, Nil, Nil, applyFilters)
        (data: Any) => {
          val record = data.asInstanceOf[DynamicMessage]
          val skipRow = writer(fieldUpdater, record)
          if (skipRow) None else Some(resultRow)
        }
    }
  } catch {
    case ise: AnalysisException =>
      throw QueryCompilationErrors.cannotConvertProtobufTypeToCatalystTypeError(
        rootDescriptor.getName,
        rootCatalystType,
        ise)
  }

  /** Entry point: deserialize a DynamicMessage into an InternalRow. */
  def deserialize(data: Message): Option[InternalRow] = converter(data)

  // Writer for repeated fields (arrays).  Creates an ArrayData and populates
  // it element by element using the element writer.
  private def newArrayWriter(
      protoField: FieldDescriptor,
      protoPath: Seq[String],
      catalystPath: Seq[String],
      elementType: DataType,
      containsNull: Boolean): (CatalystDataUpdater, Int, Any) => Unit = {
    val protoElementPath = protoPath :+ "element"
    val elementWriter = newWriter(protoField, elementType, protoElementPath, catalystPath :+ "element")
    (updater, ordinal, value) => {
      val collection = value.asInstanceOf[java.util.Collection[Any]]
      val result = createArrayData(elementType, collection.size())
      val elementUpdater = new ArrayDataUpdater(result)
      val iterator = collection.iterator()
      var i = 0
      while (iterator.hasNext) {
        val element = iterator.next()
        if (element == null) {
          if (!containsNull) {
            throw QueryCompilationErrors.notNullConstraintViolationArrayElementError(protoElementPath)
          } else {
            elementUpdater.setNullAt(i)
          }
        } else {
          elementWriter(elementUpdater, i, element)
        }
        i += 1
      }
      updater.set(ordinal, result)
    }
  }

  // Writer for map fields.  Protobuf map entries are represented as repeated
  // message fields with "key" and "value" subfields.  We produce two
  // parallel arrays for keys and values and wrap them in an ArrayBasedMapData.
  private def newMapWriter(
      protoType: FieldDescriptor,
      protoPath: Seq[String],
      catalystPath: Seq[String],
      keyType: DataType,
      valueType: DataType,
      valueContainsNull: Boolean): (CatalystDataUpdater, Int, Any) => Unit = {
    val keyField = protoType.getMessageType.getFields.get(0)
    val valueField = protoType.getMessageType.getFields.get(1)
    val keyWriter = newWriter(keyField, keyType, protoPath :+ "key", catalystPath :+ "key")
    val valueWriter = newWriter(valueField, valueType, protoPath :+ "value", catalystPath :+ "value")
    (updater, ordinal, value) => {
      if (value != null) {
        val messageList = value.asInstanceOf[java.util.List[com.google.protobuf.Message]]
        val valueArray = createArrayData(valueType, messageList.size())
        val valueUpdater = new ArrayDataUpdater(valueArray)
        val keyArray = createArrayData(keyType, messageList.size())
        val keyUpdater = new ArrayDataUpdater(keyArray)
        var i = 0
        val iter = messageList.iterator()
        while (iter.hasNext) {
          val field = iter.next()
          keyWriter(keyUpdater, i, field.getField(keyField))
          if (field.getField(valueField) == null) {
            if (!valueContainsNull) {
              throw QueryCompilationErrors.notNullConstraintViolationMapValueError(protoPath)
            } else {
              valueUpdater.setNullAt(i)
            }
          } else {
            valueWriter(valueUpdater, i, field.getField(valueField))
          }
          i += 1
        }
        updater.set(ordinal, new ArrayBasedMapData(keyArray, valueArray))
      }
    }
  }

  /**
   * Creates a writer to write Protobuf values to Catalyst values at the given
   * ordinal with the given updater.
   */
  private def newWriter(
      protoType: FieldDescriptor,
      catalystType: DataType,
      protoPath: Seq[String],
      catalystPath: Seq[String]): (CatalystDataUpdater, Int, Any) => Unit = {
    (protoType.getJavaType, catalystType) match {
      case (null, NullType) => (updater, ordinal, _) => updater.setNullAt(ordinal)
      // Primitive types
      case (BOOLEAN, BooleanType) => (updater, ordinal, value) => updater.setBoolean(ordinal, value.asInstanceOf[Boolean])
      case (INT, IntegerType) => (updater, ordinal, value) => updater.setInt(ordinal, value.asInstanceOf[Int])
      case (INT, ByteType) => (updater, ordinal, value) => updater.setByte(ordinal, value.asInstanceOf[Byte])
      case (INT, ShortType) => (updater, ordinal, value) => updater.setShort(ordinal, value.asInstanceOf[Short])
      case (LONG, LongType) => (updater, ordinal, value) => updater.setLong(ordinal, value.asInstanceOf[Long])
      case (FLOAT, FloatType) => (updater, ordinal, value) => updater.setFloat(ordinal, value.asInstanceOf[Float])
      case (DOUBLE, DoubleType) => (updater, ordinal, value) => updater.setDouble(ordinal, value.asInstanceOf[Double])
      case (STRING, StringType) => (updater, ordinal, value) => {
        val str = value match {
          case s: String => UTF8String.fromString(s)
        }
        updater.set(ordinal, str)
      }
      case (BYTE_STRING, BinaryType) => (updater, ordinal, value) => {
        val byteArray = value match {
          case s: ByteString => s.toByteArray
          case unsupported => throw QueryCompilationErrors.invalidByteStringFormatError(unsupported)
        }
        updater.set(ordinal, byteArray)
      }
      // Arrays
      case (MESSAGE | BOOLEAN | INT | FLOAT | DOUBLE | LONG | STRING | ENUM | BYTE_STRING, ArrayType(dataType, containsNull)) if protoType.isRepeated =>
        newArrayWriter(protoType, protoPath, catalystPath, dataType, containsNull)
      // Maps
      case (MESSAGE, MapType(keyType, valueType, valueContainsNull)) =>
        newMapWriter(protoType, protoPath, catalystPath, keyType, valueType, valueContainsNull)
      // Timestamp fields
      case (MESSAGE, TimestampType) => (updater, ordinal, value) => {
        val secondsField = protoType.getMessageType.getFields.get(0)
        val nanoSecondsField = protoType.getMessageType.getFields.get(1)
        val message = value.asInstanceOf[DynamicMessage]
        val seconds = message.getField(secondsField).asInstanceOf[Long]
        val nanoSeconds = message.getField(nanoSecondsField).asInstanceOf[Int]
        val micros = DateTimeUtils.millisToMicros(seconds * 1000)
        updater.setLong(ordinal, micros + TimeUnit.NANOSECONDS.toMicros(nanoSeconds))
      }
      // DayTimeInterval fields
      case (MESSAGE, DayTimeIntervalType(startField, endField)) => (updater, ordinal, value) => {
        val secondsField = protoType.getMessageType.getFields.get(0)
        val nanoSecondsField = protoType.getMessageType.getFields.get(1)
        val message = value.asInstanceOf[DynamicMessage]
        val seconds = message.getField(secondsField).asInstanceOf[Long]
        val nanoSeconds = message.getField(nanoSecondsField).asInstanceOf[Int]
        val micros = DateTimeUtils.millisToMicros(seconds * 1000)
        updater.setLong(ordinal, micros + TimeUnit.NANOSECONDS.toMicros(nanoSeconds))
      }
      // Structs
      case (MESSAGE, st: StructType) =>
        val writeRecord = getRecordWriter(
          protoType.getMessageType,
          st,
          protoPath,
          catalystPath,
          applyFilters = _ => false)
        (updater, ordinal, value) => {
          val row = new SpecificInternalRow(st)
          writeRecord(new RowUpdater(row), value.asInstanceOf[DynamicMessage])
          updater.set(ordinal, row)
        }
      // Enums represented as Strings
      case (ENUM, StringType) => (updater, ordinal, value) => updater.set(ordinal, UTF8String.fromString(value.toString))
      case _ =>
        throw QueryCompilationErrors.cannotConvertProtobufTypeToSqlTypeError(
          toFieldStr(protoPath),
          catalystPath,
          s"${protoType} ${protoType.toProto.getLabel} ${protoType.getJavaType} ${protoType.getType}",
          catalystType)
    }
  }

  // Build a writer for a record (struct) type.  Each field writer writes a
  // single field from the Protobuf message into the Catalyst row.  Filters
  // may skip rows based on predicate pushdown.
  private def getRecordWriter(
      protoType: Descriptor,
      catalystType: StructType,
      protoPath: Seq[String],
      catalystPath: Seq[String],
      applyFilters: Int => Boolean): (CatalystDataUpdater, DynamicMessage) => Boolean = {
    val protoSchemaHelper = new ProtobufUtils.ProtoSchemaHelper(protoType, catalystType, protoPath, catalystPath)
    // TODO revisit validation of protobuf‑catalyst fields. See Spark 3.4 implementation.
    // protoSchemaHelper.validateNoExtraCatalystFields(ignoreNullable = true)
    var i = 0
    val (validFieldIndexes, fieldWriters) = protoSchemaHelper.matchedFields
      .map { case ProtoMatchedField(catalystField, ordinal, protoField) =>
        val baseWriter = newWriter(
          protoField,
          catalystField.dataType,
          protoPath :+ protoField.getName,
          catalystPath :+ catalystField.name)
        val fieldWriter = (fieldUpdater: CatalystDataUpdater, value: Any) => {
          if (value == null) {
            fieldUpdater.setNullAt(ordinal)
          } else {
            baseWriter(fieldUpdater, ordinal, value)
          }
        }
        i += 1
        (protoField, fieldWriter)
      }
      .toArray
      .unzip
    (fieldUpdater, record) => {
      var i = 0
      var skipRow = false
      while (i < validFieldIndexes.length && !skipRow) {
        val field = validFieldIndexes(i)
        val value = if (field.isRepeated || field.hasDefaultValue || record.hasField(field)) {
          record.getField(field)
        } else null
        fieldWriters(i)(fieldUpdater, value)
        skipRow = applyFilters(i)
        i += 1
      }
      skipRow
    }
  }

  // Create an ArrayData of the given Catalyst element type and length.  Uses
  // specialized unsafe arrays for primitive types to avoid boxing.
  private def createArrayData(elementType: DataType, length: Int): ArrayData = elementType match {
    case BooleanType => UnsafeArrayData.fromPrimitiveArray(new Array[Boolean](length))
    case ByteType => UnsafeArrayData.fromPrimitiveArray(new Array[Byte](length))
    case ShortType => UnsafeArrayData.fromPrimitiveArray(new Array[Short](length))
    case IntegerType => UnsafeArrayData.fromPrimitiveArray(new Array[Int](length))
    case LongType => UnsafeArrayData.fromPrimitiveArray(new Array[Long](length))
    case FloatType => UnsafeArrayData.fromPrimitiveArray(new Array[Float](length))
    case DoubleType => UnsafeArrayData.fromPrimitiveArray(new Array[Double](length))
    case _ => new GenericArrayData(new Array[Any](length))
  }

  /**
   * Base interface for updating values inside Catalyst data structures like
   * `InternalRow` and `ArrayData`.  Provides specialized methods for
   * primitive types for performance.
   */
  private sealed trait CatalystDataUpdater {
    def set(ordinal: Int, value: Any): Unit

    def setNullAt(ordinal: Int): Unit = set(ordinal, null)

    def setBoolean(ordinal: Int, value: Boolean): Unit = set(ordinal, value)

    def setByte(ordinal: Int, value: Byte): Unit = set(ordinal, value)

    def setShort(ordinal: Int, value: Short): Unit = set(ordinal, value)

    def setInt(ordinal: Int, value: Int): Unit = set(ordinal, value)

    def setLong(ordinal: Int, value: Long): Unit = set(ordinal, value)

    def setDouble(ordinal: Int, value: Double): Unit = set(ordinal, value)

    def setFloat(ordinal: Int, value: Float): Unit = set(ordinal, value)

    def setDecimal(ordinal: Int, value: Decimal): Unit = set(ordinal, value)
  }

  // CatalystDataUpdater implementation for rows.
  private final class RowUpdater(row: InternalRow) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = row.update(ordinal, value)

    override def setNullAt(ordinal: Int): Unit = row.setNullAt(ordinal)

    override def setBoolean(ordinal: Int, value: Boolean): Unit = row.setBoolean(ordinal, value)

    override def setByte(ordinal: Int, value: Byte): Unit = row.setByte(ordinal, value)

    override def setShort(ordinal: Int, value: Short): Unit = row.setShort(ordinal, value)

    override def setInt(ordinal: Int, value: Int): Unit = row.setInt(ordinal, value)

    override def setLong(ordinal: Int, value: Long): Unit = row.setLong(ordinal, value)

    override def setDouble(ordinal: Int, value: Double): Unit = row.setDouble(ordinal, value)

    override def setFloat(ordinal: Int, value: Float): Unit = row.setFloat(ordinal, value)

    override def setDecimal(ordinal: Int, value: Decimal): Unit = row.setDecimal(ordinal, value, value.precision)
  }

  // CatalystDataUpdater implementation for arrays.
  private final class ArrayDataUpdater(array: ArrayData) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = array.update(ordinal, value)

    override def setNullAt(ordinal: Int): Unit = array.setNullAt(ordinal)

    override def setBoolean(ordinal: Int, value: Boolean): Unit = array.setBoolean(ordinal, value)

    override def setByte(ordinal: Int, value: Byte): Unit = array.setByte(ordinal, value)

    override def setShort(ordinal: Int, value: Short): Unit = array.setShort(ordinal, value)

    override def setInt(ordinal: Int, value: Int): Unit = array.setInt(ordinal, value)

    override def setLong(ordinal: Int, value: Long): Unit = array.setLong(ordinal, value)

    override def setDouble(ordinal: Int, value: Double): Unit = array.setDouble(ordinal, value)

    override def setFloat(ordinal: Int, value: Float): Unit = array.setFloat(ordinal, value)

    override def setDecimal(ordinal: Int, value: Decimal): Unit = array.update(ordinal, value)
  }
}