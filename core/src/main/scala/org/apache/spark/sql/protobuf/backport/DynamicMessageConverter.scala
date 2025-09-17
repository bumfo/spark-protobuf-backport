package org.apache.spark.sql.protobuf.backport

import com.google.protobuf.{Descriptors, DynamicMessage}
import fastproto.RowConverter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter
import org.apache.spark.sql.protobuf.backport.shims.QueryCompilationErrors
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

/**
 * A RowConverter implementation that uses the original DynamicMessage deserialization path.
 * This converter wraps the existing ProtobufDeserializer logic to provide a consistent
 * interface with other converter implementations while maintaining compatibility with
 * the traditional Spark deserialization approach.
 *
 * This converter uses SpecificInternalRow internally and does not support the nested
 * buffer sharing pattern used by wire format converters. When nested buffer sharing
 * is required, it throws UnsupportedOperationException.
 *
 * @param messageDescriptor The protobuf message descriptor for parsing
 * @param schema            The Catalyst data type that matches the expected schema
 */
class DynamicMessageConverter(
    messageDescriptor: Descriptors.Descriptor,
    val schema: StructType) extends RowConverter {

  @transient private lazy val fieldsNumbers0 =
    messageDescriptor.getFields.asScala.map(f => f.getNumber).toSet

  private val deserializer = new ProtobufDeserializer(messageDescriptor, schema)

  override def convert(binary: Array[Byte]): InternalRow = {
    // Parse using DynamicMessage
    val message = DynamicMessage.parseFrom(messageDescriptor, binary)

    // Check for unknown fields that clash with known field numbers; this indicates
    // a mismatch between writer and reader schemas.  Use findFieldByNumber
    // instead of indexing into getFields by number, because Protobuf field
    // numbers are 1‑based and may not align with the list index.
    message.getUnknownFields.asMap().keySet().asScala.find(fieldsNumbers0.contains(_)) match {
      case Some(number) =>
        val conflictingField = Option(messageDescriptor.findFieldByNumber(number))
          .getOrElse(messageDescriptor.getFields.get(number - 1))
        throw QueryCompilationErrors.protobufFieldTypeMismatchError(conflictingField.toString)
      case None => // no clash
    }

    // Deserialize to InternalRow
    val deserialized = deserializer.deserialize(message)
    require(
      deserialized.isDefined,
      "Protobuf deserializer cannot return an empty result because filters are not pushed down")

    // Convert any java.lang.String values in the deserialized result into UTF8String.
    // This avoids ClassCastException when Spark expects UTF8String for StringType fields.
    val rawValue = deserialized.get
    convertToInternalTypes(rawValue, schema).asInstanceOf[InternalRow]
  }


  /**
   * Convert String values to UTF8String recursively throughout the data structure.
   * This ensures compatibility with Spark's internal string representation.
   */
  private def convertToInternalTypes(value: Any, dt: DataType): Any = (value, dt) match {
    case (null, _) => null
    case (s: String, _: org.apache.spark.sql.types.StringType) => UTF8String.fromString(s)
    case (arr: scala.collection.Seq[_], at: org.apache.spark.sql.types.ArrayType) =>
      // Convert each element according to the element type
      arr.map(elem => convertToInternalTypes(elem, at.elementType))
    case (row: org.apache.spark.sql.catalyst.InternalRow, st: org.apache.spark.sql.types.StructType) =>
      val newValues = st.fields.zipWithIndex.map { case (field, idx) =>
        convertToInternalTypes(row.get(idx, field.dataType), field.dataType)
      }
      org.apache.spark.sql.catalyst.InternalRow.fromSeq(newValues)
    case (m: Map[_, _], mt: org.apache.spark.sql.types.MapType) =>
      m.map { case (k, v) =>
        convertToInternalTypes(k, mt.keyType) -> convertToInternalTypes(v, mt.valueType)
      }
    case _ => value
  }
}