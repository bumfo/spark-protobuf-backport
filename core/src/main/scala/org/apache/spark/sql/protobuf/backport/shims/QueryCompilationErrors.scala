/*
 * Shim providing error helpers missing in Spark 3.2.x.
 *
 * In Spark 3.4 the protobuf implementation throws various structured
 * `QueryCompilationErrors` and `QueryExecutionErrors` defined in
 * `org.apache.spark.sql.errors`.  These classes are not present in
 * Spark 3.2.x, so this shim implements a subset of the API used by the
 * Protobuf backport.  All methods produce an [[AnalysisException]]
 * carrying an explanatory message.  Where a cause is provided by the
 * caller it will be attached to the exception.
 */

package org.apache.spark.sql.protobuf.backport.shims

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.ParseMode
import org.apache.spark.sql.types.DataType

/**
 * A collection of helper methods used by the backported Protobuf code to
 * throw compilation‑time errors.  All methods return an AnalysisException.
 */
object QueryCompilationErrors {

  private def error(msg: String, cause: Throwable = null): AnalysisException = {
    if (cause != null) new AnalysisException(msg, cause = Some(cause))
    else new AnalysisException(msg)
  }

  def cannotConvertProtobufTypeToCatalystTypeError(name: String, t: DataType, cause: Throwable): AnalysisException =
    error(s"Cannot convert Protobuf type '$name' to Catalyst type ${t.simpleString}", cause)

  def notNullConstraintViolationArrayElementError(path: Seq[String]): AnalysisException =
    error(s"Null element encountered in non‑nullable array at ${path.mkString(".")}")

  def notNullConstraintViolationMapValueError(path: Seq[String]): AnalysisException =
    error(s"Null map value encountered in non‑nullable map at ${path.mkString(".")}")

  def invalidByteStringFormatError(value: Any): AnalysisException =
    error(s"Invalid ByteString value: $value")

  def cannotConvertProtobufTypeToSqlTypeError(protoPath: String, catalystPath: Seq[String], protoType: String, catalystType: DataType): AnalysisException =
    error(s"Cannot convert Protobuf type ($protoType) at $protoPath to Catalyst type '${catalystType.simpleString}' for field ${catalystPath.mkString(".")}")

  def cannotConvertSqlTypeToProtobufError(name: String, dataType: DataType, cause: Throwable): AnalysisException =
    error(s"Cannot convert SQL type '${dataType.simpleString}' to Protobuf message '$name'", cause)

  def cannotConvertCatalystTypeToProtobufEnumTypeError(catalystPath: Seq[String], protoPath: String, data: String, enumSymbols: String): AnalysisException =
    error(s"Value '$data' at ${catalystPath.mkString(".")} is not one of the allowed enum symbols $enumSymbols for field $protoPath")

  def cannotConvertCatalystTypeToProtobufTypeError(catalystPath: Seq[String], protoPath: String, catalystType: DataType, detail: String): AnalysisException =
    error(s"Cannot convert Catalyst type '${catalystType.simpleString}' at ${catalystPath.mkString(".")} to Protobuf field $protoPath: $detail")

  def unknownProtobufMessageTypeError(name: String, parentName: String): AnalysisException =
    error(s"Unknown Protobuf message type: $name (parent: $parentName)")

  def cannotFindCatalystTypeInProtobufSchemaError(field: String): AnalysisException =
    error(s"Cannot find Catalyst field $field in Protobuf schema")

  def cannotFindProtobufFieldInCatalystError(field: String): AnalysisException =
    error(s"Cannot find Protobuf field $field in Catalyst schema")

  def protobufFieldMatchError(name: String, context: String, count: String, matches: String): AnalysisException =
    error(s"Multiple matching fields for '$name' in $context (found $count matches: $matches)")

  def protobufClassLoadError(className: String, explanation: String, cause: Throwable = null): AnalysisException =
    error(s"Error loading Protobuf class '$className': $explanation", cause)

  def unableToLocateProtobufMessageError(messageName: String): AnalysisException =
    error(s"Unable to locate Protobuf message '$messageName' in descriptor file")

  def descriptorParseError(file: String, cause: Throwable): AnalysisException =
    error(s"Failed to parse Protobuf descriptor file: $file", cause)

  def cannotFindDescriptorFileError(file: String, cause: Throwable): AnalysisException =
    error(s"Could not read Protobuf descriptor file: $file", cause)

  def failedParsingDescriptorError(file: String, cause: Throwable): AnalysisException =
    error(s"Failed parsing Protobuf descriptor file: $file", cause)

  def protobufDescriptorDependencyError(dependency: String): AnalysisException =
    error(s"Missing dependency '$dependency' while resolving Protobuf descriptor dependencies")

  def parseModeUnsupportedError(functionName: String, mode: ParseMode): AnalysisException =
    error(s"Unsupported parse mode '${mode.name}' for function $functionName")

  def protobufFieldTypeMismatchError(detail: String): AnalysisException =
    error(s"Field type mismatch while reading Protobuf message: $detail")

  def foundRecursionInProtobufSchema(fieldDesc: String): AnalysisException =
    error(s"Found recursion in Protobuf schema while processing $fieldDesc")

  def protobufTypeUnsupportedYetError(name: String): AnalysisException =
    error(s"Protobuf type '$name' is not supported yet")
}