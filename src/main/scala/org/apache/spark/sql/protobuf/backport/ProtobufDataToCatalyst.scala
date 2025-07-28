/*
 * Backport of Spark 3.4's ProtobufDataToCatalyst to Spark 3.2.1.
 *
 * Deserializes a Protobuf binary column into a Catalyst value (usually a
 * struct) based on a message descriptor defined either by a Java class
 * name or by a serialized descriptor file.  Supports permissive and
 * fail‑fast parse modes.
 */

package org.apache.spark.sql.protobuf.backport

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.google.protobuf.DynamicMessage

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.util.{FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

import org.apache.spark.sql.protobuf.backport.utils.{ProtobufOptions, ProtobufUtils, SchemaConverters}
import org.apache.spark.sql.protobuf.backport.shims.{QueryCompilationErrors, QueryExecutionErrors}

/**
 * A Catalyst expression that deserializes a Protobuf binary column into a
 * Catalyst value.  If parsing fails it either returns null (permissive mode)
 * or throws an exception (fail‑fast mode).
 *
 * @param child        The binary column to deserialize.
 * @param messageName  The fully qualified message name or Java class name.
 * @param descFilePath Optional path to a serialized descriptor file.  If
 *                     provided the descriptor will be loaded from the file;
 *                     otherwise `messageName` is treated as a Java class name.
 * @param options      Reader options; currently supports "mode" (permissive|failfast)
 *                     and "recursive.fields.max.depth".
 */
private[backport] case class ProtobufDataToCatalyst(
    child: Expression,
    messageName: String,
    descFilePath: Option[String] = None,
    options: Map[String, String] = Map.empty,
    /**
     * Optional binary descriptor set.  If defined, this descriptor will be used
     * to build the message descriptor instead of reading from a file on the
     * executors.  This allows the descriptor to be serialized with the
     * expression and avoids file availability issues.
     */
    binaryDescriptorSet: Option[Array[Byte]] = None)
    extends UnaryExpression
    with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override lazy val dataType: DataType =
    SchemaConverters.toSqlType(messageDescriptor, protobufOptions).dataType

  override def nullable: Boolean = true

  private lazy val protobufOptions = ProtobufOptions(options)

  @transient private lazy val messageDescriptor: com.google.protobuf.Descriptors.Descriptor =
    binaryDescriptorSet match {
      case Some(bytes) => ProtobufUtils.buildDescriptorFromBytes(bytes, messageName)
      case None => ProtobufUtils.buildDescriptor(messageName, descFilePath)
    }

  @transient private lazy val fieldsNumbers =
    messageDescriptor.getFields.asScala.map(f => f.getNumber).toSet

  @transient private lazy val deserializer = new ProtobufDeserializer(messageDescriptor, dataType)

  @transient private var result: DynamicMessage = _

  @transient private lazy val parseMode: ParseMode = {
    val mode = protobufOptions.parseMode
    if (mode != PermissiveMode && mode != FailFastMode) {
      throw QueryCompilationErrors.parseModeUnsupportedError(prettyName, mode)
    }
    mode
  }

  private def handleException(e: Throwable): Any = {
    parseMode match {
      case PermissiveMode => null
      case FailFastMode =>
        throw QueryExecutionErrors.malformedProtobufMessageDetectedInMessageParsingError(e)
      case _ =>
        throw QueryCompilationErrors.parseModeUnsupportedError(prettyName, parseMode)
    }
  }

  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    try {
      result = DynamicMessage.parseFrom(messageDescriptor, binary)
      // Check for unknown fields that clash with known field numbers; this indicates
      // a mismatch between writer and reader schemas.  Use findFieldByNumber
      // instead of indexing into getFields by number, because Protobuf field
      // numbers are 1‑based and may not align with the list index.
      result.getUnknownFields.asMap().keySet().asScala.find(fieldsNumbers.contains(_)) match {
        case Some(number) =>
          val conflictingField = Option(messageDescriptor.findFieldByNumber(number))
            .getOrElse(messageDescriptor.getFields.get(number - 1))
          throw QueryCompilationErrors.protobufFieldTypeMismatchError(conflictingField.toString)
        case None => // no clash
      }
      val deserialized = deserializer.deserialize(result)
      require(deserialized.isDefined, "Protobuf deserializer cannot return an empty result because filters are not pushed down")
      deserialized.get
    } catch {
      case NonFatal(e) => handleException(e)
    }
  }

  override def prettyName: String = "from_protobuf"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(
      ctx,
      ev,
      eval => {
        val result = ctx.freshName("result")
        val dt = CodeGenerator.boxedType(dataType)
        s"""
           |$dt $result = ($dt) $expr.nullSafeEval($eval);
           |if ($result == null) {
           |  ${ev.isNull} = true;
           |} else {
           |  ${ev.value} = $result;
           |}
           |""".stripMargin
      }
    )
  }

  override protected def withNewChildInternal(newChild: Expression): ProtobufDataToCatalyst =
    copy(child = newChild)
}