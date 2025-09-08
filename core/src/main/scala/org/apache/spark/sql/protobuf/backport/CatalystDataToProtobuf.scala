/*
 * Backport of Spark 3.4's CatalystDataToProtobuf to Spark 3.2.1.
 *
 * This class serializes Catalyst rows into Protobuf binary format using
 * descriptors built from either a descriptor file or a shaded Java
 * class.  See the upstream implementation in Spark 3.4 for details.
 */

package org.apache.spark.sql.protobuf.backport

import com.google.protobuf.DynamicMessage
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.protobuf.backport.utils.ProtobufUtils
import org.apache.spark.sql.types.{BinaryType, DataType}

/**
 * A Catalyst expression that serializes a Catalyst value (typically a
 * struct) into Protobuf binary.  The message descriptor may be built
 * from a descriptor file or a Java class name.  See [[ProtobufSerializer]]
 * for the conversion details.
 *
 * @param child        The input expression to serialize.
 * @param messageName  The fully qualified Protobuf message name or Java class name.
 * @param descFilePath Optional path to a serialized descriptor file.  If provided,
 *                     the descriptor will be loaded from the file; otherwise
 *                     `messageName` is treated as a Java class name.
 * @param options      Options map (currently unused in Spark 3.4 but kept for API completeness).
 */
private[backport] case class CatalystDataToProtobuf(
    child: Expression,
    messageName: String,
    descFilePath: Option[String] = None,
    options: Map[String, String] = Map.empty,

    /**
     * Optional binary descriptor set.  If defined, this descriptor will be used
     * to build the message descriptor instead of reading from a file.  This
     * allows the descriptor to be serialized with the expression and avoids
     * requiring the descriptor file to be present on executors.
     */
    binaryDescriptorSet: Option[Array[Byte]] = None)
  extends UnaryExpression {

  override def dataType: DataType = BinaryType

  @transient private lazy val protoDescriptor: com.google.protobuf.Descriptors.Descriptor =
    binaryDescriptorSet match {
      case Some(bytes) => ProtobufUtils.buildDescriptorFromBytes(bytes, messageName)
      case None => ProtobufUtils.buildDescriptor(messageName, descFilePath)
    }

  @transient private lazy val serializer =
    new ProtobufSerializer(child.dataType, protoDescriptor, child.nullable)

  override def nullSafeEval(input: Any): Any = {
    val dynamicMessage = serializer.serialize(input).asInstanceOf[DynamicMessage]
    dynamicMessage.toByteArray
  }

  override def prettyName: String = "to_protobuf"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input => s"(byte[]) $expr.nullSafeEval($input)")
  }

  override protected def withNewChildInternal(newChild: Expression): CatalystDataToProtobuf =
    copy(child = newChild)
}