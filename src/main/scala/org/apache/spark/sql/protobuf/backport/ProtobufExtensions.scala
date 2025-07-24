/*
 * Extension for Spark 3.2.x to register Protobuf SQL functions.
 *
 * This class implements the `Function1[SparkSessionExtensions, Unit]` trait
 * such that it can be supplied via the `spark.sql.extensions` configuration
 * property.  When applied it registers `from_protobuf` and `to_protobuf`
 * as built‑in SQL functions backed by the expressions defined in this
 * backport.
 */

package org.apache.spark.sql.protobuf.backport

import scala.collection.Seq

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.AnalysisException

/**
 * Registers the Protobuf conversion functions with a [[SparkSessionExtensions]].
 */
class ProtobufExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    registerFromProtobuf(extensions)
    registerToProtobuf(extensions)
  }

  /** Register the `from_protobuf` SQL function. */
  private def registerFromProtobuf(extensions: SparkSessionExtensions): Unit = {
    val name = "from_protobuf"
    val exprInfo = new ExpressionInfo(
      classOf[org.apache.spark.sql.protobuf.backport.ProtobufDataToCatalyst].getName,
      name)
    val funcId = new FunctionIdentifier(name)
    val builder: Seq[Expression] => Expression = { exprs =>
      if (exprs.length < 2) {
        throw new AnalysisException(s"Function $name requires at least 2 arguments")
      }
      val dataExpr = exprs.head
      val messageName = getLiteralString(exprs(1), "message or class name")
      val descFilePathOpt = if (exprs.length > 2) {
        Some(getLiteralString(exprs(2), "descriptor file path"))
      } else None
      // Options are ignored in this simplified builder; additional parameters beyond the third
      // are currently ignored for SQL usage.  DataFrame API can still supply options via the
      // overloaded functions in org.apache.spark.sql.protobuf.backport.functions.
      ProtobufDataToCatalyst(dataExpr, messageName, descFilePathOpt, Map.empty)
    }
    extensions.injectFunction((funcId, exprInfo, builder))
  }

  /** Register the `to_protobuf` SQL function. */
  private def registerToProtobuf(extensions: SparkSessionExtensions): Unit = {
    val name = "to_protobuf"
    val exprInfo = new ExpressionInfo(
      classOf[org.apache.spark.sql.protobuf.backport.CatalystDataToProtobuf].getName,
      name)
    val funcId = new FunctionIdentifier(name)
    val builder: Seq[Expression] => Expression = { exprs =>
      if (exprs.length < 2) {
        throw new AnalysisException(s"Function $name requires at least 2 arguments")
      }
      val dataExpr = exprs.head
      val messageName = getLiteralString(exprs(1), "message or class name")
      val descFilePathOpt = if (exprs.length > 2) {
        Some(getLiteralString(exprs(2), "descriptor file path"))
      } else None
      CatalystDataToProtobuf(dataExpr, messageName, descFilePathOpt, Map.empty)
    }
    extensions.injectFunction((funcId, exprInfo, builder))
  }

  /**
   * Extract a literal String value from an [[Expression]].  Throws an
   * [[AnalysisException]] if the expression is not a literal or does not
   * evaluate to a string.
   */
  private def getLiteralString(expr: Expression, what: String): String = expr match {
    case Literal(value, dataType) if value != null => value.toString
    case other =>
      throw new AnalysisException(s"Expected a literal string for $what but found: $other")
  }
}