/*
 * Test utilities for verifying protobuf optimization behavior.
 *
 * This object is in the backport package to access private[backport] classes
 * like ProtobufDataToCatalyst for testing purposes.
 */

package org.apache.spark.sql.protobuf.backport

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

/**
 * Utilities for testing protobuf schema pruning and optimization.
 */
object ProtobufTestUtils {

  /**
   * Information about a ProtobufDataToCatalyst expression found in a plan.
   *
   * @param fullSchema The full schema from the protobuf descriptor
   * @param prunedSchema The pruned schema if optimization occurred, None otherwise
   */
  case class ProtobufExprInfo(
      fullSchema: StructType,
      prunedSchema: Option[StructType])

  /**
   * Collect all ProtobufDataToCatalyst expressions from a logical plan and
   * extract their schema information.
   *
   * @param plan The logical plan to inspect
   * @return Sequence of ProtobufExprInfo for each protobuf expression found
   */
  def collectProtobufExpressions(plan: LogicalPlan): Seq[ProtobufExprInfo] = {
    val exprs = scala.collection.mutable.ArrayBuffer[ProtobufExprInfo]()

    plan.foreach { node =>
      node.expressions.foreach { expr =>
        expr.foreach {
          case p: ProtobufDataToCatalyst =>
            exprs += ProtobufExprInfo(
              fullSchema = p.dataType,
              prunedSchema = p.requiredSchema
            )
          case _ =>
        }
      }
    }

    exprs.toSeq
  }
}
