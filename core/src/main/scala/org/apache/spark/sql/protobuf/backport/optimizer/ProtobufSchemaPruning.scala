/*
 * Optimizer rule for nested schema pruning in protobuf deserialization.
 *
 * This rule identifies ProtobufDataToCatalyst expressions and determines
 * which nested fields are actually accessed, then rewrites the expression
 * to use a pruned schema containing only the required fields.
 */

package org.apache.spark.sql.protobuf.backport.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.protobuf.backport.ProtobufDataToCatalyst
import org.apache.spark.sql.protobuf.backport.utils.{ProtobufConfig, SchemaUtils}
import org.apache.spark.sql.types._

/**
 * Prunes nested fields from ProtobufDataToCatalyst expressions based on
 * which fields are actually accessed in the query.  This optimization
 * reduces deserialization overhead by skipping fields that are not needed.
 *
 * The rule currently applies only to WireFormat-based expressions (binary
 * descriptor set usage) to simplify implementation.
 *
 * Example transformation:
 * {{{
 *   SELECT data.person.name FROM table
 *   -- Before: deserializes entire protobuf including all person fields
 *   -- After: deserializes only person.name field
 * }}}
 */
private[backport] object ProtobufSchemaPruning extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!ProtobufConfig.nestedSchemaPruningEnabled) {
      return plan
    }

    plan.transformDown {
      case p @ Project(projectList, child) =>
        val newChild = pruneProtobufInChild(projectList, child)
        if (newChild ne child) {
          p.copy(child = newChild)
        } else {
          p
        }
    }
  }

  /**
   * Prune ProtobufDataToCatalyst expressions in the child plan based on
   * which fields are accessed in the projection and child plan.
   */
  private def pruneProtobufInChild(
      projectList: Seq[NamedExpression],
      child: LogicalPlan): LogicalPlan = {

    // Build map of attribute references to their required paths from projection
    val requiredFromProject = collectRequiredFieldsPerAttribute(projectList)

    // Also collect requirements from the child plan itself (filters, joins, etc.)
    val requiredFromChild = collectRequiredFieldsFromPlan(child)

    // Merge requirements from both sources
    val requiredFieldsPerAttr = mergeRequiredFields(requiredFromProject, requiredFromChild)

    if (requiredFieldsPerAttr.isEmpty) {
      return child
    }

    // Transform the child plan to prune protobuf expressions
    child.transformExpressionsUp {
      case a @ Alias(p: ProtobufDataToCatalyst, name) if canPrune(p) =>
        // Check if this alias has required fields
        requiredFieldsPerAttr.get(a.toAttribute.exprId) match {
          case Some(requiredPaths) if requiredPaths.nonEmpty =>
            val prunedSchema = SchemaUtils.pruneSchema(p.dataType, requiredPaths)
            if (prunedSchema.fields.length < p.dataType.fields.length) {
              // Rewrite with pruned schema
              Alias(p.withPrunedSchema(prunedSchema), name)(a.exprId, a.qualifier, a.explicitMetadata)
            } else {
              a
            }
          case _ => a
        }
    }
  }

  /**
   * Check if a ProtobufDataToCatalyst expression can be pruned.  Pruning
   * is currently only supported for WireFormat parser (binary descriptor set).
   */
  private def canPrune(expr: ProtobufDataToCatalyst): Boolean = {
    // Only prune if using binary descriptor set (WireFormat parser)
    // and if schema hasn't already been pruned
    expr.binaryDescriptorSet.isDefined && expr.requiredSchema.isEmpty
  }

  /**
   * Collect field references from the projection list, tracking which nested
   * fields are accessed from each attribute.  Returns a map from attribute
   * expression ID to the set of required field paths.
   *
   * For example, if projecting `data.person.name` where `data` is an attribute,
   * this returns:
   * {{{
   *   Map(data.exprId -> Set(Seq("person", "name")))
   * }}}
   */
  private def collectRequiredFieldsPerAttribute(
      projectList: Seq[NamedExpression]): Map[ExprId, Set[Seq[String]]] = {

    val fieldsMap = scala.collection.mutable.Map[ExprId, Set[Seq[String]]]()

    projectList.foreach { expr =>
      collectFieldReferencesFromExpression(expr, fieldsMap)
    }

    fieldsMap.toMap
  }

  /**
   * Recursively collect field references from an expression, tracking which
   * nested fields are accessed from attribute references.
   */
  private def collectFieldReferencesFromExpression(
      expr: Expression,
      fieldsMap: scala.collection.mutable.Map[ExprId, Set[Seq[String]]]): Unit = {

    expr match {
      case GetStructField(child, _, Some(name)) =>
        // Traverse down to find the root attribute and build the path
        val pathOpt = collectFieldPath(child, Seq(name))
        pathOpt.foreach { case (attrId, path) =>
          fieldsMap(attrId) = fieldsMap.getOrElse(attrId, Set.empty) + path
        }

      case GetArrayStructFields(child, field, _, _, _) =>
        // Array of structs: traverse down to find root attribute
        val pathOpt = collectFieldPath(child, Seq(field.name))
        pathOpt.foreach { case (attrId, path) =>
          fieldsMap(attrId) = fieldsMap.getOrElse(attrId, Set.empty) + path
        }

      case a: AttributeReference =>
        // Entire attribute used, mark as requiring all fields
        fieldsMap(a.exprId) = fieldsMap.getOrElse(a.exprId, Set.empty) + Seq.empty

      case _ =>
        // Recurse into children
        expr.children.foreach(collectFieldReferencesFromExpression(_, fieldsMap))
    }
  }

  /**
   * Build a field path by traversing GetStructField expressions down to an
   * AttributeReference.  Returns Some((attributeId, fieldPath)) if an
   * attribute is found, None otherwise.
   */
  private def collectFieldPath(
      expr: Expression,
      currentPath: Seq[String]): Option[(ExprId, Seq[String])] = {

    expr match {
      case a: AttributeReference =>
        Some((a.exprId, currentPath))

      case GetStructField(child, _, Some(name)) =>
        collectFieldPath(child, name +: currentPath)

      case GetArrayItem(child, _, _) =>
        // Array element access - continue traversing
        collectFieldPath(child, currentPath)

      case _ =>
        None
    }
  }

  /**
   * Collect field references from all expressions in a logical plan.
   * This captures references in filters, joins, and other operations.
   */
  private def collectRequiredFieldsFromPlan(
      plan: LogicalPlan): Map[ExprId, Set[Seq[String]]] = {

    val fieldsMap = scala.collection.mutable.Map[ExprId, Set[Seq[String]]]()

    plan.foreach { node =>
      node.expressions.foreach { expr =>
        collectFieldReferencesFromExpression(expr, fieldsMap)
      }
    }

    fieldsMap.toMap
  }

  /**
   * Merge two required fields maps, combining the field sets for each attribute.
   */
  private def mergeRequiredFields(
      map1: Map[ExprId, Set[Seq[String]]],
      map2: Map[ExprId, Set[Seq[String]]]): Map[ExprId, Set[Seq[String]]] = {

    val merged = scala.collection.mutable.Map[ExprId, Set[Seq[String]]]()

    // Add all from map1
    map1.foreach { case (id, fields) =>
      merged(id) = fields
    }

    // Merge with map2
    map2.foreach { case (id, fields) =>
      merged(id) = merged.getOrElse(id, Set.empty) ++ fields
    }

    merged.toMap
  }
}
