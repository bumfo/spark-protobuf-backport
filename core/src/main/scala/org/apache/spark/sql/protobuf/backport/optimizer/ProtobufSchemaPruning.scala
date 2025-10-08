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
        // Handle two cases:
        // 1. Pre-collapse: child has Alias(ProtobufDataToCatalyst)
        // 2. Post-collapse: projectList has expressions directly accessing ProtobufDataToCatalyst
        val newChild = pruneProtobufInChild(projectList, child)
        val newProjectList = pruneProtobufInProjectList(projectList)

        if ((newChild ne child) || (newProjectList ne projectList)) {
          p.copy(projectList = newProjectList, child = newChild)
        } else {
          p
        }
    }
  }

  /**
   * Prune ProtobufDataToCatalyst expressions directly in the project list.
   * This handles the post-collapse case where Projects have been merged.
   */
  private def pruneProtobufInProjectList(
      projectList: Seq[NamedExpression]): Seq[NamedExpression] = {

    // Collect required field paths for each ProtobufDataToCatalyst expression
    val requiredFields = collectRequiredFieldsForProtobuf(projectList)

    if (requiredFields.isEmpty) {
      return projectList
    }

    // Build pruned schemas and ordinal mappings for each ProtobufDataToCatalyst
    val prunedInfo = scala.collection.mutable.Map[ProtobufDataToCatalyst, ProtobufDataToCatalyst]()
    requiredFields.foreach { case (proto, paths) =>
      if (canPrune(proto) && paths.nonEmpty) {
        val prunedSchema = SchemaUtils.pruneSchema(proto.dataType, paths)
        if (prunedSchema.fields.length < proto.dataType.fields.length) {
          val prunedProto = proto.withPrunedSchema(prunedSchema)
          prunedInfo(proto) = prunedProto
        }
      }
    }

    if (prunedInfo.isEmpty) {
      return projectList
    }

    // Rewrite expressions - use transformUp to handle children before parents
    projectList.map {
      case a @ Alias(child, name) =>
        val newChild = child.transformUp {
          // First, replace ProtobufDataToCatalyst with pruned version
          case p: ProtobufDataToCatalyst =>
            prunedInfo.getOrElse(p, p)

          // Then, update GetStructField ordinals based on child schema
          case g @ GetStructField(child, ordinal, fieldName) =>
            val childSchema = child.dataType match {
              case st: StructType => Some(st)
              case _ => None
            }

            (childSchema, fieldName) match {
              case (Some(schema), Some(name)) =>
                // Find the new ordinal in the (possibly pruned) child schema
                val newOrdinal = schema.fields.indexWhere(_.name == name)
                if (newOrdinal >= 0 && newOrdinal != ordinal) {
                  GetStructField(child, newOrdinal, fieldName)
                } else {
                  g
                }
              case _ => g
            }
        }
        if (newChild ne child) {
          Alias(newChild, name)(a.exprId, a.qualifier, a.explicitMetadata)
        } else {
          a
        }
      case other => other
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
   * Collect required field paths for each ProtobufDataToCatalyst in expressions.
   * Returns a map from ProtobufDataToCatalyst instance to its required field paths.
   */
  private def collectRequiredFieldsForProtobuf(
      expressions: Seq[Expression]): Map[ProtobufDataToCatalyst, Set[Seq[String]]] = {

    val fieldsMap = scala.collection.mutable.Map[ProtobufDataToCatalyst, Set[Seq[String]]]()

    expressions.foreach { expr =>
      collectProtobufFieldAccess(expr, fieldsMap)
    }

    fieldsMap.toMap
  }

  /**
   * Recursively collect field access patterns for ProtobufDataToCatalyst expressions.
   */
  private def collectProtobufFieldAccess(
      expr: Expression,
      fieldsMap: scala.collection.mutable.Map[ProtobufDataToCatalyst, Set[Seq[String]]]): Unit = {

    expr match {
      case GetStructField(child, _, Some(name)) =>
        val pathOpt = collectProtobufFieldPath(child, Seq(name))
        pathOpt.foreach { case (proto, path) =>
          fieldsMap(proto) = fieldsMap.getOrElse(proto, Set.empty) + path
        }

      case p: ProtobufDataToCatalyst =>
        // Entire protobuf used, mark as requiring all fields
        fieldsMap(p) = fieldsMap.getOrElse(p, Set.empty) + Seq.empty

      case _ =>
        // Recurse into children
        expr.children.foreach(collectProtobufFieldAccess(_, fieldsMap))
    }
  }

  /**
   * Build field path by traversing GetStructField to ProtobufDataToCatalyst.
   * Returns Some((protobuf, fieldPath)) if found, None otherwise.
   */
  private def collectProtobufFieldPath(
      expr: Expression,
      currentPath: Seq[String]): Option[(ProtobufDataToCatalyst, Seq[String])] = {

    expr match {
      case p: ProtobufDataToCatalyst =>
        Some((p, currentPath))

      case GetStructField(child, _, Some(name)) =>
        collectProtobufFieldPath(child, name +: currentPath)

      case GetArrayItem(child, _, _) =>
        collectProtobufFieldPath(child, currentPath)

      case _ =>
        None
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
