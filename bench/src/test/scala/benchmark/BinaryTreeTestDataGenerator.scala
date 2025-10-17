package benchmark

import benchmark.BinaryTreeBenchmarkProtos.{BinaryTreeDocument, BinaryTreeNode}
import com.google.protobuf.ByteString

/**
 * Generates full binary trees for controlled benchmarking.
 *
 * Full binary tree properties:
 * - Every internal node has exactly 2 children
 * - All leaf nodes are at the same depth
 * - Total nodes = 2^(depth+1) - 1
 *
 * Example depths:
 * - depth=3: 15 nodes (1+2+4+8)
 * - depth=5: 63 nodes (1+2+4+8+16+32)
 * - depth=8: 511 nodes
 */
object BinaryTreeTestDataGenerator {

  /**
   * Create a full binary tree with specified depth.
   *
   * @param maxDepth maximum depth (0 = single root node)
   * @param payloadSize size of binary payload for each node (0 = no payload)
   * @return complete binary tree document
   */
  def createBinaryTree(maxDepth: Int, payloadSize: Int = 0): BinaryTreeDocument = {
    val startTime = System.currentTimeMillis()
    val root = createNode(0, maxDepth, 1, payloadSize)
    val totalNodes = (1 << (maxDepth + 1)) - 1  // 2^(depth+1) - 1

    BinaryTreeDocument.newBuilder()
      .setRoot(root)
      .setTotalNodes(totalNodes)
      .setMaxDepth(maxDepth)
      .build()
  }

  /**
   * Create a binary tree node recursively.
   *
   * @param depth current depth (0-indexed)
   * @param maxDepth maximum depth
   * @param value node value (incremented for each node)
   * @param payloadSize size of binary payload (0 = no payload)
   * @return binary tree node
   */
  private def createNode(depth: Int, maxDepth: Int, value: Int, payloadSize: Int): BinaryTreeNode = {
    val builder = BinaryTreeNode.newBuilder()
      .setValue(value)
      .setDepth(depth)

    // Add children if not at max depth (full binary tree)
    if (depth < maxDepth) {
      val leftValue = value * 2
      val rightValue = value * 2 + 1
      builder
        .setLeft(createNode(depth + 1, maxDepth, leftValue, payloadSize))
        .setRight(createNode(depth + 1, maxDepth, rightValue, payloadSize))
    }

    // Add payload if size > 0
    if (payloadSize > 0) {
      builder.setPayload(buildPayload(payloadSize))
    }

    builder.build()
  }

  private def buildPayload(payloadSize: Int) = {
    com.google.protobuf.ByteString.copyFrom(Array.fill(payloadSize)(0.toByte))
  }

  /**
   * Get deterministic binary data for binary tree.
   */
  def getBinaryTreeBinary(maxDepth: Int, payloadSize: Int = 0): Array[Byte] = {
    createBinaryTree(maxDepth, payloadSize).toByteArray
  }

  /**
   * Create pruned schema for accessing value at specific depth.
   *
   * Examples:
   * - depth=1: root.left.value or root.right.value
   * - depth=2: root.left.left.value, root.left.right.value, etc.
   * - depth=3: root.left.left.left.value, etc.
   */
  def createPrunedSchemaForDepth(accessDepth: Int, accessField: String = "value"): String = {
    require(accessDepth >= 1, s"accessDepth must be >= 1, got $accessDepth")

    // Build path from root to leaf
    val path = (1 to accessDepth).map(_ => "left").mkString(".")
    s"root.$path.$accessField"
  }
}
