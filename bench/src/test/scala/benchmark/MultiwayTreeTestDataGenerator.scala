package benchmark

/**
 * Generator for multiway tree test data with configurable depth and branching factor.
 *
 * Uses the multiway_tree.proto structure:
 * - left: single child
 * - right: repeated children
 *
 * For branching factor B:
 * - If B >= 1: use left child + (B-1) right children
 * - Each internal node has exactly B children
 * - Leaf nodes have no children
 */
object MultiwayTreeTestDataGenerator {

  /**
   * Generate a full multiway tree with specified depth and branching factor.
   *
   * @param depth Maximum depth (1 = just root, 2 = root + children, etc.)
   * @param branchingFactor Number of children per internal node
   * @return Generated tree root node
   */
  def generateTree(depth: Int, branchingFactor: Int): benchmark.Node = {
    require(depth >= 1, s"Depth must be >= 1, got $depth")
    require(branchingFactor >= 1, s"Branching factor must be >= 1, got $branchingFactor")

    buildNode(currentDepth = 1, maxDepth = depth, branchingFactor = branchingFactor, nodeId = 0)._1
  }

  /**
   * Build a node and all its descendants.
   * Returns (node, nextNodeId) where nextNodeId is the ID to use for the next node.
   */
  private def buildNode(
      currentDepth: Int,
      maxDepth: Int,
      branchingFactor: Int,
      nodeId: Int
  ): (benchmark.Node, Int) = {
    val builder = benchmark.Node.newBuilder()
      .setValue(nodeId)
      .setPayload(s"node_$nodeId")

    // If we're at max depth, this is a leaf node
    if (currentDepth >= maxDepth) {
      return (builder.build(), nodeId + 1)
    }

    // Build children for internal nodes
    var nextId = nodeId + 1

    // Build left child if branching factor >= 1
    if (branchingFactor >= 1) {
      val (leftChild, newNextId) = buildNode(currentDepth + 1, maxDepth, branchingFactor, nextId)
      builder.setLeft(leftChild)
      nextId = newNextId
    }

    // Build right children (branchingFactor - 1 children)
    for (_ <- 0 until (branchingFactor - 1)) {
      val (rightChild, newNextId) = buildNode(currentDepth + 1, maxDepth, branchingFactor, nextId)
      builder.addRight(rightChild)
      nextId = newNextId
    }

    (builder.build(), nextId)
  }

  /**
   * Calculate the total number of nodes in a full tree.
   * Formula: (B^D - 1) / (B - 1) where B = branching factor, D = depth
   */
  def calculateNodeCount(depth: Int, branchingFactor: Int): Int = {
    if (branchingFactor == 1) {
      depth // Special case: linear chain
    } else {
      (math.pow(branchingFactor, depth).toInt - 1) / (branchingFactor - 1)
    }
  }

  /**
   * Generate example tree for debugging.
   */
  def exampleTree(depth: Int = 3, branchingFactor: Int = 2): benchmark.Node = {
    generateTree(depth, branchingFactor)
  }
}
