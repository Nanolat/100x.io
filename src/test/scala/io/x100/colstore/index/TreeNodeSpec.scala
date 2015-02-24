package io.x100.colstore.index

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import MagicValue._
/**
 * Created by unknown on 2/24/15.
 */
class TreeNodeSpec extends FlatSpec with ShouldMatchers {
  val tree = new VersionedTree(keySpaceSize = 6, keyLength = 2)

  "isInternalNode" should "work" in {
    val node = new tree.Node(INTERNAL_NODE_MAGIC)
    node should be an 'internalNode
    node should not be 'leafNode
  }

  "isLeafNode" should "work" in {
    val node = new tree.Node(LEAF_NODE_MAGIC)
    node should be a 'leafNode
    node should not be 'internalNode
  }
}
