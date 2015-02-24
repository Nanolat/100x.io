package io.x100.colstore.index

import org.scalatest.{Suite, BeforeAndAfterEach}

trait TreeInstanceTrait extends BeforeAndAfterEach {
  this: Suite =>
  val keySpaceSize = 6
  val keyLength = 2
  var tree: VersionedTree = null
  var inode : VersionedTree#InternalNode = null
  var lnode : VersionedTree#LeafNode = null
  val t = newTree // A dummy tree for creating a leaf node and an internal node.

  def newTree = new VersionedTree(keySpaceSize = 6, keyLength = 2)


  override def beforeEach() {
    // set-up code
    //
    tree = newTree
    inode = new t.InternalNode()
    lnode = new t.LeafNode()

    super.beforeEach();
  }

  override def afterEach() {
    super.beforeEach();
    // tear-down code
    //
    tree = null
    inode = null
    lnode = null
  }
}
