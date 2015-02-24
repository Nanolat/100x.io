package io.x100.colstore.index

/**
 * Created by unknown on 2/24/15.
 */
import io.x100.TestUtil._
import org.scalatest.PrivateMethodTester.PrivateMethod
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{Suite, PrivateMethodTester, BeforeAndAfterEach}

trait TreeTestTrait extends BeforeAndAfterEach with PrivateMethodTester with ShouldMatchers {
  this: Suite =>
  val keySpaceSize = 6
  val keyLength = 2
  var tree: VersionedTree = null

  var inode : VersionedTree#InternalNode = null
  var lnode : VersionedTree#LeafNode = null
  val t = newTree // A dummy tree for creating a leaf node and an internal node.

  def newTree = new VersionedTree(keySpaceSize, keyLength)
  override def beforeEach() {
    // set-up code
    //
    tree = newTree
    inode = new t.InternalNode()
    lnode = new t.LeafNode()

    super.beforeEach()
  }

  override def afterEach() {
    super.beforeEach()
    // tear-down code
    //
    tree = null
    inode = null
    lnode = null
  }

  /** ********************************************************************************************************/
  // Private methods
  val compareKeys = PrivateMethod[Int]('compareKeys)
  val findLeafNode = PrivateMethod[VersionedTree#LeafNode]('findLeafNode)
  val putToInternalNode = PrivateMethod[Unit]('putToInternalNode)
  val putToLeafNode = PrivateMethod[Unit]('putToLeafNode)
  val delFromLeafNode = PrivateMethod[Int]('delFromLeafNode)

  /** ********************************************************************************************************/
  def put(key : String): Unit = {
    tree.put( Arr(key), key )
  }

  def get(key : String) = {
    tree.get( Arr(key) )
  }

  def del(key : String) = {
    tree.del( Arr(key) )
  }
}