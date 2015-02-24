package io.x100.colstore.index

/**
 * Created by unknown on 2/24/15.
 */
import io.x100.TestUtil._
import org.scalatest.PrivateMethodTester.PrivateMethod
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{Suite, PrivateMethodTester, BeforeAndAfterEach}

trait TreePrivateTrait extends BeforeAndAfterEach with PrivateMethodTester with ShouldMatchers {
  this: Suite =>
  val keySpaceSize = 6
  val keyLength = 2
  var tree: VersionedTree = null

  override def beforeEach() {
    // set-up code
    //
    tree = new VersionedTree(keySpaceSize, keyLength);

    super.beforeEach();
  }

  override def afterEach() {
    super.beforeEach();
    // tear-down code
    //
    tree = null
  }

  /** ********************************************************************************************************/
  // Private methods
  val compareKeys = PrivateMethod[Int]('compareKeys)
  val findLeafNode = PrivateMethod[VersionedTree#LeafNode]('findLeafNode)
  val putToInternalNode = PrivateMethod[Unit]('putToInternalNode)
  val putToLeafNode = PrivateMethod[Unit]('putToLeafNode)
  val delFromLeafNode = PrivateMethod[Int]('delFromLeafNode)

}