package io.x100.colstore.index

import io.x100.UnsupportedFeature
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import io.x100.TestUtil._

/**
 * Created by unknown on 2/24/15.
 */
class TreeInternalNodeSpec extends FlatSpec with ShouldMatchers with TreeInstanceTrait {

  def assertStatus(node : VersionedTree[String]#InternalNode, keyCount : Int, isEmpty : Boolean, isFull : Boolean, minKey : String): Unit = {
    node.keysWithRightChildren.keyCount should be (keyCount)
    node.isEmpty() should be (isEmpty)
    node.isFull() should be (isFull)
    val expectedKey = if (minKey == null) null else Arr(minKey)
    node.minKey() should be ( expectedKey )
  }

  def put(key : String): Unit = {
    val node = new t.LeafNode()
    node.put(Arr(key), key)
    inode.put( Arr(key), node)
  }

  def del(key : String) = {
    val deletedNode = inode.del( Arr(key) )
    deletedNode
  }
  /**********************************************************************************************************/
  "put" should "hit an assersion failure if to many keys are put" in {
    put("ab")
    put("cd")
    put("ef")

    // If we try to put more than the node can handle, it should hit an assertion.
    a [AssertionError] should be thrownBy (put("xx"))
  }

  def assertServingNode( keyAndMinKeyOfServingNode : (String, String)* ) = {
    keyAndMinKeyOfServingNode.map { case (key, minKeyOfServingNode) =>
      val servingNode = inode.findServingNodeByKey( Arr(key) )
      if (servingNode != null) {
        // When we put a key in inode, we associated it with a leaf node whose minKey is same to the associated key for verification purpose.
        val leafNode = servingNode.asInstanceOf[VersionedTree[String]#LeafNode]
        leafNode.minKey() should be ( Arr(minKeyOfServingNode) )
      } else {
        // When the key is not served by child node mapped with a key on inode,
        // findServingNode returns inode.leftChild. But because we did not set leftChild, it is null.
        // In test cases, we set minKeyOfServingNode to null in this case.
        minKeyOfServingNode should be (null)
      }
    }
  }
  /**********************************************************************************************************/
  "put/findServingNode/del" should "work with a key" in {
    assertStatus(inode, keyCount=0, isEmpty=true, isFull=false, minKey=null)

    put("ab")
    assertStatus(inode, keyCount=1, isEmpty=false, isFull=false, minKey="ab")
    // List pair of (search key, the key that is associated with the serving node)
    assertServingNode( ("a0", null), ("ab", "ab"), ("ax", "ab") )

    del("ab")
    assertStatus(inode, keyCount=0, isEmpty=true, isFull=false, minKey=null)
  }

  it should "work with two keys" in {
    assertStatus(inode, keyCount=0, isEmpty=true, isFull=false, minKey=null)

    put("cd")
    assertStatus(inode, keyCount=1, isEmpty=false, isFull=false, minKey="cd")
    assertServingNode( ("c0", null), ("cd", "cd"), ("cx", "cd") )

    put("ab")
    assertStatus(inode, keyCount=2, isEmpty=false, isFull=false, minKey="ab")
    assertServingNode( ("a0", null), ("ab", "ab"), ("ax", "ab"), ("cd", "cd"), ("cx", "cd") )

    del("ab")
    assertStatus(inode, keyCount=1, isEmpty=false, isFull=false, minKey="cd")
    assertServingNode( ("c0", null), ("cd", "cd"), ("cx", "cd") )

    del("cd")
    assertStatus(inode, keyCount=0, isEmpty=true, isFull=false, minKey=null)
  }

  it should "work with three keys" in {
    assertStatus(inode, keyCount=0, isEmpty=true, isFull=false, minKey=null)

    put("cd")
    assertStatus(inode, keyCount=1, isEmpty=false, isFull=false, minKey="cd")
    assertServingNode( ("c0", null), ("cd", "cd"), ("cx", "cd") )

    put("ab")
    assertStatus(inode, keyCount=2, isEmpty=false, isFull=false, minKey="ab")
    assertServingNode( ("a0", null), ("ab", "ab"), ("ax", "ab"), ("cd", "cd"), ("cx", "cd") )

    put("ef")
    assertStatus(inode, keyCount=3, isEmpty=false, isFull=true, minKey="ab")
    assertServingNode( ("a0", null), ("ab", "ab"), ("ax", "ab"), ("cd", "cd"), ("cx", "cd"),
                       ("ef", "ef"), ("ex", "ef")
                     )

    del("ab")
    assertStatus(inode, keyCount=2, isEmpty=false, isFull=false, minKey="cd")
    assertServingNode( ("c0", null), ("cd", "cd"), ("cx", "cd"), ("ef", "ef"), ("ex", "ef") )

    del("cd")
    assertStatus(inode, keyCount=1, isEmpty=false, isFull=false, minKey="ef")
    assertServingNode( ("e0", null), ("ef", "ef"), ("ex", "ef") )

    del("ef")
    assertStatus(inode, keyCount=0, isEmpty=true, isFull=false, minKey=null)
  }

  it should "return null if the key was not found" in {
    del("ab") should be (null)
  }

  /**********************************************************************************************************/
  "mergeWith" should "throw the UnsupportedFeature exception" in {
    the [UnsupportedFeature] thrownBy {
      val inode2 = new t.InternalNode()
      inode.mergeWith(inode2)
    }
  }

  /**********************************************************************************************************/
  "split" should "work with tree keys" in {
    put("ab")
    put("cd")
    put("ef")

    val (rightNode, midKey) = inode.split()
    rightNode.minKey() should be ( Arr("ef") )
    midKey should be ( Arr("cd") )
  }

  "split" should "hit an assertion failure if it has less than three keys" in {
    a [java.lang.AssertionError] should be thrownBy (inode.split())

    put("ab")
    a [java.lang.AssertionError] should be thrownBy (inode.split())

    put("cd")
    a [java.lang.AssertionError] should be thrownBy (inode.split())
  }
}
