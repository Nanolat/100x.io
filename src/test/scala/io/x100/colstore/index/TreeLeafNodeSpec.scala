package io.x100.colstore.index

import io.x100.TestUtil._
import io.x100.UnsupportedFeature
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

/**
 * Created by unknown on 2/24/15.
 */
class TreeLeafNodeSpec extends FlatSpec with ShouldMatchers with TreeInstanceTrait {

  def assertStatus(node : VersionedTree#LeafNode, keyCount : Int, isEmpty : Boolean, isFull : Boolean, minKey : String): Unit = {
    node.keysWithValues.keyCount should be (keyCount)
    node.isEmpty() should be (isEmpty)
    node.isFull() should be (isFull)
    val expectedKey = if (minKey == null) null else Arr(minKey)
    node.minKey() should be ( expectedKey )
  }

  def put(key : String): Unit = {
    lnode.put(Arr(key), key)
  }

  def del(key : String) = {
    val deletedData = lnode.del( Arr(key) )
    deletedData
  }


  def assertGet(node : VersionedTree#LeafNode, key:String, expectedValue : String) : Unit = {
    node.get( Arr(key) ) should be (expectedValue)
  }

  def assertGet(key:String, expectedValue : String) : Unit = {
    assertGet(lnode, key, expectedValue)
  }

  "put/get/del" should "work with a key" in {
    assertStatus(lnode, keyCount=0, isEmpty=true, isFull=false, minKey=null)

    put("ab")
    assertStatus(lnode, keyCount=1, isEmpty=false, isFull=false, minKey="ab")
    assertGet("ab", "ab")

    del("ab")
    assertStatus(lnode, keyCount=0, isEmpty=true, isFull=false, minKey=null)
    assertGet("ab", null)
  }

  it should "work with two keys" in {
    assertStatus(lnode, keyCount=0, isEmpty=true, isFull=false, minKey=null)

    put("cd")
    assertStatus(lnode, keyCount=1, isEmpty=false, isFull=false, minKey="cd")
    assertGet("cd", "cd")

    put("ab")
    assertStatus(lnode, keyCount=2, isEmpty=false, isFull=false, minKey="ab")
    assertGet("ab", "ab")

    del("ab")
    assertStatus(lnode, keyCount=1, isEmpty=false, isFull=false, minKey="cd")
    assertGet("ab", null)

    del("cd")
    assertStatus(lnode, keyCount=0, isEmpty=true, isFull=false, minKey=null)
    assertGet("cd", null)
  }

  it should "work with three keys" in {
    assertStatus(lnode, keyCount=0, isEmpty=true, isFull=false, minKey=null)

    put("cd")
    assertStatus(lnode, keyCount=1, isEmpty=false, isFull=false, minKey="cd")
    assertGet("cd", "cd")

    put("ab")
    assertStatus(lnode, keyCount=2, isEmpty=false, isFull=false, minKey="ab")
    assertGet("ab", "ab")

    put("ef")
    assertStatus(lnode, keyCount=3, isEmpty=false, isFull=true, minKey="ab")
    assertGet("ef", "ef")

    del("ab")
    assertStatus(lnode, keyCount=2, isEmpty=false, isFull=false, minKey="cd")
    assertGet("ab", null)

    del("cd")
    assertStatus(lnode, keyCount=1, isEmpty=false, isFull=false, minKey="ef")
    assertGet("cd", null)

    del("ef")
    assertStatus(lnode, keyCount=0, isEmpty=true, isFull=false, minKey=null)
    assertGet("ef", null)
  }

  it should "return null if the key was not found" in {
    del("ab") should be (null)
  }

  /**********************************************************************************************************/
  "mergeWith" should "throw the UnsupportedFeature exception" in {
    the [UnsupportedFeature] thrownBy {
      val lnode2 = new t.LeafNode()
      lnode.mergeWith(lnode2)
    }
  }

  "split" should "work with two keys" in {
    put("ab")
    put("cd")

    val rightNode = lnode.split()
    assertStatus(lnode, 1, false, false, "ab")
    assertGet( "ab", "ab" )
    assertGet( "cd", null )

    assertStatus(rightNode, 1, false, false, "cd")
    assertGet( rightNode, "ab", null )
    assertGet( rightNode, "cd", "cd" )
  }


  "split" should "work with tree keys" in {
    put("ab")
    put("cd")
    put("ef")

    val rightNode = lnode.split()
    assertStatus(lnode, 2, false, false, "ab")
    assertGet( "ab", "ab" )
    assertGet( "cd", "cd" )
    assertGet( "ef", null )
    assertStatus(rightNode, 1, false, false, "ef")
    assertGet( rightNode, "ab", null )
    assertGet( rightNode, "cd", null )
    assertGet( rightNode, "ef", "ef" )
  }

  "split" should "hit an assertion failure if it has less than two keys" in {
    a [java.lang.AssertionError] should be thrownBy (lnode.split())

    put("ab")
    a [java.lang.AssertionError] should be thrownBy (lnode.split())

    put("cd")
    lnode.split()
  }
}
