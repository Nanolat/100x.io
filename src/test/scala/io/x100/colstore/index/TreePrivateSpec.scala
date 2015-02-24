package io.x100.colstore.index

import org.scalatest.PrivateMethodTester.PrivateMethod
import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import io.x100.TestUtil._

/**
 * Created by unknown on 2/23/15.
 */
class TreePrivateSpec extends FlatSpec with ShouldMatchers with TreeTestTrait {

  /**********************************************************************************************************/
  "compareKeys" should "work" in {
    tree invokePrivate compareKeys(Arr("ab"), Arr("ab")) should be (0)
    tree invokePrivate compareKeys(Arr("a0"), Arr("ab")) should be < 0
    tree invokePrivate compareKeys(Arr("ax"), Arr("ab")) should be > 0
  }

  def assertLeafNode(min : Int, max : Int) : Unit = {
    (min to max) map { i =>
      val key = "%02x" format i
      //info (s"findLeafNode($key)")
      val rawKey = Arr(key)
      val leafNode = tree invokePrivate findLeafNode( rawKey )
      val expectedData = key
      leafNode.get( rawKey ) should be ( expectedData )
    }
  }

  /**********************************************************************************************************/
  "findLeafNode" should "work" in {
    putKeys(0, 255)
    assertLeafNode(0, 255)
  }

  /**********************************************************************************************************/
  "putToInternalNode" should "work" in {

  }

  /**********************************************************************************************************/
  "putToLeafNode" should "work" in {

  }

  /**********************************************************************************************************/
  "delFromLeafNode" should "work" in {

  }
}
