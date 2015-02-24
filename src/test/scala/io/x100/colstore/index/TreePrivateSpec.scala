package io.x100.colstore.index

import org.scalatest.PrivateMethodTester.PrivateMethod
import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

/**
 * Created by unknown on 2/23/15.
 */
class TreePrivateSpec extends FlatSpec with ShouldMatchers {
  /**********************************************************************************************************/
  // Private methods
  val compareKeys = PrivateMethod[Int]('compareKeys)
  val findLeafNode = PrivateMethod[Int]('findLeafNode)
  val putToInternalNode = PrivateMethod[Int]('putToInternalNode)
  val putToLeafNode = PrivateMethod[Int]('putToLeafNode)
  val delFromLeafNode = PrivateMethod[Int]('delFromLeafNode)

  /**********************************************************************************************************/
  "compareKeys" should "work" in {

  }

  /**********************************************************************************************************/
  "findLeafNode" should "work" in {

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
