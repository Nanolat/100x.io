package io.x100.colstore.index

import io.x100.TestUtil._
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

/**
 * Created by unknown on 2/24/15.
 */
class TreePublicSpec extends FlatSpec with ShouldMatchers with TreeTestTrait {
  /**********************************************************************************************************/
  "put/get/del" should "work" in {
    (0 to 128) map { testKeyCount =>
      putKeys(0, testKeyCount)
      assertGetKeys(0, testKeyCount)
      // For each key, iterate tree starting from the key
      assertForwardIteratorByEachKey(0, testKeyCount)
      assertBackwardIteratorByEachKey(0, testKeyCount)

      def deleteFilter(n : Int) = n % (testKeyCount%7 + 2) == 1
      def assertFilter(n : Int) = ! deleteFilter(n)
      delKeys(0, testKeyCount, Some(deleteFilter) )
      assertGetKeys(0, testKeyCount, Some(assertFilter) )
      assertForwardIteratorByEachKey(0, testKeyCount, assertFilter)
      assertBackwardIteratorByEachKey(0, testKeyCount, assertFilter)
    }
  }

  "seekForward/moveForward" should "work with a key starting from a key" in {
    testForwardIterator(
      keys = Array("ab"),
      findingKey = Some("a0"),
      expectedKeys = Array( "ab" )
    )

    testForwardIterator(
      keys = Array("ab"),
      findingKey = Some("ab"),
      expectedKeys = Array( "ab" )
    )

    testForwardIterator(
      keys = Array("ab"),
      findingKey = Some("ax"),
      expectedKeys = Array()
    )
  }

  it should "work with two keys starting from a key" in {
    testForwardIterator(
      keys = Array("cd", "ab"),
      findingKey = Some("a0"),
      expectedKeys = Array( "ab", "cd" )
    )

    testForwardIterator(
      keys = Array("cd", "ab"),
      findingKey = Some("ab"),
      expectedKeys = Array( "ab", "cd" )
    )

    testForwardIterator(
      keys = Array("cd", "ab"),
      findingKey = Some("ax"),
      expectedKeys = Array("cd")
    )

    testForwardIterator(
      keys = Array("cd", "ab"),
      findingKey = Some("cd"),
      expectedKeys = Array("cd")
    )

    testForwardIterator(
      keys = Array("cd", "ab"),
      findingKey = Some("cx"),
      expectedKeys = Array()
    )
  }


  it should "work with three keys starting from a key" in {
    testForwardIterator(
      keys = Array("ef", "cd", "ab"),
      findingKey = Some("a0"),
      expectedKeys = Array( "ab", "cd", "ef" )
    )

    testForwardIterator(
      keys = Array("ef", "cd", "ab"),
      findingKey = Some("ab"),
      expectedKeys = Array( "ab", "cd", "ef" )
    )

    testForwardIterator(
      keys = Array("ef", "cd", "ab"),
      findingKey = Some("ax"),
      expectedKeys = Array( "cd", "ef" )
    )

    testForwardIterator(
      keys = Array("ef", "cd", "ab"),
      findingKey = Some("cd"),
      expectedKeys = Array( "cd", "ef" )
    )

    testForwardIterator(
      keys = Array("ef", "cd", "ab"),
      findingKey = Some("cx"),
      expectedKeys = Array( "ef" )
    )

    testForwardIterator(
      keys = Array("ef", "cd", "ab"),
      findingKey = Some("ef"),
      expectedKeys = Array( "ef" )
    )

    testForwardIterator(
      keys = Array("ef", "cd", "ab"),
      findingKey = Some("ex"),
      expectedKeys = Array()
    )
  }


  "seekBackward/moveBackward" should "work with a key starting from a key" in {
    testBackwardIterator(
      keys = Array("ab"),
      findingKey = Some("ax"),
      expectedKeys = Array( "ab" )
    )

    testBackwardIterator(
      keys = Array("ab"),
      findingKey = Some("ab"),
      expectedKeys = Array( "ab" )
    )

    testBackwardIterator(
      keys = Array("ab"),
      findingKey = Some("a0"),
      expectedKeys = Array()
    )
  }

  it should "work with two keys starting from a key" in {
    testBackwardIterator(
      keys = Array("cd", "ab"),
      findingKey = Some("cx"),
      expectedKeys = Array("cd", "ab")
    )

    testBackwardIterator(
      keys = Array("cd", "ab"),
      findingKey = Some("cd"),
      expectedKeys = Array("cd", "ab")
    )

    testBackwardIterator(
      keys = Array("cd", "ab"),
      findingKey = Some("ax"),
      expectedKeys = Array("ab")
    )

    testBackwardIterator(
      keys = Array("cd", "ab"),
      findingKey = Some("ab"),
      expectedKeys = Array("ab")
    )

    testBackwardIterator(
      keys = Array("cd", "ab"),
      findingKey = Some("a0"),
      expectedKeys = Array()
    )
  }

  it should "work with three keys starting from a key" in {
    testBackwardIterator(
      keys = Array("ef", "cd", "ab"),
      findingKey = Some("ex"),
      expectedKeys = Array("ef", "cd", "ab")
    )

    testBackwardIterator(
      keys = Array("ef", "cd", "ab"),
      findingKey = Some("ef"),
      expectedKeys = Array("ef", "cd", "ab")
    )

    testBackwardIterator(
      keys = Array("ef", "cd", "ab"),
      findingKey = Some("cx"),
      expectedKeys = Array("cd", "ab")
    )

    testBackwardIterator(
      keys = Array("ef", "cd", "ab"),
      findingKey = Some("cd"),
      expectedKeys = Array("cd", "ab")
    )

    testBackwardIterator(
      keys = Array("ef", "cd", "ab"),
      findingKey = Some("ax"),
      expectedKeys = Array("ab")
    )

    testBackwardIterator(
      keys = Array("ef", "cd", "ab"),
      findingKey = Some("ab"),
      expectedKeys = Array("ab")
    )

    testBackwardIterator(
      keys = Array("ef", "cd", "ab"),
      findingKey = Some("a0"),
      expectedKeys = Array()
    )
  }

  "moveBackward after seekForward" should "throw an exception" in {
    put("ab")
    val iter = VersionedTreeIterator[String]()
    assert(iter != null)
    tree.seekForward(iter, Arr("ab"))
    a [java.lang.IllegalStateException] should be thrownBy( tree.moveBackward(iter) )
  }

  "moveForward after seekBackward" should "throw an exception" in {
    put("ab")
    val iter = VersionedTreeIterator[String]()
    assert(iter != null)
    tree.seekBackward(iter, Arr("ab"))
    a [java.lang.IllegalStateException] should be thrownBy( tree.moveForward(iter) )
  }

}
