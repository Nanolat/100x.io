package io.x100.colstore.index

import io.x100.TestUtil.Arr
import org.scalatest._
import org.scalatest.matchers.ShouldMatchers



trait SortedArrayInstance extends BeforeAndAfterEach { this : Suite =>
  val keySpaceSize = 6
  val keyLength = 2
  var sarray : SortedArray = null

  override def beforeEach() {
    // set-up code
    //
    sarray = new SortedArray( keySpaceSize, keyLength );

    super.beforeEach();
  }

  override def afterEach() {
    super.beforeEach();
    // tear-down code
    //
    sarray = null
  }

}

/**
 * Created by unknown on 2/23/15.
 */
class SortedArraySpec extends FlatSpec with ShouldMatchers with PrivateMethodTester with SortedArrayInstance {

  /**********************************************************************************************************/
  // Private methods
  val insertKeyDataAt = PrivateMethod[Unit]('insertKeyDataAt)
  val removeKeyDataAt = PrivateMethod[Unit]('removeKeyDataAt)
  val moveKeysRightByTheKeyLengthFrom = PrivateMethod[Unit]('moveKeysRightByTheKeyLengthFrom)
  val moveKeysLeftByTheKeyLengthFrom = PrivateMethod[Unit]('moveKeysLeftByTheKeyLengthFrom)
  val moveDataRightByOneFrom = PrivateMethod[Unit]('moveDataRightByOneFrom)
  val moveDataLeftByOneFrom = PrivateMethod[Unit]('moveDataLeftByOneFrom)
  val setKeyDataAt = PrivateMethod[Array[Byte]]('setKeyDataAt)
  val getKey = PrivateMethod[Array[Byte]]('getKey)
  val getData = PrivateMethod[AnyRef]('getData)
  val setData = PrivateMethod[Unit]('setData)
  val compareKeys = PrivateMethod[Int]( 'compareKeys )

  /**********************************************************************************************************/

  "A sorted array" should "hit assertion failure if key count is less than 2" in {
    the [java.lang.AssertionError] thrownBy {
      sarray = new SortedArray( 1, 1 );
    }
  }

  it should "hit assertion failure if the key count of keySpace does not match the one on SortedArray" in {
    the [java.lang.AssertionError] thrownBy {
      sarray invokePrivate compareKeys(
        Arr("a"),
        Arr("abc"), 0 ) should be (0)
    }

  }

  /**********************************************************************************************************/

  "A key comparison" should "compare keys in any position" in {
    sarray = new SortedArray( 2, 1 );

    sarray invokePrivate compareKeys(
      Arr("a"),
      Arr("ab"), 0 ) should be (0)

    sarray invokePrivate compareKeys(
      Arr("b"),
      Arr("ab"), 1 ) should be (0)

    sarray = new SortedArray( 3, 1 );

    sarray invokePrivate compareKeys(
      Arr("a"),
      Arr("abc"), 0 ) should be (0)

    sarray invokePrivate compareKeys(
      Arr("b"),
      Arr("abc"), 1 ) should be (0)

    sarray invokePrivate compareKeys(
      Arr("c"),
      Arr("abc"), 2 ) should be (0)


    sarray = new SortedArray( 6, 2 );

    sarray invokePrivate compareKeys( Arr("ab"),
                                      Arr("abcdef"), 0 ) should be (0)

    sarray invokePrivate compareKeys( Arr("cd"),
                                      Arr("abcdef"), 2 ) should be (0)

    sarray invokePrivate compareKeys( Arr("ef"),
                                      Arr("abcdef"), 4 ) should be (0)

  }

  it should "hit an assertion with negative offset" in {
    the [java.lang.AssertionError] thrownBy {
      sarray invokePrivate compareKeys(
        Arr("a"),
        Arr("abc"), -1 )
    }
  }

  it should "hit an assertion with offset equal to the length of keyspace" in {
    the [java.lang.AssertionError] thrownBy {
      sarray invokePrivate compareKeys(
        Arr("a"),
        Arr("abc"), 3 )
    }
  }

  it should "hit an assertion failure with empty key" in {
    the [java.lang.AssertionError] thrownBy {
      sarray invokePrivate compareKeys(
        Arr(""),
        Arr("abc"), 0 )
    }
  }
/*
    it should "hit an assertion failure with null key" in {
      the [java.lang.AssertionError] thrownBy {
        sarray invokePrivate compareKeys(
          null,
          Arr("abc"), 0 )
      }
    }

    it should "hit an assertion failure with null keyspace" in {
      the [java.lang.AssertionError] thrownBy {
        sarray invokePrivate compareKeys(
          Arr("a"),
          null, 0 )
      }
    }
*/
  it should "hit an assertion failure with empty keyspace" in {
    the [java.lang.AssertionError] thrownBy {
      sarray invokePrivate compareKeys(
        Arr("a"),
        Arr(""), 0 )
    }
  }

  it should "hit an assertion failure with keyspace whose length does not equal to the one in SortedArray" in {
    the [java.lang.AssertionError] thrownBy {
      sarray invokePrivate compareKeys(
        Arr("a"),
        Arr("ab"), 0 )
    }
  }

  /**********************************************************************************************************/
  "insertKeyDataAt" should "successfully insert ordered data" in  {
    sarray invokePrivate insertKeyDataAt(0, 0, Arr("ab"), "ab")
    sarray invokePrivate getKey(0) should be (Arr("ab"))
    sarray invokePrivate getData(0) should be ("ab")

    sarray invokePrivate insertKeyDataAt(2, 1, Arr("cd"), "cd")
    sarray invokePrivate getKey(0) should be (Arr("ab"))
    sarray invokePrivate getKey(2) should be (Arr("cd"))
    sarray invokePrivate getData(0) should be ("ab")
    sarray invokePrivate getData(1) should be ("cd")

    sarray invokePrivate insertKeyDataAt(4, 2, Arr("ef"), "ef")
    sarray invokePrivate getKey(0) should be (Arr("ab"))
    sarray invokePrivate getKey(2) should be (Arr("cd"))
    sarray invokePrivate getKey(4) should be (Arr("ef"))
    sarray invokePrivate getData(0) should be ("ab")
    sarray invokePrivate getData(1) should be ("cd")
    sarray invokePrivate getData(2) should be ("ef")
  }

  it should "successfully insert unordered data" in  {
    sarray invokePrivate insertKeyDataAt(0, 0, Arr("cd"), "cd")
    sarray invokePrivate getKey(0) should be (Arr("cd"))
    sarray invokePrivate getData(0) should be ("cd")

    sarray invokePrivate insertKeyDataAt(0, 0, Arr("ab"), "ab")
    sarray invokePrivate getKey(0) should be (Arr("ab"))
    sarray invokePrivate getKey(2) should be (Arr("cd"))
    sarray invokePrivate getData(0) should be ("ab")
    sarray invokePrivate getData(1) should be ("cd")

    sarray invokePrivate insertKeyDataAt(4, 2, Arr("ef"), "ef")
    sarray invokePrivate getKey(0) should be (Arr("ab"))
    sarray invokePrivate getKey(2) should be (Arr("cd"))
    sarray invokePrivate getKey(4) should be (Arr("ef"))
    sarray invokePrivate getData(0) should be ("ab")
    sarray invokePrivate getData(1) should be ("cd")
    sarray invokePrivate getData(2) should be ("ef")
  }

  it should "successfully insert ordered data if inserted between two keys" in  {
    sarray invokePrivate insertKeyDataAt(0, 0, Arr("ef"), "ef")
    sarray invokePrivate getKey(0) should be (Arr("ef"))
    sarray invokePrivate getData(0) should be ("ef")

    sarray invokePrivate insertKeyDataAt(0, 0, Arr("ab"), "ab")
    sarray invokePrivate getKey(0) should be (Arr("ab"))
    sarray invokePrivate getKey(2) should be (Arr("ef"))
    sarray invokePrivate getData(0) should be ("ab")
    sarray invokePrivate getData(1) should be ("ef")

    sarray invokePrivate insertKeyDataAt(2, 1, Arr("cd"), "cd")
    sarray invokePrivate getKey(0) should be (Arr("ab"))
    sarray invokePrivate getKey(2) should be (Arr("cd"))
    sarray invokePrivate getKey(4) should be (Arr("ef"))
    sarray invokePrivate getData(0) should be ("ab")
    sarray invokePrivate getData(1) should be ("cd")
    sarray invokePrivate getData(2) should be ("ef")
  }

  it should "successfully insert ordered data ordered in descending order" in  {
    sarray invokePrivate insertKeyDataAt(0, 0, Arr("ef"), "ef")
    sarray invokePrivate getKey(0) should be (Arr("ef"))
    sarray invokePrivate getData(0) should be ("ef")

    sarray invokePrivate insertKeyDataAt(0, 0, Arr("cd"), "cd")
    sarray invokePrivate getKey(0) should be (Arr("cd"))
    sarray invokePrivate getKey(2) should be (Arr("ef"))
    sarray invokePrivate getData(0) should be ("cd")
    sarray invokePrivate getData(1) should be ("ef")

    sarray invokePrivate insertKeyDataAt(0, 0, Arr("ab"), "ab")
    sarray invokePrivate getKey(0) should be (Arr("ab"))
    sarray invokePrivate getKey(2) should be (Arr("cd"))
    sarray invokePrivate getKey(4) should be (Arr("ef"))
    sarray invokePrivate getData(0) should be ("ab")
    sarray invokePrivate getData(1) should be ("cd")
    sarray invokePrivate getData(2) should be ("ef")
  }

  it should "hit an assertion failure if no more space left" in  {
    sarray invokePrivate insertKeyDataAt(0, 0, Arr("ab"), "ab")
    sarray invokePrivate insertKeyDataAt(2, 1, Arr("cd"), "cd")
    sarray invokePrivate insertKeyDataAt(4, 2, Arr("ef"), "ef")

    the [java.lang.AssertionError] thrownBy {
      sarray invokePrivate insertKeyDataAt(0, 0, Arr("xx"), "xx")
    }
  }

  /**********************************************************************************************************/

  "setKeyDataAt" should "work" in  {

  }

  /**********************************************************************************************************/

  "removeKeyDataAt" should "hit an assertion failure if empty" in {

  }

  it should "hit an assertion failure if no key found at the position" in {

  }


  it should "work with one key" in {

  }

  it should "work with two keys" in {

  }

  it should "work with three keys" in {

  }

  /**********************************************************************************************************/

  "moveKeysRightByTheKeyLengthFrom" should "hit an assertion failure if empty" in {

  }

  it should "hit an assertion failure if full" in {

  }

  it should "work with one key" in {

  }

  it should "work with two keys" in {

  }

  /**********************************************************************************************************/

  "moveKeysLeftByTheKeyLengthFrom" should "hit an assertion failure if empty" in {

  }

  it should "hit an assertion failure if full" in {

  }

  it should "work with one key" in {

  }

  it should "work with two keys" in {

  }

  /**********************************************************************************************************/

  "moveDataRightByOneFrom" should "hit an assertion failure if empty" in {

  }

  it should "hit an assertion failure if full" in {

  }

  it should "work with one key" in {

  }

  it should "work with two keys" in {

  }

  /**********************************************************************************************************/

  "moveDataLeftByOneFrom" should "hit an assertion failure if empty" in {

  }

  it should "hit an assertion failure if full" in {

  }

  it should "work with one key" in {

  }

  it should "work with two keys" in {

  }
}
