package io.x100.colstore.index

import io.x100.TestUtil.Arr
import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

/**
 * Created by unknown on 2/23/15.
 */
class SortedArrayPrivateSpec extends FlatSpec with ShouldMatchers with SortedArrayPrivateTrait {
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
    insertKeyData(0, "ab")
    assertKeyData( "ab" )

    insertKeyData(1, "cd")
    assertKeyData("ab", "cd")

    insertKeyData(2, "ef")
    assertKeyData("ab", "cd", "ef")
  }

  it should "successfully insert unordered data" in  {
    insertKeyData(0, "cd")
    assertKeyData( "cd" )

    insertKeyData(0, "ab")
    assertKeyData("ab", "cd")

    insertKeyData(2, "ef")
    assertKeyData("ab", "cd", "ef")
  }

  it should "successfully insert ordered data if inserted between two keys" in  {
    insertKeyData(0, "ef")
    assertKeyData( "ef" )

    insertKeyData(0, "ab")
    assertKeyData("ab", "ef")

    insertKeyData(1, "cd")
    assertKeyData("ab", "cd", "ef")
  }

  it should "successfully insert ordered data ordered in descending order" in  {
    insertKeyData(0, "ef")
    assertKeyData( "ef" )

    insertKeyData(0, "cd")
    assertKeyData("cd", "ef")

    insertKeyData(0, "ab")
    assertKeyData("ab", "cd", "ef")
  }

  it should "hit an assertion failure if no more space left" in  {
    fillKeyData("ab","cd","ef")

    the [java.lang.AssertionError] thrownBy {
      sarray invokePrivate insertKeyDataAt(0, 0, Arr("xx"), "xx")
    }
  }

  /**********************************************************************************************************/
  "setKeyDataAt" should "work" in  {
    sarray invokePrivate setKeyDataAt(0, 0, Arr("ab"), "ab")
    sarray invokePrivate increaseKeyCountForUnitTest()
    assertKeyData("ab")

    sarray invokePrivate setKeyDataAt(2, 1, Arr("cd"), "cd")
    sarray invokePrivate increaseKeyCountForUnitTest()
    assertKeyData(1, "cd")

    // Replace
    sarray invokePrivate setKeyDataAt(2, 1, Arr("11"), "11")
    sarray invokePrivate setKeyDataAt(0, 0, Arr("00"), "00")
    assertKeyData("00", "11")
  }

  it should "hit an assertion failure if either keyPos or dataPos is out of range" in {
    Array( (-1,0), (0,-1), (-1,-1), (6,0), (0,3), (6,3) ) map { case ( keyPos, dataPos) =>
      a [java.lang.AssertionError] should be thrownBy (sarray invokePrivate setKeyDataAt(keyPos, dataPos, Arr("ab"), "ab"))
    }
  }
/*
  it should "hit an assertion failure if either key or data is set to null " in {
    the [java.lang.AssertionError] thrownBy {
      sarray invokePrivate setKeyDataAt(0, 0, null, "ab")
    }

    the [java.lang.AssertionError] thrownBy {
      sarray invokePrivate setKeyDataAt(0, 0, Arr("ab"), null)
    }
  }
*/
  /**********************************************************************************************************/
  "removeKeyDataAt" should "work with one key" in {
    insertKeyData(0, "ab")

    sarray invokePrivate removeKeyDataAt(0, 0)

    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate removeKeyDataAt(0, 0))
  }

  it should "work with two keys" in {
    // case 1 : remove the first key continously.
    insertKeyData(0, "ab")
    insertKeyData(1, "cd")

    sarray invokePrivate removeKeyDataAt(0, 0)
    assertKeyData("cd")

    sarray invokePrivate removeKeyDataAt(0, 0)

    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate removeKeyDataAt(0, 0))

    // case 2 : remove the second key first, and then remove the first one.
    insertKeyData(0, "ab")
    insertKeyData(1, "cd")

    sarray invokePrivate removeKeyDataAt(2, 1)
    assertKeyData("ab")

    sarray invokePrivate removeKeyDataAt(0, 0)

    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate removeKeyDataAt(0, 0))
  }

  it should "work with three keys" in {
    // case 1 : remove the first key continously.
    insertKeyData(0, "ab")
    insertKeyData(1, "cd")
    insertKeyData(2, "ef")

    sarray invokePrivate removeKeyDataAt(0, 0)
    assertKeyData("cd")

    sarray invokePrivate removeKeyDataAt(0, 0)
    assertKeyData("ef")

    sarray invokePrivate removeKeyDataAt(0, 0)

    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate removeKeyDataAt(0, 0))

    // case 2 : remove the second key two times and then remove the first one.
    fillKeyData("ab", "cd", "ef")

    sarray invokePrivate removeKeyDataAt(2, 1)
    assertKeyData("ab", "ef")

    sarray invokePrivate removeKeyDataAt(2, 1)
    assertKeyData("ab")

    sarray invokePrivate removeKeyDataAt(0, 0)

    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate removeKeyDataAt(0, 0))

    // case 3 : remove the third key, second key, and then first key.
    fillKeyData("ab", "cd", "ef")

    sarray invokePrivate removeKeyDataAt(4, 2)
    assertKeyData("ab", "cd")

    sarray invokePrivate removeKeyDataAt(2, 1)
    assertKeyData("ab")

    sarray invokePrivate removeKeyDataAt(0, 0)

    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate removeKeyDataAt(0, 0))
  }

  it should "hit an assertion failure if empty" in {
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate removeKeyDataAt(0, 0))
  }

  it should "hit an assertion failure if no key found at the position" in {
    insertKeyData(0, "ab")

    // Should hit an assertion, because no key is at the postition.
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate removeKeyDataAt(2, 1))
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate removeKeyDataAt(4, 2))

    insertKeyData(1, "cd")

    // Should hit an assertion, because no key is at the postition.
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate removeKeyDataAt(4, 2))
  }

  it should "hit an assertion failure if either keyPos or dataPos is out of range" in {
    Array( (-1,0), (0,-1), (-1,-1), (6,0), (0,3), (6,3) ) map { case ( keyPos, dataPos) =>
      info(s"with keyPos=$keyPos, dataPos=$dataPos")
      a [java.lang.AssertionError] should be thrownBy (sarray invokePrivate removeKeyDataAt(keyPos, dataPos))
    }
  }


  /**********************************************************************************************************/
  "moveKeysRightByTheKeyLengthFrom" should "work with one key" in {
    fillKeyData("ab")

    sarray invokePrivate moveKeysRightByTheKeyLengthFrom(0)
    sarray invokePrivate increaseKeyCountForUnitTest()

    assertKey(1, "ab")
  }

  it should "work with two keys if moved from 1st key" in {
    fillKeyData("ab", "cd")

    sarray invokePrivate moveKeysRightByTheKeyLengthFrom(0)
    sarray invokePrivate increaseKeyCountForUnitTest()

    assertKey(1, "ab")
    assertKey(2, "cd")
  }

  it should "work with two keys if moved from 2nd key" in {
    fillKeyData("ab", "cd")

    sarray invokePrivate moveKeysRightByTheKeyLengthFrom(1)
    sarray invokePrivate increaseKeyCountForUnitTest()

    assertKey(0, "ab")
    assertKey(2, "cd")
  }

  it should "hit an assertion failure if empty" in {
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveKeysRightByTheKeyLengthFrom(0))
  }

  it should "hit an assertion failure if full" in {
    fillKeyData("ab", "cd", "ef")

    Array( 0, 2, 4 ) map { keyPos =>
      an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveKeysRightByTheKeyLengthFrom( keyPos ))
    }
  }

  it should "hit an assertion failure if keyPos is out of range" in {
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveKeysRightByTheKeyLengthFrom( -1 ))

    insertKeyData(0, "ab")
    // It should fail to try to move keys at a position that no key exists
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveKeysRightByTheKeyLengthFrom( 2 ))

    insertKeyData(1, "cd")
    // It should fail to try to move keys at a position that no key exists
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveKeysRightByTheKeyLengthFrom( 4 ))

    insertKeyData(2, "ef")
    // It should fail to try to move keys at a position that no key exists
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveKeysRightByTheKeyLengthFrom( 6 ))
  }

  /**********************************************************************************************************/
  "moveKeysLeftByTheKeyLengthFrom" should "hit an assertion failure with a key" in {
    fillKeyData("ab")

    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveKeysLeftByTheKeyLengthFrom(0))
  }

  it should "work with two keys if moved from 2nd key" in {
    fillKeyData("ab", "cd")

    sarray invokePrivate moveKeysLeftByTheKeyLengthFrom(2)
    sarray invokePrivate decreaseKeyCountForUnitTest()

    assertKey(0, "cd")
  }

  it should "work with three keys if moved from 2nd key" in {
    fillKeyData("ab", "cd", "ef")

    sarray invokePrivate moveKeysLeftByTheKeyLengthFrom(2)
    sarray invokePrivate decreaseKeyCountForUnitTest()

    assertKey(0, "cd")
    assertKey(1, "ef")
  }

  it should "work with three keys if moved from 3rd key" in {
    fillKeyData("ab", "cd", "ef")

    sarray invokePrivate moveKeysLeftByTheKeyLengthFrom(4)
    sarray invokePrivate decreaseKeyCountForUnitTest()

    assertKey(0, "ab")
    assertKey(1, "ef")
  }

  it should "hit an assertion failure if empty" in {
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveKeysLeftByTheKeyLengthFrom(0))
  }

  it should "hit an assertion failure if keyPos is out of range" in {
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveKeysLeftByTheKeyLengthFrom( -1 ))

    insertKeyData(0, "ab")
    // It should fail to try to move keys at a position that no key exists
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveKeysLeftByTheKeyLengthFrom( 2 ))

    insertKeyData(1, "cd")
    // It should fail to try to move keys at a position that no key exists
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveKeysLeftByTheKeyLengthFrom( 4 ))

    insertKeyData(2, "ef")
    // It should fail to try to move keys at a position that no key exists
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveKeysLeftByTheKeyLengthFrom( 6 ))
  }

  /**********************************************************************************************************/
  "moveDataRightByOneFrom"  should "work with one key" in {
    fillKeyData("ab")

    sarray invokePrivate moveDataRightByOneFrom(0)
    sarray invokePrivate increaseKeyCountForUnitTest()

    assertData(1, "ab")
  }

  it should "work with two keys if moved from 1st key" in {
    fillKeyData("ab", "cd")

    sarray invokePrivate moveDataRightByOneFrom(0)
    sarray invokePrivate increaseKeyCountForUnitTest()

    assertData(1, "ab")
    assertData(2, "cd")
  }

  it should "work with two keys if moved from 2nd key" in {
    fillKeyData("ab", "cd")

    sarray invokePrivate moveDataRightByOneFrom(1)
    sarray invokePrivate increaseKeyCountForUnitTest()

    assertData(0, "ab")
    assertData(2, "cd")
  }

  it should "hit an assertion failure if empty" in {
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveDataRightByOneFrom(0))
  }

  it should "hit an assertion failure if full" in {
    fillKeyData("ab", "cd", "ef")

    Array( 0, 2, 4 ) map { keyPos =>
      an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveDataRightByOneFrom( keyPos ))
    }
  }

  it should "hit an assertion failure if keyPos is out of range" in {
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveKeysLeftByTheKeyLengthFrom( -1 ))

    insertKeyData(0, "ab")
    // It should fail to try to move keys at a position that no key exists
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveDataRightByOneFrom( 1 ))

    insertKeyData(1, "cd")
    // It should fail to try to move keys at a position that no key exists
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveDataRightByOneFrom( 2 ))

    insertKeyData(2, "ef")
    // It should fail to try to move keys at a position that no key exists
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveDataRightByOneFrom( 3 ))
  }

  /**********************************************************************************************************/
  "moveDataLeftByOneFrom" should "hit an assertion failure with a key" in {
    fillKeyData("ab")

    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveDataLeftByOneFrom(0))
  }

  it should "work with two keys if moved from 2nd key" in {
    fillKeyData("ab", "cd")

    sarray invokePrivate moveDataLeftByOneFrom(1)
    sarray invokePrivate decreaseKeyCountForUnitTest()

    assertData(0, "cd")
  }

  it should "work with three keys if moved from 2nd key" in {
    fillKeyData("ab", "cd", "ef")

    sarray invokePrivate moveDataLeftByOneFrom(1)
    sarray invokePrivate decreaseKeyCountForUnitTest()

    assertData(0, "cd")
    assertData(1, "ef")
  }

  it should "work with three keys if moved from 3rd key" in {
    fillKeyData("ab", "cd", "ef")

    sarray invokePrivate moveDataLeftByOneFrom(2)
    sarray invokePrivate decreaseKeyCountForUnitTest()

    assertData(0, "ab")
    assertData(1, "ef")
  }

  it should "hit an assertion failure if empty" in {
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveDataLeftByOneFrom(0))
  }

  it should "hit an assertion failure if keyPos is out of range" in {
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveDataLeftByOneFrom( -1 ))

    insertKeyData(0, "ab")
    // It should fail to try to move keys at a position that no key exists
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveDataLeftByOneFrom( 1 ))

    insertKeyData(1, "cd")
    // It should fail to try to move keys at a position that no key exists
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveDataLeftByOneFrom( 2 ))

    insertKeyData(2, "ef")
    // It should fail to try to move keys at a position that no key exists
    an [java.lang.AssertionError] should be thrownBy (sarray invokePrivate moveDataLeftByOneFrom( 3 ))
  }
}
