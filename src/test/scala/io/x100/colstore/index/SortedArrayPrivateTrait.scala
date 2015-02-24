package io.x100.colstore.index

import io.x100.TestUtil._
import org.scalatest.PrivateMethodTester.PrivateMethod
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{Suite, PrivateMethodTester, BeforeAndAfterEach}

trait SortedArrayPrivateTrait extends BeforeAndAfterEach with PrivateMethodTester with ShouldMatchers { this : Suite =>
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

  /**********************************************************************************************************/
  // Private methods
  val insertKeyDataAt = PrivateMethod[Unit]('insertKeyDataAt)
  val removeKeyDataAt = PrivateMethod[Unit]('removeKeyDataAt)
  val moveKeysRightByTheKeyLengthFrom = PrivateMethod[Unit]('moveKeysRightByTheKeyLengthFrom)
  val moveKeysLeftByTheKeyLengthFrom = PrivateMethod[Unit]('moveKeysLeftByTheKeyLengthFrom)
  val moveDataRightByOneFrom = PrivateMethod[Unit]('moveDataRightByOneFrom)
  val moveDataLeftByOneFrom = PrivateMethod[Unit]('moveDataLeftByOneFrom)
  val setKeyDataAt = PrivateMethod[Unit]('setKeyDataAt)
  val increaseKeyCountForUnitTest = PrivateMethod[Unit]('increaseKeyCountForUnitTest)
  val decreaseKeyCountForUnitTest = PrivateMethod[Unit]('decreaseKeyCountForUnitTest)
  val getKey = PrivateMethod[Array[Byte]]('getKey)
  val getData = PrivateMethod[AnyRef]('getData)
  val setData = PrivateMethod[Unit]('setData)
  val compareKeys = PrivateMethod[Int]( 'compareKeys )

  def assertKey(keyIndex : Int, expectedKey : Array[Byte]): Unit = {
    val keyPos = keyIndex * keyLength
    sarray invokePrivate getKey(keyPos) should be (expectedKey)
  }

  def assertData(keyIndex : Int, expectedData : AnyRef): Unit = {
    val dataPos = keyIndex
    sarray invokePrivate getData(dataPos) should be (expectedData)
  }

  def assertKey(keyIndex : Int, key : String): Unit = {
    assertKey(keyIndex, Arr(key))
  }

  /** Check if a key and a data is on the key position.
    *
    * @param keyIndex 0-based index indicating which key/data to check. ex> 0 for the 1st key, 1 for the 2nd key, etc.
    */
  def assertKeyDataAt(keyIndex : Int, expectedKey : Array[Byte], expectedData : AnyRef ) {
    assertKey( keyIndex, expectedKey )
    assertData( keyIndex, expectedData )
  }

  def assertKeyData(keyIndex : Int, key : String ) {
    assertKeyDataAt(keyIndex, Arr(key), key)
  }

  def assertKeyData(key1 : String): Unit = {
    assertKeyDataAt( 0, Arr(key1), key1 )
  }

  def assertKeyData(key1 : String, key2 : String): Unit = {
    assertKeyDataAt( 0, Arr(key1), key1 )
    assertKeyDataAt( 1, Arr(key2), key2 )
  }

  def assertKeyData(key1 : String, key2 : String, key3 : String): Unit = {
    assertKeyDataAt( 0, Arr(key1), key1 )
    assertKeyDataAt( 1, Arr(key2), key2 )
    assertKeyDataAt( 2, Arr(key3), key3 )
  }


  /** Insert a key as keyIndex the key. Data is the key string itself.
    *
    * @param keyIndex 0-based index indicating which key/data to check. ex> 0 for the 1st key, 1 for the 2nd key, etc.
    */
  def insertKeyData(keyIndex : Int, key : String): Unit = {
    sarray invokePrivate insertKeyDataAt(keyIndex * keyLength, keyIndex, Arr(key), key)
  }

  def fillKeyData(key1 : String): Unit = {
    sarray invokePrivate insertKeyDataAt(0, 0, Arr(key1), key1)
  }

  def fillKeyData(key1 : String, key2 : String): Unit = {
    insertKeyData( 0, key1)
    insertKeyData( 1, key2)
  }

  def fillKeyData(key1 : String, key2 : String, key3 : String): Unit = {
    insertKeyData( 0, key1)
    insertKeyData( 1, key2)
    insertKeyData( 2, key3)
  }

}