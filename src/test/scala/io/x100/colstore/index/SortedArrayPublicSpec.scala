package io.x100.colstore.index

import org.scalatest.{Suite, BeforeAndAfterEach, FlatSpec}
import org.scalatest.matchers.ShouldMatchers
import io.x100._

import io.x100.TestUtil.Arr


/**
 * Created by unknown on 2/23/15.
 */
class SortedArrayPublicSpec  extends FlatSpec with ShouldMatchers with SortedArrayInstanceTrait {


  def put(key : String): Unit = {
    sarray.put( Arr(key), key )
  }

  def put(key : String, data : String): Unit = {
    sarray.put( Arr(key), data )
  }


  def del(key : String): String = {
    sarray.del( Arr(key) ).asInstanceOf[String]
  }

  def get(key : String): String = {
    sarray.get( Arr(key) ).asInstanceOf[String]
  }


  def assertValues(keys:String*) : Unit = {
    keys.map { key =>
      val expectedData = key
      get(key) should be (expectedData)
    }
  }

  def delKey(key:String) : Unit = {
    val expectedData = key
    del(key) should be (expectedData)
  }

  def assertStatus(sa : SortedArray, keyCount : Int, isEmpty : Boolean, isFull : Boolean, minKey : String): Unit = {
    sa.keyCount should be (keyCount)
    sa.isEmpty() should be (isEmpty)
    sa.isFull() should be (isFull)
    val expectedKey = if (minKey == null) null else Arr(minKey)
    sa.minKey() should be ( expectedKey )
  }

  def assertStatus(keyCount : Int, isEmpty : Boolean, isFull : Boolean, minKey : String): Unit = {
    assertStatus(sarray, keyCount, isEmpty, isFull, minKey)
  }


  /**********************************************************************************************************/
  "put/get/del/isEmpty/isFull/minKey" should "work" in {
    assertStatus(0, isEmpty=true, isFull=false, null)
    get("cd") should be (null)

    put("cd")
    assertStatus(1, isEmpty=false, isFull=false, "cd")
    assertValues("cd")


    put("ab")
    assertStatus(2, isEmpty=false, isFull=false, "ab")
    assertValues("ab", "cd")

    put("ef")
    assertStatus(3, isEmpty=false, isFull=true, "ab")
    assertValues("ab", "cd", "ef")

    delKey("cd")
    assertStatus(2, isEmpty=false, isFull=false, "ab")
    get("cd") should be (null)
    assertValues("ab", "ef")

    delKey("ab")
    assertStatus(1, isEmpty=false, isFull=false, "ef")
    get("ab") should be (null)
    assertValues("ef")

    delKey("ef")
    assertStatus(0, isEmpty=true, isFull=false, null)
    get("ef") should be (null)
  }

  /**********************************************************************************************************/

  "put" should "replace the existing data" in {
    put("ab")
    get("ab") should be ("ab")

    (1 to 10).map { i =>
      val data = s"new-$i"
      put("ab", data)
      get("ab") should be (data)
    }
  }

  it should "hit an assertion failure if too many keys are put" in {
    put("ab")
    put("cd")
    put("ef")
    a [java.lang.AssertionError] should be thrownBy ( put("xx") )
  }

  /**********************************************************************************************************/

  "del" should "return null if the key does not exist" in {
    put("ab")
    put("cd")
    put("ef")
    del("ax") should be (null)
    del("cx") should be (null)
    del("ex") should be (null)
  }

  /**********************************************************************************************************/

  "removeMaxKey" should "work" in {
    put("ab")
    put("ef")
    put("cd")
    assertStatus(3, isEmpty=false, isFull=true, "ab")

    {
      val (key, data) = sarray.removeMaxKey()
      key should be (Arr("ef"))
      data should be ("ef")
      assertStatus(2, isEmpty=false, isFull=false, "ab")
    }

    {
      val (key, data) = sarray.removeMaxKey()
      key should be (Arr("cd"))
      data should be ("cd")
      assertStatus(1, isEmpty=false, isFull=false, "ab")
    }


    {
      val (key, data) = sarray.removeMaxKey()
      key should be (Arr("ab"))
      data should be ("ab")
      assertStatus(0, isEmpty=true, isFull=false, null)
    }
  }

  /**********************************************************************************************************/

  "split" should "work with two keys" in {
    put("ab")
    put("cd")
    assertStatus(sarray, 2, isEmpty=false, isFull=false, "ab")

    val rightArray = newArray
    sarray.split(rightArray)
    assertStatus(sarray, 1, isEmpty=false, isFull=false, "ab")
    assertStatus(rightArray, 1, isEmpty=false, isFull=false, "cd")
  }

  it should "work with three keys" in {
    put("ab")
    put("cd")
    put("ef")
    assertStatus(sarray, 3, isEmpty=false, isFull=true, "ab")

    val rightArray = newArray
    sarray.split(rightArray)
    assertStatus(sarray, 2, isEmpty=false, isFull=false, "ab")
    assertStatus(rightArray, 1, isEmpty=false, isFull=false, "ef")
  }

  it should "hit an assertion with a key" in {
    put("ab")
    val rightArray = newArray
    a [java.lang.AssertionError] should be thrownBy ( sarray.split(rightArray) )
  }

  /**********************************************************************************************************/

  "mergeWith" should "throw the UnsupportedFeature exception" in {
    the [UnsupportedFeature] thrownBy {
      val sarray2 = new SortedArray(keySpaceSize = 6, keyLength = 2)
      sarray.mergeWith(sarray2)
    }
  }

  def testForwardIterator(keys : Array[String], expectedKeys : Array[String], findingKey : Option[String] = None): Unit = {
    sarray = newArray

    keys.map { key =>
      put(key)
    }

    // Initialize iterator
    val iter = SortedArrayIterator()
    findingKey match {
      case Some(key) => sarray.iterForward(iter, Arr(key) )
      case None => sarray.iterForward(iter)
    }

    // Check (key,value) pair for each iteration.
    expectedKeys.map {
      expectedKey => {
        sarray.iterNext(iter) match { case (key, data) => {
            key should be ( Arr(expectedKey) )
            val expectedValue = expectedKey
            data should be (expectedValue)
          }
        }
      }
    }

    // If we iterate once more, both key and value should be null
    sarray.iterNext(iter) match { case (key, data) => {
        key should be (null)
        data should be (null)
      }
    }
  }

  def testBackwardIterator(keys : Array[String], expectedKeys : Array[String], findingKey : Option[String] = None): Unit = {
    sarray = newArray

    keys.map { key =>
      put(key)
    }

    // Initialize iterator
    val iter = SortedArrayIterator()
    findingKey match {
      case Some(key) => sarray.iterBackward(iter, Arr(key) )
      case None => sarray.iterBackward(iter)
    }

    // Check (key,value) pair for each iteration.
    expectedKeys.map {
      expectedKey => {
        sarray.iterPrev(iter) match { case (key, data) => {
          key should be ( Arr(expectedKey) )
          val expectedValue = expectedKey
          data should be (expectedValue)
        }
        }
      }
    }

    // If we iterate once more, both key and value should be null
    sarray.iterPrev(iter) match { case (key, data) => {
      key should be (null)
      data should be (null)
    }
    }
  }


  /**********************************************************************************************************/
  "iterForward and iterNext" should "work with a key" in {
    testForwardIterator(
      keys = Array("ab"),
      expectedKeys = Array( "ab" )
    )
  }

  it should "work with two keys" in {
    testForwardIterator(
      keys = Array("cd", "ab"),
      expectedKeys = Array( "ab", "cd" )
    )
  }

  it should "work with three keys" in {
    testForwardIterator(
      keys = Array("ef", "cd", "ab"),
      expectedKeys = Array( "ab", "cd", "ef" )
    )
  }

  it should "work with a key starting from a key" in {
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

  "iterBackward and iterPrev" should "work with a key" in {
    testBackwardIterator(
      keys = Array("ab"),
      expectedKeys = Array( "ab" )
    )
  }

  it should "work with two keys" in {
    testBackwardIterator(
      keys = Array("cd", "ab"),
      expectedKeys = Array( "cd", "ab" )
    )
  }


  it should "work with three keys" in {
    testBackwardIterator(
      keys = Array("ef", "cd", "ab" ),
      expectedKeys = Array( "ef", "cd", "ab" )
    )
  }

  it should "work with a key starting from a key" in {
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

  "iterPrev after iterForward" should "throw an exception" in {
    // without specifying a search key
    {
      put("ab")
      val iter = SortedArrayIterator()
      sarray.iterForward(iter)
      a [java.lang.IllegalStateException] should be thrownBy( sarray.iterPrev(iter) )
    }

    // with a search key
    {
      put("ab")
      val iter = SortedArrayIterator()
      sarray.iterForward(iter, Arr("ab"))
      a [java.lang.IllegalStateException] should be thrownBy( sarray.iterPrev(iter) )
    }
  }

  "iterNext after iterBackward" should "throw an exception" in {
    // without specifying a search key
    {
      put("ab")
      val iter = SortedArrayIterator()
      sarray.iterBackward(iter)
      a [java.lang.IllegalStateException] should be thrownBy( sarray.iterNext(iter) )
    }

    // with a search key
    {
      put("ab")
      val iter = SortedArrayIterator()
      sarray.iterBackward(iter, Arr("ab"))
      a [java.lang.IllegalStateException] should be thrownBy( sarray.iterNext(iter) )
    }
  }
}
