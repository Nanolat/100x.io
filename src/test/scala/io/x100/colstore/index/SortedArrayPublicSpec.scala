package io.x100.colstore.index

import org.scalatest.{Suite, BeforeAndAfterEach, PrivateMethodTester, FlatSpec}
import org.scalatest.matchers.ShouldMatchers
import io.x100._

import io.x100.TestUtil.Arr

trait SortedArrayInstanceTrait extends BeforeAndAfterEach {
  this: Suite =>
  val keySpaceSize = 6
  val keyLength = 2
  var sarray: SortedArray = null

  def newArray = new SortedArray(keySpaceSize = 6, keyLength = 2)

  override def beforeEach() {
    // set-up code
    //
    sarray = newArray

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

  "put" should "replace the existing data" in {
    put("ab")
    get("ab") should be ("ab")

    (1 to 10).map { i =>
      val data = s"new-$i"
      put("ab", data)
      get("ab") should be (data)
    }
  }

  "del" should "return null if the key does not exist" in {
    put("ab")
    put("cd")
    put("ef")
    del("ax") should be (null)
    del("cx") should be (null)
    del("ex") should be (null)
  }

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

  "split" should "work with two keys" in {
    put("ab")
    put("cd")
    assertStatus(sarray, 2, isEmpty=false, isFull=false, "ab")

    val rightArray = newArray
    sarray.split(rightArray)
    assertStatus(sarray, 1, isEmpty=false, isFull=false, "ab")
    assertStatus(rightArray, 1, isEmpty=false, isFull=false, "cd")
  }

  "split" should "work with three keys" in {
    put("ab")
    put("cd")
    put("ef")
    assertStatus(sarray, 3, isEmpty=false, isFull=true, "ab")

    val rightArray = newArray
    sarray.split(rightArray)
    assertStatus(sarray, 2, isEmpty=false, isFull=false, "ab")
    assertStatus(rightArray, 1, isEmpty=false, isFull=false, "ef")
  }

  "split" should "hit an assertion with a key" in {
    put("ab")
    val rightArray = newArray
    a [java.lang.AssertionError] should be thrownBy ( sarray.split(rightArray) )
  }

  "mergeWith" should "throw the UnsupportedFeature exception" in {
    the [UnsupportedFeature] thrownBy {
      val sarray2 = new SortedArray(keySpaceSize = 6, keyLength = 2)
      sarray.mergeWith(sarray2)
    }
  }
}
