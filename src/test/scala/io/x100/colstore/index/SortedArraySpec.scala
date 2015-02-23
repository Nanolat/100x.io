package io.x100.colstore.index

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
class SortedArraySpec extends FlatSpec with ShouldMatchers with SortedArrayInstance with PrivateMethodTester {

  /*
  class SortedArrayFixture( val keySpaceSize : Int, val keyLength : Int) {
    val sarray = new SortedArray( keySpaceSize, keyLength )
  }

  "A sorted array" should "" in new SortedArrayFixture(6,2) {
    assert( sarray.isEmpty() )
  }
  */
  val compareKeys = PrivateMethod[Int]( 'compareKeys )


  "A sorted array" should "hit assertion failure if key count is less than 2" in {
    the [java.lang.AssertionError] thrownBy {
      sarray = new SortedArray( 1, 1 );
    }
  }

  it should "hit assertion failure if the key count of keySpace does not match the one on SortedArray" in {
    the [java.lang.AssertionError] thrownBy {
      sarray invokePrivate compareKeys(
        Array[Byte]('a'),
        Array[Byte]('a','b','c'), 0 ) should be (0)
    }

  }

  "A key comparison" should "compare keys in any position" in {
    sarray = new SortedArray( 2, 1 );

    sarray invokePrivate compareKeys(
      Array[Byte]('a'),
      Array[Byte]('a','b'), 0 ) should be (0)

    sarray invokePrivate compareKeys(
      Array[Byte]('b'),
      Array[Byte]('a','b'), 1 ) should be (0)

    sarray = new SortedArray( 3, 1 );

    sarray invokePrivate compareKeys(
      Array[Byte]('a'),
      Array[Byte]('a','b','c'), 0 ) should be (0)

    sarray invokePrivate compareKeys(
      Array[Byte]('b'),
      Array[Byte]('a','b','c'), 1 ) should be (0)

    sarray invokePrivate compareKeys(
      Array[Byte]('c'),
      Array[Byte]('a','b','c'), 2 ) should be (0)

    sarray = new SortedArray( 6, 2 );

    sarray invokePrivate compareKeys( Array[Byte]('a','b'),
                                      Array[Byte]('a','b','c','d','e','f'), 0 ) should be (0)

    sarray invokePrivate compareKeys( Array[Byte]('c','d'),
                                      Array[Byte]('a','b','c','d','e','f'), 2 ) should be (0)

    sarray invokePrivate compareKeys( Array[Byte]('e','f'),
                                      Array[Byte]('a','b','c','d','e','f'), 4 ) should be (0)

  }

  it should "result in assertion failure with invalid arguments" in {

  }

}
