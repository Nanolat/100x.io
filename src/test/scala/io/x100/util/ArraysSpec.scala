package io.x100.util

import org.scalatest.{PrivateMethodTester, FlatSpec}
import org.scalatest.matchers.ShouldMatchers




/**
 * Created by unknown on 2/23/15.
 */
class ArraysSpec extends FlatSpec with ShouldMatchers  {

  "Two empty arrays" should "be equal" in {
    Arrays.compare(Array[Byte](), 0, 0, Array[Byte](), 0, 0) should be (0)
  }

  "An empty array " should "be less than a non-empty array" in {
    Arrays.compare(Array[Byte](), 0, 0, Array[Byte]('a'), 0, 1) should be < 0
    Arrays.compare(Array[Byte]('a'), 0, 1, Array[Byte](), 0, 0) should be > 0
  }

  "Two arrays with same length and same data" should "be equal" in {
    Arrays.compare(Array[Byte]('a'), 0, 1, Array[Byte]('a'), 0, 1) should be (0)
    Arrays.compare(Array[Byte]('a', 'b'), 0, 2, Array[Byte]('a','b'), 0, 2) should be (0)
  }

  it should "be equal with offset > 0" in {
    Arrays.compare(Array[Byte](' ', 'a'), 1, 1, Array[Byte](' ', 'a'), 1, 1) should be (0)
    Arrays.compare(Array[Byte](' ', 'a', 'b'), 1, 2, Array[Byte](' ', 'a','b'), 1, 2) should be (0)
  }

  it should "return a negative integer if the first is less than the second" in {
    Arrays.compare(Array[Byte]('a'), 0, 1, Array[Byte]('b'), 0, 1) should be < 0
    Arrays.compare(Array[Byte](' ','a'), 1, 1, Array[Byte](' ','b'), 1, 1) should be < 0
  }

  it should "return a positive integer if the first is less than the second" in {
    Arrays.compare(Array[Byte]('b'), 0, 1, Array[Byte]('a'), 0, 1) should be > 0
    Arrays.compare(Array[Byte](' ','b'), 1, 1, Array[Byte](' ','a'), 1, 1) should be > 0
  }

  "A shorter array that matches the common prefix of a longer one" should "be less than the longer one" in {
    Arrays.compare(Array[Byte]('a'), 0, 1, Array[Byte]('a', 'b'), 0, 2) should be < 0
    Arrays.compare(Array[Byte]('a', 'b'), 0, 2, Array[Byte]('a'), 0, 1) should be > 0
  }

  "A shorter array that is less than the prefix of a longer one" should "be less than the longer one" in {
    Arrays.compare(Array[Byte]('a'), 0, 1, Array[Byte]('c', 'd'), 0, 2) should be < 0
    Arrays.compare(Array[Byte]('c', 'd'), 0, 2, Array[Byte]('a'), 0, 1) should be > 0
  }

  "A shorter array that is greater than the prefix of a longer one" should "be greater than the longer one" in {
    Arrays.compare(Array[Byte]('b'), 0, 1, Array[Byte]('a', 'b'), 0, 2) should be > 0
    Arrays.compare(Array[Byte]('a', 'b'), 0, 2, Array[Byte]('b'), 0, 1) should be < 0
  }

  "A null value for the array" should "throw an exception" in {
    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(null, 0, 0, Array[Byte]('a'), 0, 1)
    }

    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Array[Byte]('a'), 0, 1, null, 0, 0)
    }

    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(null, 0, 0, null, 0, 0)
    }
  }

  "An array with negative offset" should "throw an exception" in {
    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Array[Byte]('a'), -1, 1, Array[Byte]('a'), 0, 1)
    }

    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Array[Byte]('a'), 0, 1, Array[Byte]('a'), -1, 1)

    }
  }

  "An array with offset greater than or equal to the length" should "throw an exception" in {
    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Array[Byte]('a'), 1, 1, Array[Byte]('a'), 0, 0)
    }
    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Array[Byte]('a'), 0, 0, Array[Byte]('a'), 1, 1)
    }
  }


  "An array with negative length" should "throw an exception" in {
    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Array[Byte](), 0, -1, Array[Byte](), 0, 0)
    }

    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Array[Byte](), 0, 0, Array[Byte](), 0, -1)
    }
  }


  "An array with length greater than remaining bytes" should "throw an exception" in {
    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Array[Byte]('a'), 0, 2, Array[Byte]('a'), 0, 1)
    }

    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Array[Byte]('a'), 0, 1, Array[Byte]('a'), 0, 2)
    }

    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Array[Byte]('a', 'b'), 1, 2, Array[Byte]('a', 'b'), 1, 1)
    }

    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Array[Byte]('a', 'b'), 1, 1, Array[Byte]('a', 'b'), 1, 2)
    }
  }

}
