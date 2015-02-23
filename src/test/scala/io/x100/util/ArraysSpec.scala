package io.x100.util

import org.scalatest.{PrivateMethodTester, FlatSpec}
import org.scalatest.matchers.ShouldMatchers

import io.x100.TestUtil.Arr

/**
 * Created by unknown on 2/23/15.
 */
class ArraysSpec extends FlatSpec with ShouldMatchers  {

  "Two empty arrays" should "be equal" in {
    Arrays.compare(Arr(""), 0, 0, Arr(""), 0, 0) should be (0)
  }

  "An empty array " should "be less than a non-empty array" in {
    Arrays.compare(Arr(""), 0, 0, Arr("a"), 0, 1) should be < 0
    Arrays.compare(Arr("a"), 0, 1, Arr(""), 0, 0) should be > 0
  }

  "Two arrays with same length and same data" should "be equal" in {
    Arrays.compare(Arr("a"), 0, 1, Arr("a"), 0, 1) should be (0)
    Arrays.compare(Arr("ab"), 0, 2, Arr("ab"), 0, 2) should be (0)
  }

  it should "be equal with offset > 0" in {
    Arrays.compare(Arr("_a"), 1, 1, Arr("_a"), 1, 1) should be (0)
    Arrays.compare(Arr("_ab"), 1, 2, Arr("_ab"), 1, 2) should be (0)
  }

  it should "return a negative integer if the first is less than the second" in {
    Arrays.compare(Arr("a"), 0, 1, Arr("b"), 0, 1) should be < 0
    Arrays.compare(Arr("_a"), 1, 1, Arr("_b"), 1, 1) should be < 0
  }

  it should "return a positive integer if the first is less than the second" in {
    Arrays.compare(Arr("b"), 0, 1, Arr("a"), 0, 1) should be > 0
    Arrays.compare(Arr("_b"), 1, 1, Arr("_a"), 1, 1) should be > 0
  }

  "A shorter array that matches the common prefix of a longer one" should "be less than the longer one" in {
    Arrays.compare(Arr("a"), 0, 1, Arr("ab"), 0, 2) should be < 0
    Arrays.compare(Arr("ab"), 0, 2, Arr("a"), 0, 1) should be > 0
  }

  "A shorter array that is less than the prefix of a longer one" should "be less than the longer one" in {
    Arrays.compare(Arr("a"), 0, 1, Arr("cd"), 0, 2) should be < 0
    Arrays.compare(Arr("cd"), 0, 2, Arr("a"), 0, 1) should be > 0
  }

  "A shorter array that is greater than the prefix of a longer one" should "be greater than the longer one" in {
    Arrays.compare(Arr("b"), 0, 1, Arr("ab"), 0, 2) should be > 0
    Arrays.compare(Arr("ab"), 0, 2, Arr("b"), 0, 1) should be < 0
  }

  "A null value for the array" should "throw an exception" in {
    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(null, 0, 0, Arr("a"), 0, 1)
    }

    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Arr("a"), 0, 1, null, 0, 0)
    }

    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(null, 0, 0, null, 0, 0)
    }
  }

  "An array with negative offset" should "throw an exception" in {
    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Arr("a"), -1, 1, Arr("a"), 0, 1)
    }

    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Arr("a"), 0, 1, Arr("a"), -1, 1)

    }
  }

  "An array with offset greater than or equal to the length" should "throw an exception" in {
    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Arr("a"), 1, 1, Arr("a"), 0, 0)
    }
    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Arr("a"), 0, 0, Arr("a"), 1, 1)
    }
  }


  "An array with negative length" should "throw an exception" in {
    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Arr(""), 0, -1, Arr(""), 0, 0)
    }

    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Arr(""), 0, 0, Arr(""), 0, -1)
    }
  }


  "An array with length greater than remaining bytes" should "throw an exception" in {
    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Arr("a"), 0, 2, Arr("a"), 0, 1)
    }

    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Arr("a"), 0, 1, Arr("a"), 0, 2)
    }

    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Arr("ab"), 1, 2, Arr("ab"), 1, 1)
    }

    the [java.lang.IllegalArgumentException] thrownBy  {
      Arrays.compare(Arr("ab"), 1, 1, Arr("ab"), 1, 2)
    }
  }

}
