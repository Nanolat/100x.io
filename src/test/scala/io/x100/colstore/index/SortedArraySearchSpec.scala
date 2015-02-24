package io.x100.colstore.index

import io.x100.TestUtil.Arr
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{PrivateMethodTester, FlatSpec, Suite, BeforeAndAfterEach}

trait SortedArrayForSearch extends BeforeAndAfterEach { this : Suite =>
  val keySpaceSize = 6
  val keyLength = 2
  var sarray : SortedArray = null

  override def beforeEach() {
    // set-up code
    //
    sarray = new SortedArray( keySpaceSize, keyLength );

    // We will put 03, 05, 07 into the sorted array
    // to test if keys from 01 to 09 are all searched correctly as shown on the comment of searchForward and searchBackward function.
    sarray.put(Arr("05"), new Integer(5))
//    sarray.put(Arr("03"), new Integer(3))
//    sarray.put(Arr("07"), new Integer(7))

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
class SortedArraySearchSpec extends FlatSpec with ShouldMatchers with PrivateMethodTester with SortedArrayForSearch {
  val searchForward = PrivateMethod[(Int, Int, Boolean)]('searchForward)
  val searchBackward = PrivateMethod[(Int, Int, Boolean)]('searchBackward)

  case class TestCase(val searchKey : String, val expectedKeyPos : Int, val expectedDataPos : Int, val expectedKeyFound : Boolean)
/*
  "searchForward" should "find the correct key and data position." in {
    // See the comment of searchForward why keys should return these keyPos and dataPos.
    val testCases = Array(
      TestCase("00", -2, -1, false),
      TestCase("01", -2, -1, false),
      TestCase("02", -2, -1, false),
      TestCase("03",  0,  0, true),
      TestCase("04",  0,  0, false),
      TestCase("05",  2,  1, true),
      TestCase("06",  2,  1, false),
      TestCase("07",  4,  2, true),
      TestCase("08",  4,  2, false),
      TestCase("09",  4,  2, false)
    )

    testCases map { tcase =>
      val (keyPos, dataPos, keyFound) = sarray invokePrivate searchForward( tcase.searchKey )
      assert( keyPos == tcase.expectedKeyPos)
      assert( dataPos == tcase.expectedDataPos)
      assert( keyFound == tcase.expectedKeyFound)
    }
  }


  "searchBackward" should "find the correct key and data position." in {
    // See the comment of searchForward why keys should return these keyPos and dataPos.
    val testCases = Array(
      TestCase("00",  0,  0, false),
      TestCase("01",  0,  0, false),
      TestCase("02",  0,  0, false),
      TestCase("03",  0,  0, true),
      TestCase("04",  2,  1, false),
      TestCase("05",  2,  1, true),
      TestCase("06",  4,  2, false),
      TestCase("07",  4,  2, true),
      TestCase("08",  6,  3, false),
      TestCase("09",  6,  3, false)
    )

    testCases map { tcase =>
      val (keyPos, dataPos, keyFound) = sarray invokePrivate searchBackward( tcase.searchKey )
      assert( keyPos == tcase.expectedKeyPos)
      assert( dataPos == tcase.expectedDataPos)
      assert( keyFound == tcase.expectedKeyFound)
    }
  }
*/

  /**********************************************************************************************************/
  "findLastLeKey" should "work" in  {

  }

}
