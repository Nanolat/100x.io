package io.x100.colstore.index

import io.x100.TestUtil.Arr
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{PrivateMethodTester, FlatSpec, Suite, BeforeAndAfterEach}

/**
 * Created by unknown on 2/23/15.
 */
class SortedArraySearchSpec extends FlatSpec with ShouldMatchers with SortedArrayPrivateTrait {
  val searchForward = PrivateMethod[(Int, Int, Boolean)]('searchForward)
  val searchBackward = PrivateMethod[(Int, Int, Boolean)]('searchBackward)

  case class SearchCase(val searchKey : String, val expectedKeyPos : Int, val expectedDataPos : Int, val expectedKeyFound : Boolean)

  "searchForward" should "find the correct key and data position." in {
    fillKeyData("03", "05", "07")

    // See the comment of searchForward why keys should return these keyPos and dataPos.
    val testCases = Array(
      SearchCase("00", -2, -1, false),
      SearchCase("01", -2, -1, false),
      SearchCase("02", -2, -1, false),
      SearchCase("03",  0,  0, true),
      SearchCase("04",  0,  0, false),
      SearchCase("05",  2,  1, true),
      SearchCase("06",  2,  1, false),
      SearchCase("07",  4,  2, true),
      SearchCase("08",  4,  2, false),
      SearchCase("09",  4,  2, false)
    )

    testCases map { tcase =>
      info(s"searchForward with key : ${tcase.searchKey}")

      val (keyPos, dataPos, keyFound) = sarray invokePrivate searchForward( Arr(tcase.searchKey) )
      assert( keyPos == tcase.expectedKeyPos)
      assert( dataPos == tcase.expectedDataPos)
      assert( keyFound == tcase.expectedKeyFound)
    }
  }

  "searchBackward" should "find the correct key and data position." in {
    fillKeyData("03", "05", "07")

    // See the comment of searchForward why keys should return these keyPos and dataPos.
    val testCases = Array(
      SearchCase("00",  0,  0, false),
      SearchCase("01",  0,  0, false),
      SearchCase("02",  0,  0, false),
      SearchCase("03",  0,  0, true),
      SearchCase("04",  2,  1, false),
      SearchCase("05",  2,  1, true),
      SearchCase("06",  4,  2, false),
      SearchCase("07",  4,  2, true),
      SearchCase("08",  6,  3, false),
      SearchCase("09",  6,  3, false)
    )

    testCases map { tcase =>
      info(s"searchBackward with key : ${tcase.searchKey}")

      val (keyPos, dataPos, keyFound) = sarray invokePrivate searchBackward( Arr(tcase.searchKey) )
      assert( keyPos == tcase.expectedKeyPos)
      assert( dataPos == tcase.expectedDataPos)
      assert( keyFound == tcase.expectedKeyFound)
    }
  }

  /**********************************************************************************************************/
  "findLastLeKey" should "work" in  {
    fillKeyData("03", "05", "07")

    case class FindLastLeKeyCase(val searchKey : String, val expectedData : String, val expectedDataPos : Int)

    val testCases = Array(
      FindLastLeKeyCase("00", null, -1),
      FindLastLeKeyCase("01", null, -1),
      FindLastLeKeyCase("02", null, -1),
      FindLastLeKeyCase("03", "03", 0),
      FindLastLeKeyCase("04", "03", 0),
      FindLastLeKeyCase("05", "05", 1),
      FindLastLeKeyCase("06", "05", 1),
      FindLastLeKeyCase("07", "07", 2),
      FindLastLeKeyCase("08", "07", 2),
      FindLastLeKeyCase("09", "07", 2)
    )

    testCases map { tcase =>
      info(s"findLastLeKey with key : ${tcase.searchKey}")

      val (data, dataPos) = sarray.findLastLeKey( Arr(tcase.searchKey) )
      assert( data == tcase.expectedData)
      assert( dataPos == tcase.expectedDataPos)
    }
  }
}
