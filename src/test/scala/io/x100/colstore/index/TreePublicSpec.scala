package io.x100.colstore.index

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

/**
 * Created by unknown on 2/24/15.
 */
class TreePublicSpec extends FlatSpec with ShouldMatchers with TreeTestTrait {
  /**********************************************************************************************************/
  "put/get/del" should "work" in {
    (0 to 255) map { testKeyCount =>
      putKeys(0, testKeyCount)
      assertGetKeys(0, testKeyCount)

      def deleteFilter(n : Int) = n % (testKeyCount%7 + 2) == 1
      def assertFilter(n : Int) = ! deleteFilter(n)
      delKeys(0, testKeyCount, Some(deleteFilter) )
      assertGetKeys(0, testKeyCount, Some(assertFilter) )
    }
  }

  "seekForward" should "work" in {

  }

  "seekBackward" should "work" in {

  }

  "moveForward" should "work" in {

  }

  "moveBackward" should "work" in {

  }
}
