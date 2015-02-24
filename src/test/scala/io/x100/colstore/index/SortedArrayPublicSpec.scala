package io.x100.colstore.index

import org.scalatest.{PrivateMethodTester, FlatSpec}
import org.scalatest.matchers.ShouldMatchers
import io.x100._

import io.x100.TestUtil.Arr

/**
 * Created by unknown on 2/23/15.
 */
class SortedArrayPublicSpec  extends FlatSpec with ShouldMatchers {

  val sarray = new SortedArray(keySpaceSize = 6, keyLength = 2)

  /**********************************************************************************************************/
  /*
    "put" should "be able to put data" in {
      sarray.put( Arr("cd"), new String("cd") )
      sarray.put( Arr("ab"), new String("ab") )
      sarray.put( Arr("ef"), new String("ef") )
    }


    it should "test1" in {
      sarray.put( Arr("x"), new String("x") )

    }

    "get" should "be able to get existing data" in {
      assert( sarray.get( Arr("ab") ).asInstanceOf[String] == "ab" )
      assert( sarray.get( Arr("cd") ).asInstanceOf[String] == "cd" )
      assert( sarray.get( Arr("ef") ).asInstanceOf[String] == "ef" )
    }


    // Dirty cases
    it should "test2" in {
      assert( sarray.get( Arr("xx") ) == null )
    }
  */
  it should "return null for data that does not exist" in {
    assert( sarray.get( Arr("xx") ) == null )
  }

  "get" should "work" in {

  }

  "del" should "work" in {

  }

  "removeMaxKey" should "work" in {

  }

  "split" should "work" in {

  }

  "mergeWith" should "throw the UnsupportedFeature exception" in {
    the [UnsupportedFeature] thrownBy {
      val sarray2 = new SortedArray(keySpaceSize = 6, keyLength = 2)
      sarray.mergeWith(sarray2)
    }
  }

  "isEmpty" should "work" in {

  }

  "isFull" should "work" in {

  }

  "minKey" should "work" in {

  }
}
