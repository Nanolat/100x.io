package io.x100.colstore.index

/**
 * Created by unknown on 2/24/15.
 */
import io.x100.TestUtil._
import org.scalatest.PrivateMethodTester.PrivateMethod
import org.scalatest.matchers.ShouldMatchers
import org.scalatest._

trait TrieTestTrait extends BeforeAndAfterEach with PrivateMethodTester with ShouldMatchers {
  this: Suite =>
  val keySpaceSize = 16
  val keyLength = 1
  var trie: Trie[String] = null

  val t = newTrie // A dummy tree for creating a leaf node and an internal node.

  def newTrie = new Trie[String]()

  override def beforeEach() {
    // set-up code
    //
    trie = newTrie

    super.beforeEach()
  }

  override def afterEach() {
    super.beforeEach()
    // tear-down code
    //
    trie = null
  }

  /** ********************************************************************************************************/
  def put(key : String): Unit = {
    trie.put( NibArr(key), Some(key) )
  }

  def get(key : String) = {
    trie.get( NibArr(key) )
  }

  def del(key : String) = {
    trie.del( NibArr(key) )
  }

  // convert an integer ranging from 0 to 255 to a key with chars. The key has hex representation of the integer value.
  def i2key(i : Int) = "%d" format i

  def putKeys(min : Int, max : Int): Unit = {
    // Generate keys with two characters from a0 to ff.
    (min to max) map { i =>
      val key = i2key(i)
      put( key )
      // To improve test coverage :-)
      //val dummy = tree.toString()
    }
  }

  def assertGetKeys(min : Int, max : Int, filterOption:Option[(Int=>Boolean)] = None ): Unit = {
    // Generate keys with two characters from a0 to ff.
    (min to max) map { i =>
      val key = i2key(i)
      val expectedData = filterOption match {
        // If the filter evaluates to false, it means the key should not exist, meaning expectedData is null.
        case Some(filter) => if (filter(i)) Some(key) else null
        case None => Some(key)
      }

      get( key ) should be (expectedData)
    }
  }

  def delKeys(min : Int, max : Int, filterOption:Option[(Int=>Boolean)] = None): Unit = {
    // Generate keys with two characters from a0 to ff.
    (min to max) map { i =>
      // In case a filter is provided, delete a key only if the filter evaluates to true.
      if ( filterOption match { case Some(filter) => filter(i); case None => true } ) {
        val key = i2key(i)
        del(key)
        //info (s"put($key)")
        //info( tree.toString() )
      }
    }
  }
}