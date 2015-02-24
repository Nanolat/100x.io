package io.x100.colstore.index

/**
 * Created by unknown on 2/24/15.
 */
import io.x100.TestUtil._
import org.scalatest.PrivateMethodTester.PrivateMethod
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{Suite, PrivateMethodTester, BeforeAndAfterEach}

trait TreeTestTrait extends BeforeAndAfterEach with PrivateMethodTester with ShouldMatchers {
  this: Suite =>
  val keySpaceSize = 6
  val keyLength = 2
  var tree: VersionedTree = null

  var inode : VersionedTree#InternalNode = null
  var lnode : VersionedTree#LeafNode = null
  val t = newTree // A dummy tree for creating a leaf node and an internal node.

  def newTree = new VersionedTree(keySpaceSize, keyLength)
  override def beforeEach() {
    // set-up code
    //
    tree = newTree
    inode = new t.InternalNode()
    lnode = new t.LeafNode()

    super.beforeEach()
  }

  override def afterEach() {
    super.beforeEach()
    // tear-down code
    //
    tree = null
    inode = null
    lnode = null
  }

  /** ********************************************************************************************************/
  // Private methods
  val compareKeys = PrivateMethod[Int]('compareKeys)
  val findLeafNode = PrivateMethod[VersionedTree#LeafNode]('findLeafNode)
  val putToInternalNode = PrivateMethod[Unit]('putToInternalNode)
  val putToLeafNode = PrivateMethod[Unit]('putToLeafNode)
  val delFromLeafNode = PrivateMethod[Int]('delFromLeafNode)

  /** ********************************************************************************************************/
  def put(key : String): Unit = {
    tree.put( Arr(key), key )
  }

  def get(key : String) = {
    tree.get( Arr(key) )
  }

  def del(key : String) = {
    tree.del( Arr(key) )
  }

  // convert an integer ranging from 0 to 255 to a key with to chars. The key has hex representation of the integer value.
  def i2key(i : Int) = "%02x" format i

  def putKeys(min : Int, max : Int): Unit = {
    // Generate keys with two characters from a0 to ff.
    (min to max) map { i =>
      val key = i2key(i)
      put( key )
      //info (s"put($key)")
      //info( tree.toString() )
    }
  }

  def assertGetKeys(min : Int, max : Int, filterOption:Option[(Int=>Boolean)] = None ): Unit = {
    // Generate keys with two characters from a0 to ff.
    (min to max) map { i =>
      val key = i2key(i)
      val expectedData = filterOption match {
        // If the filter evaluates to false, it means the key should not exist, meaning expectedData is null.
        case Some(filter) => if (filter(i)) key else null
        case None => key
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

  def assertForwardIterator(expectedKeys : Array[String], findingKey : String): Unit = {
    // Initialize iterator
    val iter = VersionedTreeIterator()
    assert(iter != null)

    tree.seekForward(iter, Arr(findingKey) )

    // Check (key,value) pair for each iteration.
    expectedKeys.map {
      expectedKey => {
        tree.moveForward(iter) match {
          case (key, data) => {
            key should be ( Arr(expectedKey) )
            val expectedValue = expectedKey
            data should be (expectedValue)
          }
        }
      }
    }

    // If we iterate once more, both key and value should be null
    tree.moveForward(iter) match {
      case (key, data) => {
        key should be (null)
        data should be (null)
      }
    }

  }

  // Even though findingKey always has some value, use Option type to reuse iteration test codes of SortedArray.
  def testForwardIterator(keys : Array[String], expectedKeys : Array[String], findingKey : Option[String]): Unit = {
    tree = newTree

    keys.map { key =>
      put(key)
    }

    assertForwardIterator(expectedKeys, findingKey.get)
  }

  def assertBackwardIterator(expectedKeys : Array[String], findingKey : String): Unit = {
    // Initialize iterator
    val iter = VersionedTreeIterator()
    assert(iter != null)

    tree.seekBackward(iter, Arr(findingKey) )

    // Check (key,value) pair for each iteration.
    expectedKeys.map {
      expectedKey => {
        tree.moveBackward(iter) match {
          case (key, data) => {
            key should be ( Arr(expectedKey) )
            val expectedValue = expectedKey
            data should be (expectedValue)
          }
        }
      }
    }

    // If we iterate once more, both key and value should be null
    tree.moveBackward(iter) match {
      case (key, data) => {
        key should be (null)
        data should be (null)
      }
    }
  }

    // Even though findingKey always has some value, use Option type to reuse iteration test codes of SortedArray.
  def testBackwardIterator(keys : Array[String], expectedKeys : Array[String], findingKey : Option[String]): Unit = {
    tree = newTree

    keys.map { key =>
      put(key)
    }

    assertBackwardIterator(expectedKeys, findingKey.get)
  }

  def assertForwardIteratorByEachKey(min:Int, max:Int, filterFunc:(Int=>Boolean) = (_:Int)=>(true) ) : Unit = {
    (min to max) map { i =>
      val findingKey = i2key(i)
      // We should see values only if the filter function evaluates to true.
      val expectedKeys = (i to max).filter(filterFunc).map{ i2key(_) }.toArray
      assertForwardIterator( expectedKeys, findingKey)
    }
  }

  def assertBackwardIteratorByEachKey(min:Int, max:Int, filterFunc:(Int=>Boolean) = (_:Int)=>(true) ) : Unit = {
    (min to max) map { i =>
      val findingKey = i2key(i)
      // We should see values only if the filter function evaluates to true.
      val expectedKeys = (i to min by -1 ).filter(filterFunc).map( i2key(_) ).toArray
      assertBackwardIterator( expectedKeys, findingKey)
    }
  }
}