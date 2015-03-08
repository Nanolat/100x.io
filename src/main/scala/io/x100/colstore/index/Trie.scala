package io.x100.colstore.index


object TrieNode {
  def isNibble(value : Byte) = (value & 0xf0) == 0
}

import java.nio._
import java.util
import java.util

import TrieNode._
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream
import io.x100.util.Varint

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

/**
 * A TrieNode. It is either an internal node with children or a leaf node without it.
 */
class TrieNode[ ValueType >: Null ](var value : ValueType, var children : SortedArray[TrieNode[ValueType]] = new SortedArray[TrieNode[ValueType]](keySpaceSize=16, keyLength = 1))(implicit m : ClassTag[ValueType]) {
  trait Visitor {
    def visit(node : TrieNode[ValueType] ) : Unit
  }

  // Let's store each nibble on a byte. We will compact the size when we serialize it on a byte[] later.
  // We will have up to 16 nibbles per node, because a nibble can have value from 0 to 15.

  /**
   * Put a key(=nibble array) onto the trie. Return a value if one was associated with the key already.
   * @param key the key to put
   * @param offset the offset on the key
   * @param value the value we are going to associate with the key
   * @return the old option value associated with the key if the key was existing. null otherwise.
   */
  def put( key : Array[Byte], offset: Int, value : ValueType) : ValueType = {
    // We don't allow the value to be null. If you want to put null, pass None and Some(value) instead.
    if ( value == null ) throw new IllegalArgumentException()

    val nibble = key(offset)
    // We accept nibbles only. If the nibbleKey is above 15, thrown an exception.
    if ( ! isNibble(nibble) ) throw new IllegalArgumentException()

    // TODO : Optimize to pass the input parameter, nibbleArray, instead of creating a new array.
    val keyArray = Array(nibble)

    var child : TrieNode[ValueType] = if (children == null) {
      // This is a leaf node without any child. Let's make the node an internal node to put the new nibble key.
      // Note that an internal node can also have an associated option value.
      children = new SortedArray[TrieNode[ValueType]](keySpaceSize=16, keyLength = 1)
      null // We don't have a child that serves the given nibble key at this point.
    } else {
      children.get( keyArray )
    }

    if ( offset == key.length -1 ) { // Base case : We are processing the last nibble key.
      if (child == null) { // Add a new child if one with the nibble key does not exist yet.
        child = new TrieNode[ValueType](value, children = null) // This is the leaf node without any children.
        children.put( keyArray, child)
        null // Return null, because the nibble key was not found.
      } else {
        val oldValue = child.value
        child.value = value
        oldValue // Return the old value because the nibble key was found.
      }
    } else { // We still have more nibble(s) to process.
      if (child == null) { // Add an internal node with the given nibble key if it does not exist yet.
        child = new TrieNode[ValueType](null)
        children.put( keyArray, child)
      }
      // We have more nibbles on the given key array. Continue to put them on the child.
      child.put(key, offset + 1, value)
    }
  }

  /**
   * Find a trie node that serves a given key.
   * @param key The key that the node we want to find serves
   * @param offset the offset of the key.
   * @return The trie node that serves the given key.
   */
  def servingNode( key : Array[Byte], offset : Int ) : TrieNode[ValueType] = {
    val nibble = key(offset)
    // We accept nibbles only. If the nibbleKey is above 15, thrown an exception.
    if ( ! isNibble(nibble) ) throw new IllegalArgumentException()

    // TODO : Optimize to pass the input parameter, nibbleArray, instead of creating a new array.
    val keyArray = Array(nibble)

    if (children == null) // This is a leaf node. no more children, meaning we don't have the key on the trie.
      null
    else {
      val node = children.get( keyArray )
      if (node == null) {
        null
      } else {
        if ( offset == key.length -1 ) { // If the last one, return the node.
          node
        } else { // Otherwise, keep searching with the next offset.
          node.servingNode( key, offset + 1)
        }
      }
    }
  }

  /** Do visit nodes in post order.
    * This is necessary to visit children first to collect their data
    * such as offset of each child in a continuous byte array.
    *
    * @param visitor The visitor that visits each node in the trie.
    */
  def visitInPostOrder(visitor : TrieNode[ValueType]#Visitor) : Unit = {
    if (children != null ) {
      val iter = new SortedArrayIterator()
      children.iterForward(iter)

      var (key, child) = children.iterNext(iter)
      while( key != null && child != null) {

        child.visitInPostOrder(visitor)

        val (nextKey, nextChild) = children.iterNext(iter)

        key = nextKey
        child = nextChild
      }
    }
    visitor.visit(this)
  }
}

trait ValueSerializer[ValueType >: Null ] {
  def serialize(buffer : ByteBuffer, value : ValueType)
  def deserialize(buffer : ByteBuffer) : ValueType
}

class Trie[ ValueType >: Null ]()(implicit m : ClassTag[ValueType]) {

  // The root node of the trie. It does not have any option value associated with it.
  val rootNode = new TrieNode[ValueType](value = null)

  /**
   * Put a value with a key. If the key was found, return the previous value associated with the key.
   * @param key The key to put.
   * @param value The value associated with an already existing key. Otherwise, return null.
   */
  def put( key : Array[Byte], value : ValueType ) : Unit =  {
    rootNode.put( key, 0, value)
  }

  /**
   * Get a value associated with a given nibble key. Return null if the key was not found.
   * @param nibbleKey
   * @return null if the key was not found. Otherwise return the associated option value.
   */
  def get( nibbleKey : Array[Byte] ) = {
    // Get the serving node.
    val node = rootNode.servingNode(nibbleKey, 0)
    if ( node == null ) // If no node is found, it means we don't have the key.
      null
    else // Found the serving node. Return the option value associated with it.
      node.value
  }

  /**
   * Delete a key on the trie. This does not actually remove any node, but set the option value to null.
   * @param nibbleKey the key to remove
   * @return the option value associated with the given key if the key was found.
   */
  def del( nibbleKey : Array[Byte] ) = {
    // Get the serving node.
    val node = rootNode.servingNode(nibbleKey, 0)
    if ( node == null ) // If no node is found, it means we don't have the key.
      null
    else {
      // Found the serving node. Return the option value associated with it.
      val oldValue = node.value
      node.value = null
      oldValue
    }
  }

  def serialize(buffer : ByteBuffer, valueSerializer : ValueSerializer[ValueType]) = {
    // Data to keep for each node while serializing each node.
    case class NodeKeeping(offset : Int)

    val nodeMap = new HashMap[TrieNode[ValueType], NodeKeeping]()

    // The offset to the root node. First write 0 to it,
    // and then we write the actual offset of the root node at the end of this function.
    buffer.putInt(0)

    // TODO : Use BufferedOutputStream for speed optimization?
    // Serialize data by visiting the trie in post order, which visits children first.
    // By this way, we can get offset of child nodes when we write the parent(current) node.
    rootNode.visitInPostOrder( new rootNode.Visitor() {
      def visit(node : TrieNode[ValueType]): Unit = {

        val nodeOffset = buffer.position()
        nodeMap(node) = NodeKeeping(nodeOffset)

        // Serialize the value first. Note that null is also serialized and deserialized by this function call.
        valueSerializer.serialize(buffer, node.value)

        // Write (key, relative offset) pair for each child.
        // The relative offset of this child node is
        // the offset difference between the current node(parent) and a child.
        if (node.children == null ) {
          // Write number of children.
          buffer.put(0.toByte)
        } else {
          // Write number of children.
          val keyCount = node.children.keyCount
          assert(keyCount < 256)
          buffer.put(keyCount.toByte)

          val iter = new SortedArrayIterator()
          node.children.iterForward(iter)

          var (key, child) = node.children.iterNext(iter)
          while( key != null && child != null) {

            // Write key
            // TODO : Optimize : The key is only a nibble. Write it by consuming only 4 bits.
            assert(key.length == 1)
            buffer.put(key)

            // Write the relative offset of the child.
            val childKeeping = nodeMap(child)
            assert(childKeeping != null)
            val relativeOffset = nodeOffset - childKeeping.offset
            assert( relativeOffset > 0 )
            Varint.writeUnsignedVarInt(buffer, relativeOffset)

            val (nextKey, nextChild) = node.children.iterNext(iter)

            key = nextKey
            child = nextChild
          }
        }
      }
    })
    val writtenBytes = buffer.position()

    val rootNodeKeeping = nodeMap(rootNode)
    assert( rootNodeKeeping != null )
    // Set position to the beginning of the file.
    buffer.position(0)
    buffer.putInt(rootNodeKeeping.offset)

    writtenBytes
  }
}
