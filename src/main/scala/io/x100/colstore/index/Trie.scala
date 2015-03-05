package io.x100.colstore.index


object TrieNode {
  def isNibble(value : Byte) = (value & 0xf0) == 0
}

import TrieNode._

import scala.reflect.ClassTag

/**
 * A TrieNode. It is either an internal node with children or a leaf node without it.
 */
class TrieNode[ValueType](var value : Option[ValueType], var children : SortedArray[TrieNode[ValueType]] = new SortedArray[TrieNode[ValueType]](keySpaceSize=16, keyLength = 1))(implicit m : ClassTag[ValueType]) {
  // Let's store each nibble on a byte. We will compact the size when we serialize it on a byte[] later.
  // We will have up to 16 nibbles per node, because a nibble can have value from 0 to 15.

  /**
   * Put a key(=nibble array) onto the trie. Return a value if one was associated with the key already.
   * @param key the key to put
   * @param offset the offset on the key
   * @param value the value we are going to associate with the key
   * @return the old option value associated with the key if the key was existing. null otherwise.
   */
  def put( key : Array[Byte], offset: Int, value : Option[ValueType]) : Option[ValueType]= {
    // We don't allow the value to be null. If you want to put null, pass None and Some(value) instead.
    if ( value == null ) throw new IllegalArgumentException()

    val nibble = key(offset)
    // We accept nibbles only. If the nibbleKey is above 15, thrown an exception.
    if ( ! isNibble(nibble) ) throw new IllegalArgumentException()

    // TODO : Optimize to pass the input parameter, nibbleArray, instead of creating a new array.
    val keyArray = Array(nibble)

    var child = if (children == null) {
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
        child = new TrieNode[ValueType](None)
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
}

class Trie[ValueType]()(implicit m : ClassTag[ValueType]) {
  // The root node of the trie. It does not have any option value associated with it.
  val rootNode = new TrieNode[ValueType](value = null)

  /**
   * Put a value with a key. If the key was found, return the previous value associated with the key.
   * @param key The key to put.
   * @param value The value associated with an already existing key. Otherwise, return null.
   */
  def put( key : Array[Byte], value : Option[ValueType] ) : Unit =  {
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

  def serialize() : Array[Byte] = {
    // TODO : Implement it
    assert(false)
    null
  }
}
