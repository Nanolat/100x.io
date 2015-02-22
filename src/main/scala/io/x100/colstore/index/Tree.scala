package io.x100.colstore.index

import io.x100.UnsupportedFeature
import io.x100.colstore.Types._

/**
 * Created by unknown on 2/20/15.
 */
object MagicValue extends Enumeration {
  type MagicValue = Value
  val INTERNAL_NODE_MAGIC=Value(0xcafecafe)
  val LEAF_NODE_MAGIC=Value(0xcafebebe)
}
import MagicValue._

class VersionedTree(keySpaceSize : Int, keyLength : Int) {

  var rootNode: InternalNode = NodeFactory.newInternalNode(keyLength)

  rootNode.leftChild = NodeFactory.newLeafNode(keyLength)

  class Node(val magic: MagicValue) {

    var keyCount: Long = 0L
    var parent: Node = null

    def isInternalNode() = {
      magic == INTERNAL_NODE_MAGIC
    }

    def isLeafNode() = {
      magic == LEAF_NODE_MAGIC
    }
  }

  class LeafNode(keyLength: KeyLength) extends Node(LEAF_NODE_MAGIC) {
    assert(isLeafNode())
    assert(keyLength > 0)
    assert(keySpaceSize / keyLength >= 2)

    // The sorted keys in an array. Each key has a mapping from a key to pointer of the value.
    val keysWithValues = new SortedArray(keySpaceSize, keyLength)

    // The next sibling node
    var next: LeafNode = null
    // The previous sibling node
    var prev: LeafNode = null

    def isEmpty() = keysWithValues.isEmpty()

    def isFull() = keysWithValues.isFull()

    def minKey() = keysWithValues.minKey()

    /**
     * Put a key with a value.
     * @param key The key to put
     * @param value The value to assiciate with the key
     * @return true if an existing key was replaced. false otherwise.
     */
    def put(key: Array[Byte], value: AnyRef) = {
      assert( key != null )
      assert( value != null )


      val keyCountBeforePut = keysWithValues.keyCount

      keysWithValues.put(key, value)

      val isReplaced = keyCountBeforePut == keysWithValues.keyCount

      isReplaced
    }

    def get(key: Array[Byte]) = {
      keysWithValues.get(key)
    }

    def del(key: Array[Byte]): AnyRef = {
      keysWithValues.del(key)
    }

    def mergeWith(node: LeafNode): Unit = {
      throw new UnsupportedFeature();
    }

    def split() = {
      assert( keysWithValues.keyCount >= 2 )

      val rightNode = NodeFactory.newLeafNode( keysWithValues.keyLength )

      // Check link consistency
      assert( this.prev == null || this.prev.next == this )
      assert( this.next == null || this.next.prev == this )

      rightNode.prev = this
      rightNode.next = this.next

      // The parent is not set yet.
      // It can be set after the min key of the new node is put into a parent node.
      // However, we can't guarantee that node->parent is new_node's parent
      // because the node->parent may split while putting the min key of the new node into it.

      if ( this.next != null )
        this.next.prev = rightNode

      rightNode
    }
  }

  class InternalNode(keyLength: KeyLength) extends Node(INTERNAL_NODE_MAGIC) {
    // The sorted keys in an array. Each key has a mapping from a key to pointer of the value.
    var keysWithRightChildren = new SortedArray(keySpaceSize, keyLength)

    // The child node which has keys less than than the first key.
    var leftChild: Node = null

    def isEmpty() = keysWithRightChildren.isEmpty()

    def isFull() = keysWithRightChildren.isFull()

    def minKey() = keysWithRightChildren.minKey()

    def put(key: Array[Byte], node: Node): Unit = {
      keysWithRightChildren.put(key, node)
      node.parent = this
    }

    /** Return the subtree which should serve the given key.
      * Note that the key might not exist in the subtree.
      * Set the root of the subtree to *node.
      *
      * @param key The key should be served by the found node.
      * @return node The found serving node.
      */
    def findServingNodeByKey(key: Array[Byte]) = {
      assert(key != null)

      var (node:Node, nodePosition) = keysWithRightChildren.findLastLeKey(key)

      // keysWithRightChildren does not have the serving node for the key.
      // IOW, all keys in keysWithRightChildren are greater than the given key.
      // In this case, the left child of this node should serve the key in this case.
      if (node == null) {
        assert(nodePosition == -1)
        node = leftChild
      }

      node
    }

    def del(key: Array[Byte]): AnyRef = {
      assert(key != null)

      val node = keysWithRightChildren.del(key).asInstanceOf[Node]

      if (node != null)
        node.parent = null

      node
    }

    def mergeWith(node: InternalNode): Unit = {
      throw new UnsupportedFeature()
    }

    def split() = {
      var rightNode = NodeFactory.newInternalNode( keysWithRightChildren.keyLength )

      // An internal node should have at least two keys before the split operation
      // The original node should have at least three keys.
      // One for the left node, one for the right node, and the last one for the key in between them.
      assert( keysWithRightChildren.keyCount >= 3 )

      // Move half of keys in this node to "node".
      val keysOnRightNode : SortedArray = keysWithRightChildren.split()
      rightNode.keysWithRightChildren = keysOnRightNode

      // Update parent of the children who moved to rightNode.
      {
        val iter = keysOnRightNode.Iterator()
        keysOnRightNode.iterForward(iter)

        var childNode : Node = null
        do {
          val ( _ /* key */, child : Node) = keysOnRightNode.iterNext(iter)
          childNode = child
          if (childNode != null) {
            childNode.parent = rightNode
          }
        } while ( childNode != null)
      }

      // The key in the middle between the two split nodes.
      val (midKey, midKeyNode:Node) = keysWithRightChildren.removeMaxKey()

      // Even after removing the maximum key, the node should have at least a key.
      assert( keysWithRightChildren.keyCount > 0 )

      // Set the node attached with the mid key to the left child of the new split node.
      rightNode.leftChild = midKeyNode

      (rightNode, midKey)
    }
  }

  case class Iterator(var currentNode: LeafNode, var keyIter: SortedArray#Iterator)

  object NodeFactory {
    def newLeafNode(keyLength: KeyLength) = {
      assert(keyLength > 0)
      new LeafNode(keyLength)
    }

    def newInternalNode(keyLength: KeyLength) = {
      assert(keyLength > 0)
      new InternalNode(keyLength)
    }
  }

  private def compareKeys(key1: Array[Byte], key2: Array[Byte]) = {
    io.x100.util.Arrays.compare(key1, 0, key1.length, key2, 0, key2.length)
  }


  /** Find the leaf node that has the given key.
    * @param key the key to find in the tree.
    * @return node the found node which has the key.
    */
  private def findLeafNode(key: Array[Byte]) = {
    assert( key != null )

    var node: LeafNode = null

    var n : Node = rootNode
    while( n.isInternalNode() ) {
      val internalNode : InternalNode = n.asInstanceOf[InternalNode]

      n = internalNode.findServingNodeByKey( key )
    }

    node
  }

  /** Put a key into an internal node. Let the key point to another node.
    * @param node The internal node where the key is put.
    * @param key The key to put into the internal node.
    * @param keyNode The key node that the key will point to.
    */
  private def putToInternalNode(node: InternalNode, key: Array[Byte], keyNode: Node): Unit = {
    assert(node != null)
    assert(key != null)
    assert(keyNode != null)

    if ( node.isFull() )
    {

      // The key in the middle between the original node and the right node.
      // The key was removed from the original node.
      // rightNode does not keep the key, but its leftChild points to the node of the removed key.
      val (rightNode, midKey) = node.split()
      assert( rightNode != null )

      // Modify parent nodes first.
      if ( node.parent != null ) // not a root node
      {
        putToInternalNode( node.parent.asInstanceOf[InternalNode], midKey, rightNode);
      }
      else // root node
      {
        // create a new root node
        val newRootNode = NodeFactory.newInternalNode(keyLength)

        // Set the old root node as the left child of the new root node.
        // node.parent is set to newRootNode by this code.
        newRootNode.leftChild = node
        // Set the right node as the 2nd child of the new root node.
        newRootNode.put(midKey, rightNode)

        // Yes! the new root node is elected!
        rootNode = newRootNode
      }

      val cmp = compareKeys(key, midKey)

      if ( cmp < 0 )
      {
        // put the key into the parent node. This code also sets the keyNode's parent.
        node.put(key, keyNode)
      }
      else if (cmp > 0 )
      {
        // put the key into the parent node. This code also sets the keyNode's parent.
        rightNode.put(key, keyNode)
      }
      else // cmp == 0
      {
        // The midKey is the key in the middle in the node to be split.
        // The key and keyNode is from the child of this node, which is split before this function was called.
        // It does not make sense the key in the middle of the split child node exists in the parent(this) node.
        assert(false);
        throw new IllegalStateException()
      }
    }
    else
    {
      // put the key into the parent node. This code also sets the keyNode's parent.
      node->put(key, keyNode)
    }

  }

  /** Put a key into the given leaf node.
    * @param node The node where the key will be put.
    * @param key The key to put
    * @param value The value to put
    */
  private def putToLeafNode(node: LeafNode, key: Array[Byte], value: AnyRef) {
    assert( node != null )
    assert( key != null )
    assert( value != null )


    if ( node.isFull() )
    {
      val rightNode = node.split();
      assert( rightNode != null )

      val midKey = rightNode.minKey();

      if ( compareKeys(key, midKey) < 0 )
      {
        node.put(key, value);
      }
      else
      {
        rightNode.put(key,value)
      }

      putToInternalNode( node.parent.asInstanceOf[InternalNode], midKey, rightNode )
    }
    else
    {
      node.put(key, value)
    }
  }

  private def delFromLeafNode(node: LeafNode, key: Array[Byte]) = {
    assert(node != null)
    assert(key != null)
    node.del(key)
  }

  def put(key: Array[Byte], value: AnyRef): Unit = {
    assert(key != null)
    assert(value != null)

    val leafNode = findLeafNode(key)
    putToLeafNode(leafNode, key, value)
  }

  /** Get the value and order searching by the given key.
    * Assign the order of the key in this tree to *order only if order is not NULL.
    * @param key The key to get.
    * @return The value associated with the key.
    */
  def get(key: Array[Byte]) = {
    assert(key != null)

    val leafNode = findLeafNode(key)
    val foundValue = leafNode.get(key)
    foundValue
  }

  def del(key: Array[Byte]) = {
    assert(key != null)
    val leafNode = findLeafNode(key)
    val deletedValue = delFromLeafNode(leafNode, key)
    deletedValue
  }

  def seekForward(iter: Iterator, key: Array[Byte]) = {
    assert(iter != null)
    assert(key != null)

    val leafNode = findLeafNode(key)
    iter.currentNode = leafNode

    leafNode.keysWithValues.iterForward(iter.keyIter, key)
  }

  def seekBackward(iter: Iterator, key: Array[Byte]) = {
    assert(iter != null)
    assert(key != null)

    val leafNode = findLeafNode(key)
    iter.currentNode = leafNode
    val keyIyer: SortedArray#Iterator = iter.keyIter;

    leafNode.keysWithValues.iterBackward(keyIyer, key)
  }

  /**
   * Move to the next key on the leaf node of the tree. Follow LeafNode.next if no more keys found on the current one.
   * @param iter The iterator we are working on
   * @return (key, value). key is not null if the key exists to iterate. Otherwise it is null.
   */
  def moveForward(iter: Iterator) = {
    val keysWithValues = iter.currentNode.keysWithValues
    val (key, value) = keysWithValues.iterNext(iter.keyIter)
    if (key == null) {
      // No more keys to iterate
      // Move to next leaf node
      iter.currentNode = iter.currentNode.next

      if (iter.currentNode != null) {
        iter.currentNode.keysWithValues.iterForward(iter.keyIter)
        val (key, value) = iter.currentNode.keysWithValues.iterNext(iter.keyIter)
        (key, value)
      } else {
        // No more keys to iterate
        (null, null)
      }
    } else {
      (key, value)
    }
  }


  /**
   * Move to the previous key on the leaf node of the tree. Follow LeafNode.prev if no more keys found on the current one.
   * @param iter The iterator we are working on
   * @return (key, value). key is not null if the key exists to iterate. Otherwise it is null.
   */
  def moveBackward(iter: Iterator) = {
    val keysWithValues = iter.currentNode.keysWithValues
    val (key, value) = keysWithValues.iterPrev(iter.keyIter)
    if (key == null) {
      // No more keys to iterate
      // Move to next leaf node
      iter.currentNode = iter.currentNode.next

      if (iter.currentNode != null) {
        iter.currentNode.keysWithValues.iterBackward(iter.keyIter)
        val (key, value) = iter.currentNode.keysWithValues.iterPrev(iter.keyIter)
        (key, value)
      } else {
        // No more keys to iterate
        (null, null)
      }
    } else {
      (key, value)
    }
  }
}
