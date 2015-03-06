package io.x100.colstore.index

/**
 * Represents a trie node.
 * For internal nodes, it has keyCount > 0 && keys != null.
 * For leaf nodes, keyCount == 0 && keys == null.
 * @param keyCount The number of keys in this node. A byte in keys array is a key.
 * @param keys The key array. A byte in this array is a key.
 *             Currently only nibble is supported for the key type, so each byte in the key should be less than 16.
 */
case class SerializableTrieNode(keyCount : Int, keys : Array[Byte]) {
  assert( keys.length >= keyCount)
  assert( keyCount >= 0)
  if (keyCount == 0) assert( keys == null )
  if (keyCount > 0) assert( keys != null )
}

/**
 * A serializable trie should implement SerializableTrie trait.
 * It provides information such as the number of total nodes including the leaf node, maximum level, and nodes at each level.
 * The level is zero based index from the root. The level of children of root is 1, etc.
 */
trait SerializableTrie {
  def totalNodes() : Int
  def maxLevel() : Int
  def nodes(level : Int) : Seq[SerializableTrieNode]
}
/**
 * Created by unknown on 3/6/15.
 */
class SerializedTrie {

}
