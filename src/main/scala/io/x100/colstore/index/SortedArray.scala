package io.x100.colstore.index


import io.x100.UnsupportedFeature
import io.x100.util.Arrays
import scala.collection.mutable
import io.x100.colstore.Types._

/**
 * Created by unknown on 2/20/15.
 */
class SortedArray(keySpaceSize:Int, var keyLength:KeyLength) {
  assert( keySpaceSize > 0)
  assert( keyLength > 0)
  assert( keySpaceSize > keyLength )


  private val keySpace = new Array[Byte](keySpaceSize)

  assert( keySpaceSize % keyLength == 0 )
  private val dataArray = new Array[AnyRef](keySpaceSize/keyLength)

  var keyCount = 0
  private var usedKeySpace = 0

  private def compareKeys(key : Array[Byte], keySpace : Array[Byte], offset : Int) = {
    assert( keyLength > 0 )
    assert(key != null)
    assert(keySpace != null)
    assert(key.length == keyLength)

    Arrays.compare(key, 0, keyLength, keySpace, offset, keyLength)
  }

  case class Iterator() {
    var keyPos : Int = -1
    var dataPos : Int = -1
  }

  private def nextKey(iter : SortedArray#Iterator): Unit = {
    iter.keyPos += keyLength
    iter.dataPos += 1
  }

  private def prevKey(iter : SortedArray#Iterator): Unit = {
    iter.keyPos -= keyLength
    iter.dataPos -= 1
  }


  // Search forward. For the keys stored in keySpace, the given key will be positioned as follows.
  // Assume : Key size is 2 byte (big endian encoded) integer, and keySpace has three keys, 3,5,and 7.
  // This picture shows the returned keyPos and dataPos for the given input keys from 0.
  //
  // given                   +-------+-------+-------+
  // keySpace =>             |   3   |   5   |   7   |
  //                         +-------+-------+-------+
  //
  // with             +------+-------+-------+-------+
  // key =>           | 0,1,2|  3,4  |  5,6  |7,8,...|
  //                  +------+-------+-------+-------+
  //                      |      |       |       |
  //                      V      V       V       V
  // maps             +------+-------+-------+-------+
  // keyPos =>        |  -1  |   0   |   2   |   4   |
  //                  +------+-------+-------+-------+
  // maps             +------+-------+-------+-------+
  // dataPos =>       |  -1  |   0   |   1   |   2   |
  //                  +------+-------+-------+-------+
  //
  // This function is used in following use cases.
  //
  // (1) internal node for tree traversal
  //     if (keyPos < 0) traverse to the left child.
  //     otherwise get the child node on the data at dataPos.
  //
  // (2) forward cursor on leaf node. Search all keys keys >= KEY )
  //     before returning the first key check followings.
  //     a. if (keyPos < 0) keyPos ++;
  //     b. if (key matches with keySpace(keyPos)) start returning the key from keyPos.
  //        else start returning key from (keyPos + keyLength)
  //
  // (3) put operation - put a key with mapping a data pointer.
  //     b. if keyFound, replace the key/data at the keyPos and dataPos.
  //        else insert the new key at keySpace(keyPos+keyLength), insert the new data at data(dataPos+1)
  //
  // (4) get operation - return data pointer that is mapped with a given key.
  //     a. if (keyPos < 0) return key and data as null meaning that the key was not found.
  //     a. if (keyFound) return data(dataPos)
  //        else return null both for key and data meaning tha the key was not found.
  //
  // (5) del operation
  //     a. if keyFound, remove the key at keySpace(keyPos), remove the data at data(dataPos)
  //
  // TODO : Optimize using binary search.

  private def searchForward(key : Array[Byte]) = {
    assert(key != null)

    var prevCmp : Int = -1 // The result of compareKeys for the previous loop.
    var keyPos = -keyLength
    var dataPos = -1

    var offset = 0
    var cmp = 0
    while ( offset < usedKeySpace && cmp >= 0 ) {
      var cmp = compareKeys(key, keySpace, offset)
      if (cmp >= 0) {
        prevCmp = cmp

        keyPos += keyLength
        dataPos += 1
        offset += keyLength
      }
    }

    // The key comparison of the previous loop : The key matched. This means the key was found in the keySpace
    val keyFound = prevCmp == 0

    (keyPos, dataPos, keyFound)
  }


  // Search backward. For the keys stored in keySpace, the given key will be positioned as follows.
  // Assume : Key size is 2 byte (big endian encoded) integer, and keySpace has three keys, 3,5,and 7.
  // This picture shows the returned keyPos and dataPos for the given input keys from 0.
  //
  // given            +-------+-------+-------+
  // keySpace =>      |   3   |   5   |   7   |
  //                  +-------+-------+-------+
  //
  // with             +-------+-------+-------+-------+
  // key =>           |0,1,2,3|  4,5  |  6,7  |8,9,...|
  //                  +-------+-------+-------+-------+
  //                      |       |       |       |
  //                      V       V       V       V
  // maps             +-------+-------+-------+-------+
  // keyPos =>        |   0   |   2   |   4   |   6   |
  //                  +-------+-------+-------+-------+
  // maps             +-------+-------+-------+-------+
  // dataPos =>       |   0   |   1   |   2   |   3   |
  //                  +-------+-------+-------+-------+
  //
  // This function is used in following use cases.
  //
  // (1) backward cursor on leaf node. Search all keys <= KEY )
  //     before returning the first key check followings.
  //     a. if (keyPos > usedKeySpace ) keyPos -= keyLength;
  //     b. if (KEY matches with keySpace(keyPos)) start returning the key from keyPos.
  //        else start returning key from (keyPos - 1)
  //
  private def searchBackward(key : Array[Byte]) = {
    assert(key != null)
    assert(usedKeySpace >= keyLength)

    var prevCmp : Int = -1 // The result of compareKeys for the previous loop.
    var keyPos = usedKeySpace
    var dataPos = keyCount

    var offset = usedKeySpace - keyLength
    var cmp = 0
    while ( offset >= 0 && cmp <= 0 ) {
      var cmp = compareKeys(key, keySpace, offset)
      if (cmp <= 0) {
        prevCmp = cmp

        keyPos -= keyLength
        dataPos -= 1
        offset -= keyLength
      }
    }

    // The key comparison of the previous loop : The key matched. This means the key was found in the keySpace.
    val keyFound = prevCmp == 0

    (keyPos, dataPos, keyFound)
  }


  private def setData(dataPos : Int, data : AnyRef): Unit = {
    assert( dataPos >=0 )
    assert( data != null )
    dataArray(dataPos) = data
  }

  private def getKey(keyPos : Int) = {
    assert( keyPos >= 0 )
    assert( keyPos + keyLength <= usedKeySpace )

    val key = new Array[Byte](keyLength)
    System.arraycopy(keySpace, keyPos, key, 0, keyLength)

    key
  }

  // Move keys right by the key length from the given key position.
  private def moveKeysRightByTheKeyLengthFrom(keyPos : Int): Unit = {
    assert( keyPos >= 0 )
    assert( keyPos + keyLength < keySpaceSize )

    val shiftSrcOffset = keyPos
    val shiftDestOffset = keyPos + keyLength
    val sizeOfKeysToShift = usedKeySpace - keyPos

    assert( sizeOfKeysToShift > 0)

    // Make sure that the keys to shift are within the keySpace.
    assert( shiftSrcOffset >= 0)
    assert( shiftDestOffset > 0)
    assert( shiftSrcOffset + sizeOfKeysToShift < keySpaceSize )
    assert( shiftDestOffset + sizeOfKeysToShift <= keySpaceSize )

    // Even though the dest and src overlaps, System.arraycopy works well.
    System.arraycopy(keySpace, shiftSrcOffset, keySpace, shiftDestOffset, sizeOfKeysToShift)
  }

  // Move keys left by the key length from the given key position.
  // The key right before the given keyPos is removed.
  private def moveKeysLeftByTheKeyLengthFrom(keyPos : Int): Unit = {
    assert( keyPos >= keyLength )
    assert( keyPos < keySpaceSize )

    val shiftSrcOffset = keyPos
    val shiftDestOffset = keyPos - keyLength
    val sizeOfKeysToShift = usedKeySpace - keyPos

    assert( sizeOfKeysToShift > 0)

    // Make sure that the keys to shift are within the keySpace.
    assert( shiftSrcOffset > 0)
    assert( shiftDestOffset >= 0)
    assert( shiftSrcOffset + sizeOfKeysToShift <= keySpaceSize )
    assert( shiftDestOffset + sizeOfKeysToShift < keySpaceSize )

    // Even though the dest and src overlaps, System.arraycopy works well.
    System.arraycopy(keySpace, shiftSrcOffset, keySpace, shiftDestOffset, sizeOfKeysToShift)
  }

  private def setKeyDataAt( keyPos : Int, dataPos : Int, key : Array[Byte], data : AnyRef): Unit = {
    assert( keyPos >= 0 )
    assert( dataPos > 0 )
    assert( keyPos + keyLength < keySpaceSize )
    assert( key != null )
    assert( data != null )
    assert( key.length == keyLength )

    // copy Key
    System.arraycopy(key, 0, keySpace, keyPos, keyLength)

    // copy data
    dataArray(dataPos) = data
  }

  private def moveDataRightByOneFrom(dataPos : Int): Unit = {
    assert( dataPos >= 0 )
    assert( dataPos + 1 < dataArray.length )

    val shiftSrcOffset = dataPos
    val shiftDestOffset = dataPos + 1
    val countOfDataToShift = keyCount - dataPos
    assert( countOfDataToShift > 0 )

    // Make sure that both the keys to shift are within the keySpace.
    assert( shiftSrcOffset >= 0 )
    assert( shiftDestOffset > 0 )
    assert( shiftSrcOffset + countOfDataToShift < dataArray.length )
    assert( shiftDestOffset + countOfDataToShift <= dataArray.length )

    // Even though the dest and src overlaps, System.arraycopy works well.
    System.arraycopy(dataArray, shiftSrcOffset, dataArray, shiftDestOffset, countOfDataToShift)
  }

  private def moveDataLeftByOneFrom(dataPos : Int): Unit = {
    assert( dataPos > 0 );
    assert( dataPos < dataArray.length );

    val shiftSrcOffset = dataPos
    val shiftDestOffset = dataPos - 1
    val countOfDataToShift = keyCount - dataPos
    assert( countOfDataToShift > 0 )

    // Make sure that both the keys to shift are within the keySpace.
    assert( shiftSrcOffset > 0 )
    assert( shiftDestOffset >= 0 )
    assert( shiftSrcOffset + countOfDataToShift <= dataArray.length )
    assert( shiftDestOffset + countOfDataToShift < dataArray.length )

    // Even though the dest and src overlaps, System.arraycopy works well.
    System.arraycopy(dataArray, shiftSrcOffset, dataArray, shiftDestOffset, countOfDataToShift)
  }

  private def insertKeyDataAt(keyPos : Int, dataPos : Int, key : Array[Byte], data : AnyRef): Unit = {
    assert( keyPos >= 0 )
    assert( dataPos >= 0 )
    assert( keyPos + keyLength <= keySpaceSize )
    assert( dataPos < dataArray.length )
    assert( key != null )
    assert( data != null )

    if (keyPos == usedKeySpace) { // Append at the end of the key_space_
      // No keys to to move
      assert( dataPos == keyCount )
    }
    else
    {
      assert( dataPos < keyCount )
      moveKeysRightByTheKeyLengthFrom( keyPos )
      moveDataRightByOneFrom( dataPos )
    }

    setKeyDataAt(keyPos, dataPos, key, data)

    keyCount += 1
    usedKeySpace += keyLength

    assert( keyCount <= dataArray.length )
    assert( usedKeySpace <= keySpaceSize )
  }

  private def removeKeyDataAt(keyPos : Int, dataPos : Int ): Unit = {
    assert( keyPos >= 0 )
    assert( dataPos >= 0 )
    assert( keyPos + keyLength <= keySpaceSize )
    assert( dataPos < dataArray.length )

    if ( keyPos + keyLength == usedKeySpace )
    {
      // No keys to move
      assert( dataPos + 1 == keyCount )
    }
    else
    {
      assert( dataPos + 1 < keyCount )

      // We have keys and data to move left
      moveKeysLeftByTheKeyLengthFrom( keyPos + keyLength )
      moveDataLeftByOneFrom( dataPos + 1 )
    }

    keyCount -= 1
    usedKeySpace -= keyLength

    assert( keyCount >= 0 )
    assert( usedKeySpace >= 0 )

    assert( keyCount <= dataArray.length )
  }

  def destroy(): Unit = {
    // Do nothing
  }

  def getData(dataPos : Int) = {
    dataArray(dataPos)
  }

  def put(key : Array[Byte], data : AnyRef) : Unit = {
    assert( key != null );
    assert( data != null );
    assert( keyLength > 0 );

    val (keyPos, dataPos, keyFound) = searchForward(key);

    if ( keyFound ) { // The exact key is found. Replace the value.
      setData(dataPos, data);
    } else { // The exact key is not found. Insert the key at the position.
      insertKeyDataAt(keyPos + keyLength, dataPos + 1, key, data);
    }
    assert( keyCount <= dataArray.length )
  }


  def get(key : Array[Byte]) = {

    assert( key != null );

    val (keyPos, dataPos, keyFound) = searchForward(key)

    if (keyFound)
    {
      // The key was found.
      getData(dataPos);
    }
    else
    {
      // We don't have the key.
      null
    }
  }


  /** Set the data of the last less than or equal to the key in keySpace to *data.
   * Set null to data in case the keySpace does not have any key less than or equal to the given key.
   *
   * c.f. "le" means "less than or equal to".
   */
  def findLastLeKey(key : Array[Byte]) = {
    assert( key != null )

    val ( keyPos, dataPos, keyFound ) = searchForward(key)

    val data = if (dataPos < 0) // The first key in the keySpace is greater than the given key.
    {
      null // We don't have the key greater than or equal to the given key. Return null.
    }
    else
    {
      getData(dataPos);
    }

    (data, dataPos)
  }

  def del(key : Array[Byte]) = {
    assert( key != null )

    val (keyPos, dataPos, keyFound) = searchForward(key);

    val data = if (keyFound)
    {
      // The key was found.
      val data = getData(dataPos);
      removeKeyDataAt( keyPos, dataPos )
      data
    }
    else
    {
      null
    }

    assert( keyCount >= 0 )
    assert( keyCount <= dataArray.length )

    data
  }

  def removeMaxKey() = {

    val (key, data) = if ( keyCount > 0 )
    {
      assert( usedKeySpace >= keyLength );

      val maxKeyPos = usedKeySpace - keyLength
      val maxDataPos = keyCount - 1

      val key = getKey( maxKeyPos )
      val data = getData( maxDataPos )

      removeKeyDataAt( maxKeyPos, maxDataPos)

      (key, data)
    }
    else
    {
      ( null, null )
    }

    assert( keyCount >= 0 )
    assert( keyCount <= dataArray.length );

    (key, data)
  }

  def iterForward( iter :  SortedArray#Iterator, key : Array[Byte]): Unit = {
    assert( iter != null )
    assert( key != null )

    val ( keyPos, dataPos, keyFound ) = searchForward( key )

    if (keyFound) {
      iter.keyPos = keyPos
      iter.dataPos = dataPos
    } else { // The key does not match. Move one key forward to locate the first key to iterate.
      // Make sure that the positions did not go beyond the last key. see searchForward.
      assert( keyPos + keyLength <= usedKeySpace )
      assert( dataPos + 1 <= keyCount )
      iter.keyPos = keyPos + keyLength
      iter.dataPos = dataPos + 1
    }
  }

  def iterBackward( iter :  SortedArray#Iterator, key : Array[Byte] ): Unit = {
    assert( iter != null )
    assert( key != null )

    val ( keyPos, dataPos, keyFound ) = searchBackward( key )

    if (keyFound) {
      iter.keyPos = keyPos
      iter.dataPos = dataPos
    } else { // The key does not match. Move one key backward to locate the first key to iterate.
      // Make sure that the positions did not go beyond the first key. see searchBackward.
      assert( keyPos >= 0 )
      assert( dataPos >= 0 )
      iter.keyPos = keyPos - keyLength
      iter.dataPos = dataPos - 1
    }
  }

  // Iterate forward for all keys within the sorted array.
  def iterForward( iter : SortedArray#Iterator ): Unit = {
    assert( iter != null )

    iter.keyPos = 0
    iter.dataPos = 0
  }

  def iterBackward( iter : SortedArray#Iterator ): Unit = {
    assert( iter != null )

    iter.keyPos = usedKeySpace - keyLength
    iter.dataPos = keyCount - 1
  }

  def iterNext( iter : SortedArray#Iterator ) = {
    assert( iter != null )
    assert( iter.keyPos >=0 )
    assert( iter.dataPos >= 0 )

    val (key, data) = if ( iter.keyPos < usedKeySpace )
    {
      assert( iter.dataPos < keyCount );

      val key = getKey(iter.keyPos)
      val data = getData(iter.dataPos)

      nextKey(iter);

      (key, data)
    }
    else
    {
      (null, null)
    }

    (key, data)
  }

  def iterPrev( iter : SortedArray#Iterator ) = {
    assert( iter != null )
    assert( iter.keyPos + keyLength <= usedKeySpace )
    assert( iter.dataPos < keyCount )

    val (key, data) = if ( iter.keyPos >= 0 )
    {
      assert( iter.dataPos >= 0 );

      val key = getKey(iter.keyPos)
      val data = getData(iter.dataPos)

      prevKey(iter);

      (key, data)
    }
    else
    {
      (null, null)
    }

    (key, data)
  }

  def mergeWith( sortedArray : SortedArray ): Unit = {
    throw new UnsupportedFeature()
  }


  def init( newKeyLength : Int,
            newKeyCount : Int,
            newUsedKeySpace : Int,
            srcKeySpace : Array[Byte], srcKeySpaceOffset : Int,
            srcDataArray : Array[AnyRef], srcDataArrayOffset : Int ): Unit = {

    assert( newKeyLength > 0 )
    assert( newKeyCount > 0 )
    assert( newUsedKeySpace > 0 )
    assert( srcKeySpace != null)
    assert( srcDataArray != null)

    assert( newUsedKeySpace / newKeyLength == newKeyCount )

    assert( srcKeySpace.length / newKeyLength == srcDataArray.length )
    // The size of the key space should be same to the existing one.
    assert( keySpaceSize == srcKeySpace.length )

    keyCount = newKeyCount
    usedKeySpace = newUsedKeySpace
    keyLength = newKeyLength

    // copy key and data.
    System.arraycopy( srcKeySpace, srcKeySpaceOffset, keySpace, 0, newUsedKeySpace)
    System.arraycopy( srcDataArray, srcDataArrayOffset, dataArray, 0, newKeyCount)

    assert( keyCount <= dataArray.length )
  }


  def split(rightHalf : SortedArray) = {

    assert( rightHalf != null )
    assert( rightHalf.keyCount == 0 )

    // The split sorted array should have at least two keys.
    assert( keyCount >= 2 )

    // Calculate the new key count on the new right node.
    val rightHalfKeyCount = keyCount >> 1 // key_count_ / 2
    val rightHalfUsedKeySpace = rightHalfKeyCount * keyLength

    // Calculate the new key count on this node.
    val leftHalfKeyCount = keyCount - rightHalfKeyCount
    val leftHalfUsedKeySpace = usedKeySpace - rightHalfUsedKeySpace

    // Calculate the address of keySpace and data from where the rightHalf copies key and data.
    val srcKeySpaceOffset = leftHalfUsedKeySpace
    val srcDataArrayOffset = leftHalfKeyCount

    // Copy the key and data.
    rightHalf.init(
      keyLength,
      rightHalfKeyCount,
      rightHalfUsedKeySpace,
      keySpace,
      srcKeySpaceOffset,
      dataArray,
      srcDataArrayOffset)

    keyCount = leftHalfKeyCount
    usedKeySpace = leftHalfUsedKeySpace

    assert( keyCount > 0 )
    assert( usedKeySpace > 0 )
    assert( keyCount <= dataArray.length )
    assert( usedKeySpace <= keySpaceSize )
  }

  def isEmpty() = usedKeySpace == 0

  def isFull() = usedKeySpace == keySpaceSize

  def minKey() = {
    if ( usedKeySpace == 0 ) {
      null
    } else {
      getKey(0)
    }
  }
}
