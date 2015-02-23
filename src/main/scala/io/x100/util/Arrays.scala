package io.x100.util

/**
 * Created by unknown on 2/20/15.
 */
object Arrays {
  // TODO : Unit tests with same length, different lengths, etc.
  def compare(a : Array[Byte], aOffset : Int, aLength : Int,
              b : Array[Byte], bOffset : Int, bLength : Int): Int = {
    if ( a == null || aOffset < 0 || aLength < 0 || aOffset + aLength > a.length)
      throw new IllegalArgumentException()

    if ( b == null || bOffset < 0 || bLength < 0 || bOffset + bLength > b.length)
      throw new IllegalArgumentException()

    var i = 0

    for (i <- 0 until math.max(aLength, bLength) ) {
      if ( i >= aLength )
        return -1
      if ( i >= bLength )
        return 1

      val aValue = a(aOffset+i)
      val bValue = b(bOffset+i)

      if ( aValue != bValue ) {
        return aValue - bValue
      }
    }
    0
  }
}
