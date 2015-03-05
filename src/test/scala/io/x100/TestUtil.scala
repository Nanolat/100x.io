package io.x100

/**
 * Created by unknown on 2/23/15.
 */
object TestUtil {
  /**
   * Convert a string to an array of bytes.
   * @param string the string to convert. This function assumes that the string only contains ascii characters.
   * @return Array[Byte] The converted array of bytes.
   */
  def Arr(string : String) = {
    string.getBytes()
  }

  /**
   * Convert a string to an array of nibbles.
   * @param string the string to convert. This function assumes that the string only contains ascii characters.
   * @return Array[Byte] the converted array of nibbles.
   */
  def NibArr(string : String) : Array[Byte] = {
    string.getBytes().flatMap { b : Byte =>
      val highNibble : Byte = ((b & 0xf0) >> 4).toByte
      val lowNibble : Byte = (b & 0x0f).toByte
      Array( highNibble, lowNibble)
    }
  }

}
