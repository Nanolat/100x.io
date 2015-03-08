package io.x100.colstore.index

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.io.RandomAccessFile
import java.io.File
import java.nio.MappedByteBuffer

import io.x100.util.Varint
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

/**
 * Created by unknown on 3/5/15.
 */
class TriePublicSpec extends FlatSpec with ShouldMatchers with TrieTestTrait {
  // Serialize the trie.
  val stringSerializer = new ValueSerializer[String]() {
    def serialize(buffer: ByteBuffer, value: String): Unit = {

      if (value == null) {
        Varint.writeSignedVarInt(buffer, -1)
      } else {
        val bytes = value.getBytes(StandardCharsets.UTF_8)
        Varint.writeSignedVarInt(buffer, bytes.length)
        buffer.put(bytes)
      }
    }

    def deserialize(buffer: ByteBuffer): String = {
      val length = Varint.readSignedVarInt(buffer)
      if (length < 0) {
        null // length less than 0 means null object
      } else {
        val bytes = new Array[Byte](length)
        buffer.get(bytes)
        new String(bytes, StandardCharsets.UTF_8)
      }
    }

    def skip(buffer: ByteBuffer): Unit = {
      val length = Varint.readSignedVarInt(buffer)
      if (length <= 0) {
        // do nothing
      } else {
        buffer.position( buffer.position() + length )
      }
    }
  }

  "put/get/del" should "work" in {

    (0 to 128) map { testKeyCount =>
      putKeys(0, testKeyCount)
      assertGetKeys(0, testKeyCount, None)

      def deleteFilter(n : Int) = n % (testKeyCount%7 + 2) == 1
      def assertFilter(n : Int) = ! deleteFilter(n)

      delKeys(0, testKeyCount, Some(deleteFilter) )
      assertGetKeys(0, testKeyCount, Some(assertFilter) )
    }

    // Serialize the trie
    {
      val f = new File( "./trie.dat");
      f.delete();
      val rafile = new RandomAccessFile(f, "rw")
      val fc = rafile.getChannel();
      val bufferSize = 1024 * 1024
      val buffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize)
      val writtenBytes = trie.serialize(buffer, stringSerializer)
      rafile.setLength(writtenBytes)

      val immutableTrie = ImmutableTrie.createFrom[String](buffer, stringSerializer)

      val testKeyCount = 128
      def deleteFilter(n : Int) = n % (testKeyCount%7 + 2) == 1
      def assertFilter(n : Int) = ! deleteFilter(n)
      assertGetKeys(immutableTrie, 0, testKeyCount, Some(assertFilter) )
    }
  }
}
