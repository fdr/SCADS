package edu.berkeley.cs.scads.util

object Hash {
  val checksum = new java.util.zip.CRC32
  val adler = new java.util.zip.Adler32

  def hashCode(data: Array[Byte]) = {
    java.util.Arrays.hashCode(data)
  }
  def hashCRC32(data: Array[Byte]) = {
    checksum.reset
    checksum.update(data)
    checksum.getValue
  }
  def hashAdler32(data: Array[Byte]) = {
    adler.reset
    adler.update(data)
    adler.getValue
  }
  // Only MurmurHash2 seems to produce real hash results.
  // translated from: http://www.getopt.org/murmur/MurmurHash.java
  def hashMurmur2(data: Array[Byte]) = {
    val seed = 1
    val m = 0x5bd1e995L
    val r = 24

    var h:Long = seed ^ data.length

    val len = data.length
    val len_4 = len >> 2

    for (i <- (0 until len_4)) {
      val i_4 = i << 2
      var k = data(i_4 + 3) & 0xffL
      k = k << 8
      k = k | (data(i_4 + 2) & 0xffL)
      k = k << 8
      k = k | (data(i_4 + 1) & 0xffL)
      k = k << 8
      k = k | (data(i_4 + 0) & 0xffL)
      k *= m
      k ^= k >>> r
      k *= m
      h *= m
      h ^= k
    }

    val len_m = len_4 << 2
    val left = len - len_m

    if (left != 0) {
      if (left >= 3) {
        h ^= (data(len - 3) & 0xffL) << 16
      }
      if (left >= 2) {
        h ^= (data(len - 2) & 0xffL) << 8
      }
      if (left >= 1) {
        h ^= data(len - 1) & 0xffL
      }
      h *= m
    }

    h ^= h >>> 13
    h *= m
    h ^= h >>> 15

    h & 0xffffffffL
  }

}
