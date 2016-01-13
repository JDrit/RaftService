package edu.rit.csh.scaladb.serialization.binary

import edu.rit.csh.scaladb.serialization.Input

/**
 * Reads input from a binary format and returns integers for each byte
 * @param buffer the underlying array to pull from
 */
class ByteArrayInput(val buffer: Array[Byte]) extends Input[Byte] {

  var index = 0

  override def read(): Int = {
    val i = buffer(index)
    index += 1
    i
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    var count = 0
    var i = off
    while (i < len) {
      b(i) = read().toByte
      i += 1
      count += 1
    }
    count
  }

  def deserialize[T](implicit ser: BinarySerializer[T]): T = ser.read(this)

}
