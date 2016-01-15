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
    System.arraycopy(buffer, index, b, off, len)
    index += len - off
    len - off
  }
}
