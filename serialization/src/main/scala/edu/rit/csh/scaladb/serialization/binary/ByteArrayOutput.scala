package edu.rit.csh.scaladb.serialization.binary

import edu.rit.csh.scaladb.serialization.Output

/**
 * Writes out the binary data to an underlying byte buffer
 */
class ByteArrayOutput(size: Int) extends Output[Array[Byte], Int] {

  private val buffer = new Array[Byte](size)
  var index = 0

  def output: Array[Byte] = buffer

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    var i = off
    while (i < len) {
      buffer(index) = b(i)
      i += 1
      index += 1
    }
  }

  override def write(i: Int): Unit = {
    buffer(index) = i.toByte
    index += 1
  }
}
