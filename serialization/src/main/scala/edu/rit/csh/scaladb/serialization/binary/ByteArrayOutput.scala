package edu.rit.csh.scaladb.serialization.binary

class ByteArrayOutput(size: Int) extends BinaryOutput {

  private val buffer = new Array[Byte](size)
  private var index = 0

  def output: Array[Byte] = buffer

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    System.arraycopy(b, off, output, index, len)
    index += len
  }

  override def write(i: Int): Unit = {
    buffer(index) = i.toByte
    index += 1
  }

  override def toString: String = s"ByteArrayOutput of size: $size, current position: $index"
}