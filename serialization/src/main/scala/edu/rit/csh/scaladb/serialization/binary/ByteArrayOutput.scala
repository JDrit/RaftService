package edu.rit.csh.scaladb.serialization.binary

import edu.rit.csh.scaladb.serialization.Output

/**
 * Writes out the binary data to an underlying byte buffer
 */
class ByteArrayOutput extends Output[Array[Byte], Int] {

  private val buffer = new ByteBufferedOutputStream(32)

  def output: Array[Byte] = buffer.array()

  override def write(b: Array[Byte], off: Int, len: Int): Unit = buffer.write(b, off, len)

  override def write(i: Int): Unit = buffer.write(i)

  def serialize[T](elem: T)(implicit ser: BinarySerializer[T]): Unit = ser.write(elem, this)

}
