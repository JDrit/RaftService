package edu.rit.csh.scaladb.serialization.binary

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import edu.rit.csh.scaladb.serialization.Output

/**
 * Writes out the binary data to an underlying byte buffer
 */
class ByteArrayOutput extends Output[Array[Byte], Int] {
  //private val buffer = new ByteArrayOutputStream()
  //private val buffer = ByteBuffer.allocate(100 * 1000)
  private val buffer = new ByteBufferedOutputStream(30, true)

  //def output: Array[Byte] = buffer.toByteArray
  def output: Array[Byte] = buffer.array()

  //override def write(b: Array[Byte], off: Int, len: Int): Unit = buffer.write(b, off, len)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = buffer.write(b, off, len)

  def serialize[T](elem: T)(implicit ser: BinarySerializer[T]): Unit = ser.write(elem, this)

  override def write(i: Int): Unit = buffer.write(i)
}
