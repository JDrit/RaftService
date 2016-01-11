package edu.rit.csh.scaladb.serialization.binary

import java.io.ByteArrayOutputStream

import edu.rit.csh.scaladb.serialization.Output

class ByteBufferOutput extends Output[Array[Byte], Int] {
  private val buffer = new ByteArrayOutputStream()

  def output: Array[Byte] = buffer.toByteArray

  def add(b: Int): Unit = buffer.write(b)

  def serialize[T](elem: T)(implicit ser: BinarySerializer[T]): Unit = ser.write(elem, this)

  override def write(i: Int): Unit = buffer.write(i)
}
