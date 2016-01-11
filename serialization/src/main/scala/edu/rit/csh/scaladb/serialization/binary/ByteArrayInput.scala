package edu.rit.csh.scaladb.serialization.binary

import edu.rit.csh.scaladb.serialization.Input

/**
 * Reads input from a binary format and returns integers for each byte
 * @param buffer the underlying array to pull from
 */
class ByteArrayInput(buffer: Array[Byte]) extends Input[Int] {

  private var index = 0

  override def next(): Int = {
    val i = buffer(index)
    index += 1
    i
  }

  def deserialize[T](implicit ser: BinarySerializer[T]): T = ser.read(this)

  override def read(): Int = next()

}
