package edu.rit.csh.scaladb.serialization.binary

import java.io.ByteArrayInputStream

import edu.rit.csh.scaladb.serialization.Input


class ByteBufferInput(buffer: ByteArrayInputStream) extends Input[Int] {

  override def next: Int = buffer.read()

  def deserialize[T](implicit ser: BinarySerializer[T]): T = ser.read(this)

  override def read(): Int = buffer.read()

}
