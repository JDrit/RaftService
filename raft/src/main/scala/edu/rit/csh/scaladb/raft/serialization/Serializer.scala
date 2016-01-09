package edu.rit.csh.scaladb.raft.serialization

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.nio.ByteBuffer

abstract class Serializer[T] {

  def read(buffer: ByteBuffer): T = {
    // Retrieve bytes between the position and limit
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes, 0, bytes.length)
    read(new ByteArrayInputStream(bytes))
  }

  def read(implicit buffer: ByteArrayInputStream): T

  def write(elem: T)(implicit buffer: ByteArrayOutputStream): Unit

  protected def implRead[S](implicit buff: ByteArrayInputStream, ser: Serializer[S]): S = ser.read

  protected def implWrite[S](elem: S)(implicit ser: Serializer[S], buf: ByteArrayOutputStream): Unit = ser.write(elem)

}


