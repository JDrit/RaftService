package edu.rit.csh.scaladb.raft.serialization

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.nio.ByteBuffer

abstract class Serializer[T] {

  final def read(buffer: ByteBuffer): T = {
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes, 0, bytes.length)
    read(new ByteArrayInputStream(bytes))
  }

  def read(buffer: ByteArrayInputStream): T

  def write(elem: T, buffer: ByteArrayOutputStream): Unit
}

object Serializer {

  def read[S](buff: ByteArrayInputStream)(implicit ser: Serializer[S]): S = ser.read(buff)

  def write[S](elem: S, buf: ByteArrayOutputStream)(implicit ser: Serializer[S]): Unit = ser.write(elem, buf)
}


