package edu.rit.csh.jdb.scaladb.server

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

import edu.rit.csh.scaladb.serialization.{CommonSerializers, Serializer}
import edu.rit.csh.scaladb.raft.Command


object CommandSerializer extends Serializer[Command] {
  import CommonSerializers._

  private val getSer = Serializer.materializeSerializer[Get]
  private val putSer = Serializer.materializeSerializer[Put]
  private val deleteSer = Serializer.materializeSerializer[Delete]
  private val casSer = Serializer.materializeSerializer[CAS]
  private val appendSer = Serializer.materializeSerializer[Append]

  def read(buffer: ByteArrayInputStream): Command = {
    Serializer.read[Byte](buffer) match {
      case 0 => getSer.read(buffer)
      case 1 => putSer.read(buffer)
      case 2 => deleteSer.read(buffer)
      case 3 => casSer.read(buffer)
      case 4 => appendSer.read(buffer)
    }
  }

  def write(elem: Command, buffer: ByteArrayOutputStream): Unit = elem match {
    case g: Get =>
      Serializer.write[Byte](0, buffer)
      getSer.write(g, buffer)
    case p: Put =>
      Serializer.write[Byte](1, buffer)
      putSer.write(p, buffer)
    case d: Delete =>
      Serializer.write[Byte](2, buffer)
      deleteSer.write(d, buffer)
    case c: CAS =>
      Serializer.write[Byte](3, buffer)
      casSer.write(c, buffer)
    case a: Append =>
      Serializer.write[Byte](4, buffer)
      appendSer.write(a, buffer)
  }
}
