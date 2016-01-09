package edu.rit.csh.jdb.scaladb.server

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

import edu.rit.csh.scaladb.raft.Command
import edu.rit.csh.scaladb.raft.serialization.CommonSerializers._
import edu.rit.csh.scaladb.raft.serialization.Serializer

object CommandSerializer extends Serializer[Command] {

  def read(buffer: ByteArrayInputStream): Command = {
    val client = Serializer.read[String](buffer)
    val id = Serializer.read[Int](buffer)
    Serializer.read[Int](buffer) match {
      case 0 => Get(client, id, Serializer.read[String](buffer))
      case 1 => Put(client, id, Serializer.read[String](buffer), Serializer.read[String](buffer))
      case 2 => Delete(client, id, Serializer.read[String](buffer))
      case 3 => CAS(client, id, Serializer.read[String](buffer), Serializer.read[String](buffer), Serializer.read[String](buffer))
      case 4 => Append(client, id, Serializer.read[String](buffer), Serializer.read[String](buffer))
    }
  }

  def write(elem: Command, buf: ByteArrayOutputStream): Unit = {
    Serializer.write[String](elem.client, buf)
    Serializer.write[Int](elem.id, buf)
    elem match {
      case Get(_, _, key) =>
        Serializer.write(0, buf)
        Serializer.write(key, buf)
      case Put(_, _, key, value) =>
        Serializer.write(1, buf)
        Serializer.write(key, buf)
        Serializer.write(value, buf)
      case Delete(_, _, key) =>
        Serializer.write(2, buf)
        Serializer.write(key, buf)
      case CAS(_, _, key, cur, newVal) =>
        Serializer.write(3, buf)
        Serializer.write(key, buf)
        Serializer.write(cur, buf)
        Serializer.write(newVal, buf)
      case Append(_, _, key, value) =>
        Serializer.write(4, buf)
        Serializer.write(key, buf)
        Serializer.write(value, buf)
    }
  }
}
