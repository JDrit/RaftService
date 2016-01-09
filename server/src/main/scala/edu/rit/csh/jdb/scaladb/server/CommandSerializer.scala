package edu.rit.csh.jdb.scaladb.server

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

import edu.rit.csh.scaladb.raft.Command
import edu.rit.csh.scaladb.raft.serialization.CommonSerializers._
import edu.rit.csh.scaladb.raft.serialization.Serializer

object CommandSerializer extends Serializer[Command] {

  def read(implicit buffer: ByteArrayInputStream): Command = {
    val client = implRead[String]
    val id = implRead[Int]
    println(s"$client : $id")
    implRead[Int] match {
      case 0 => Get(client, id, implRead[String])
      case 1 => Put(client, id, implRead[String], implRead[String])
      case 2 => Delete(client, id, implRead[String])
      case 3 => CAS(client, id, implRead[String], implRead[String], implRead[String])
      case 4 => Append(client, id, implRead[String], implRead[String])
    }
  }

  def write(elem: Command)(implicit buf: ByteArrayOutputStream): Unit = {
    implWrite[String](elem.client)
    implWrite[Int](elem.id)
    elem match {
      case Get(_, _, key) =>
        implWrite[Int](0)
        implWrite[String](key)
      case Put(_, _, key, value) =>
        implWrite[Int](1)
        implWrite(key)
        implWrite(value)
      case Delete(_, _, key) =>
        implWrite[Int](2)
        implWrite(key)
      case CAS(_, _, key, cur, newVal) =>
        implWrite[Int](3)
        implWrite(key)
        implWrite(cur)
        implWrite(newVal)
      case Append(_, _, key, value) =>
        implWrite[Int](4)
        implWrite(key)
        implWrite(value)
    }
  }
}
