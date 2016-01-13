package edu.rit.csh.jdb.scaladb.server

import edu.rit.csh.scaladb.raft.Command
import edu.rit.csh.scaladb.serialization.binary.{BinaryMacro, ByteArrayOutput, ByteArrayInput, BinarySerializer}
import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._
import edu.rit.csh.scaladb.serialization.binary.BinaryMacro._

/**
 * Uses the macro to generate Binary Serializers for each case class
 */
object CommandSerializer {

  implicit val gSer = BinaryMacro.materializeSerializer[Get]
  implicit val pSer = BinaryMacro.materializeSerializer[Put]
  implicit val dSer = BinaryMacro.materializeSerializer[Delete]
  implicit val aSer = BinaryMacro.materializeSerializer[Append]
  implicit val cSer = BinaryMacro.materializeSerializer[CAS]

  object CommandSerializer extends BinarySerializer[Command] {

    override def read(buffer: ByteArrayInput): Command = buffer.deserialize[Byte] match {
      case 0 => buffer.deserialize[Get]
      case 1 => buffer.deserialize[Put]
      case 2 => buffer.deserialize[Delete]
      case 3 => buffer.deserialize[CAS]
      case 4 => buffer.deserialize[Append]
    }

    override def write(elem: Command, buffer: ByteArrayOutput): Unit = elem match {
      case g: Get =>
        buffer.serialize[Byte](0)
        buffer.serialize(g)
      case p: Put =>
        buffer.serialize[Byte](1)
        buffer.serialize(p)
      case d: Delete =>
        buffer.serialize[Byte](2)
        buffer.serialize(d)
      case c: CAS =>
        buffer.serialize[Byte](3)
        buffer.serialize(c)
      case a: Append =>
        buffer.serialize[Byte](4)
        buffer.serialize(a)
    }
  }

}
