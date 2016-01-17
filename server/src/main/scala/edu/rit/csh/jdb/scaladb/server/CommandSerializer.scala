package edu.rit.csh.jdb.scaladb.server

import edu.rit.csh.scaladb.raft.Command
import edu.rit.csh.scaladb.serialization.binary._
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

  val byteSer = implicitly[BinarySerializer[Byte]]

  object CommandSerializer extends DynamicSerializer[Command] {

    override def read(buffer: ByteArrayInput): Command = byteSer.read(buffer) match {
      case 0 => gSer.read(buffer)
      case 1 => pSer.read(buffer)
      case 2 => dSer.read(buffer)
      case 3 => cSer.read(buffer)
      case 4 => aSer.read(buffer)
    }

    override def write(elem: Command, buffer: BinaryOutput): Unit = elem match {
      case g: Get =>
        byteSer.write(0, buffer)
        gSer.write(g, buffer)
      case p: Put =>
        byteSer.write(1, buffer)
        pSer.write(p, buffer)
      case d: Delete =>
        byteSer.write(2, buffer)
        dSer.write(d, buffer)
      case c: CAS =>
        byteSer.write(3, buffer)
        cSer.write(c, buffer)
      case a: Append =>
        byteSer.write(4, buffer)
        aSer.write(a, buffer)
    }
  }

}
