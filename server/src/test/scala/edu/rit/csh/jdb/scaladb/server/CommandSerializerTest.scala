package edu.rit.csh.jdb.scaladb.server

import edu.rit.csh.scaladb.raft.Command
import edu.rit.csh.scaladb.serialization.binary.{ByteArrayInput, ByteArrayOutput}
import org.scalatest.FunSuite

class CommandSerializerTest extends FunSuite {

  implicit val cmdSer = CommandSerializer.CommandSerializer

  def serTest(elem: Command): Unit = {
    val output = new ByteArrayOutput
    output.serialize(elem)
    val input = new ByteArrayInput(output.output)
    assert(elem === input.deserialize)
  }

  test("Get Serialization") {
    serTest(Get("client", 5, "key"))
  }

  test("Put Serialization") {
    serTest(Put("client", 6, "key", "value"))
  }

  test("Delete Serialization") {
    serTest(Delete("client", 7, "key"))
  }

  test("CAS Serialization") {
    serTest(CAS("client", 8, "key", "current", "new"))
  }

  test("Append Serialization") {
    serTest(Append("client", 9, "key", "value"))
  }
}
