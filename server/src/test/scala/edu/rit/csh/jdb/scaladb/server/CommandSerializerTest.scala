package edu.rit.csh.jdb.scaladb.server

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import edu.rit.csh.scaladb.raft.Command
import edu.rit.csh.scaladb.serialization.Serializer
import org.scalatest.FunSuite

class CommandSerializerTest extends FunSuite {

  def serTest(elem: Command): Unit = {
    val bao = new ByteArrayOutputStream()
    Serializer.write(elem, bao)(CommandSerializer)
    val bai = new ByteArrayInputStream(bao.toByteArray)
    assert(elem === Serializer.read(bai)(CommandSerializer))
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
