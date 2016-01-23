package edu.rit.csh.scaladb.raft.serialization

import edu.rit.csh.scaladb.serialization.json.JsonSerializer
import edu.rit.csh.scaladb.serialization.json.DefaultJsonSerializers._
import edu.rit.csh.scaladb.serialization.json.JsonSerializer._
import org.scalatest.FunSuite

/**
  * Created by jd on 1/22/16.
  */
class JsonTest extends FunSuite {

  def jsonTest[T](elem: T)(implicit ser: JsonSerializer[T]): Unit = {
    assert(elem === elem.json().parse[T])
  }

  test("normal string test") {
    jsonTest("this is a test")
    jsonTest("")
  }

  test("string escape characters test") {
    jsonTest("\"quote\"")
    jsonTest("\\back\\")
    jsonTest("/this/")
    jsonTest("this\nis\na\ntest")
    jsonTest("this\tis a \ttab")

  }

  test("Int test") {
    jsonTest(5)
    jsonTest(0)
    jsonTest(-50)
  }

}
