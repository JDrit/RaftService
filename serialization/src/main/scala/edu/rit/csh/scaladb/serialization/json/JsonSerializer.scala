package edu.rit.csh.scaladb.serialization.json

import edu.rit.csh.scaladb.serialization.Serializer

/**
  * Created by jd on 1/22/16.
  */
abstract class JsonSerializer[T] extends Serializer[T, JsonOutput, JsonInput]

object JsonSerializer {

  implicit class ToJson[T](any: T) {
    def json()(implicit ser: JsonSerializer[T]): String = {
      val output = new JsonOutput()
      ser.write(any, output)
      output.output
    }
  }

  implicit class FromJson(str: String) {
    def parse[T](implicit ser: JsonSerializer[T]): T = {
      val input = new JsonInput(str)
      ser.read(input)
    }
  }
}
