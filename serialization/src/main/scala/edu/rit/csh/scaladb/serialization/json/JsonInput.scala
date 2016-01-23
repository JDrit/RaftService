package edu.rit.csh.scaladb.serialization.json

import java.io.StringReader

import edu.rit.csh.scaladb.serialization.Input

class JsonInput(str: String) extends Input[Char] {
  val reader = new StringReader(str)

  override def read(): Int = reader.read()

  def readChar(): Char = read().toChar
}
