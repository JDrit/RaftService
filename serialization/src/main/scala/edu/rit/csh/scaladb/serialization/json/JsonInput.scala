package edu.rit.csh.scaladb.serialization.json

import edu.rit.csh.scaladb.serialization.Input

class JsonInput(str: String) extends Input[Char] {
  private var index = 0

  override def read(): Int = {
    val c = str.charAt(index)
    index += 1
    c
  }
}
