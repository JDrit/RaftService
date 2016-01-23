package edu.rit.csh.scaladb.serialization.json

import edu.rit.csh.scaladb.serialization.Output

class JsonOutput extends Output[String, String] {
   private val buffer = new StringBuilder()

  def output: String = buffer.toString()

  override def write(i: Int): Unit = buffer.append(i)

  def write(str: String): Unit = buffer.append(str)

}
