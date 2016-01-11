package edu.rit.csh.scaladb.serialization.json

import edu.rit.csh.scaladb.serialization.Output


class JsonOutput extends Output[String, String] {
   private val buffer = new StringBuilder()

   def output: String = buffer.toString()

   def add(s: String): Unit = buffer.append(s)

  override def write(i: Int): Unit = buffer.append(i)
}
