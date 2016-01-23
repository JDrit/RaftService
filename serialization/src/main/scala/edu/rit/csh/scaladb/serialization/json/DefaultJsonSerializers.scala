package edu.rit.csh.scaladb.serialization.json

import java.io.IOException

import org.apache.commons.lang3.StringEscapeUtils

/**
  * Created by jd on 1/22/16.
  */
object DefaultJsonSerializers {

  implicit object IntSerializer extends JsonSerializer[Int] {
    override def write(elem: Int, output: JsonOutput): Unit = output.write(Integer.toString(elem))

    override def read(i: JsonInput): Int = i.read()
  }

  implicit object ByteSerializer extends JsonSerializer[Byte] {
    override def write(elem: Byte, output: JsonOutput): Unit = IntSerializer.write(elem, output)

    override def read(i: JsonInput): Byte = IntSerializer.read(i).toByte
  }

  implicit object StringSerializer extends JsonSerializer[String] {
    override def write(str: String, output: JsonOutput): Unit = {
      output.write('\"' + StringEscapeUtils.escapeJson(str) + '\"')
    }

    override def read(input: JsonInput): String = {
      val builder = new StringBuilder()
      var going = true
      input.readChar()
      while (going) {
        input.readChar() match {
          case '\\' =>
            input.readChar() match {
              case '\\' => builder.append('\\')
              case 'b' => builder.append('\b')
              case 'r' => builder.append('\r')
              case 'n' => builder.append('\n')
              case 't' => builder.append('\t')
              case 'f' => builder.append('\f')
              case '"' => builder.append('\"')
              case ''' => builder.append('\'')
              case c => throw new IOException(s"Invalid Char sequence")
            }
          case '\"' => going = false
          case c => builder.append(c)
        }
      }
      builder.toString()
    }
  }


}
