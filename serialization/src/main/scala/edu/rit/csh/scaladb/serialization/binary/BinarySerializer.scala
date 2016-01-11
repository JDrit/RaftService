package edu.rit.csh.scaladb.serialization.binary

import java.io.ByteArrayInputStream

import edu.rit.csh.scaladb.serialization.Serializer

abstract class BinarySerializer[T] extends Serializer[T, ByteArrayInput, ByteArrayOutput]

/**
 * Implicit converters to add method calls to regular classes
 */
object BinarySerializer {

  implicit class BinaryConverter[T](any: T) {
    def binary()(implicit ser: BinarySerializer[T]): Array[Byte] = {
      val output = new ByteArrayOutput()
      output.serialize(any)
      output.output
    }
  }

  implicit class FromBinary(arr: Array[Byte]) {
    def parse[T](implicit ser: BinarySerializer[T]): T = ser.read(new ByteArrayInput(arr))
  }
}

