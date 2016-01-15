package edu.rit.csh.scaladb.serialization.binary

import edu.rit.csh.scaladb.serialization.Serializer

abstract class BinarySerializer[T] extends Serializer[T, ByteArrayInput, ByteArrayOutput] {
  def write(elem: T, offset: Int, buffer: Array[Byte]): Int
  def size(elem: T): Int
}

abstract class DynamicSerializer[T] extends BinarySerializer[T]

abstract class StaticSerializer[T] extends BinarySerializer[T] {
  val size: Int
  override final def size(elem: T): Int = size
}

/**
 * Implicit converters to add method calls to regular classes
 */
object BinarySerializer {

  implicit class BinaryConverter[T](any: T) {
    def oldBinary()(implicit ser: BinarySerializer[T]): Array[Byte] = {
      val output = new ByteArrayOutput()
      output.serialize(any)
      output.output
    }

    def binary()(implicit ser: BinarySerializer[T]): Array[Byte] = {
      val size = ser.size(any)
      val arr = new Array[Byte](size)
      ser.write(any, 0, arr)
      arr
    }
  }

  implicit class FromBinary(arr: Array[Byte]) {
    def parse[T](implicit ser: BinarySerializer[T]): T = ser.read(new ByteArrayInput(arr))
  }
}

