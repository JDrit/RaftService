package edu.rit.csh.scaladb.serialization.binary

import edu.rit.csh.scaladb.serialization.Serializer

/**
 * The serializer that read / writes binary data for type T
 * @tparam T the data type that this serializer reads + writes
 */
abstract class BinarySerializer[T] extends Serializer[T, BinaryOutput, ByteArrayInput] {

  /**
   * If the type [[T]] serializes to the same size or if it is dynamic, such as a
   * collection
   */
  def staticSize: Option[Int]

  /**
   * Gets the max size of the output that would be generated by this data type [[T]]
   */
  def getSize(elem: T): Int

  /**
    * If this serializer can easily calculate the size that it needs to serialize itself
    */
  def canGetSize: Boolean

  /**
   * Generates the correct type of output given the element type
   */
  final def generateOutput(elem: T): BinaryOutput =
    if (canGetSize) new ByteArrayOutput(getSize(elem))
    else new ByteBufferOutput()
}

/**
 * Represents all the serializers where the size of the output data is not known ahead of time,
 * such as list of maps
 * @tparam T the data type that this serializer reads + writes
 */
abstract class DynamicSerializer[T] extends BinarySerializer[T] {

  /**
   * Dynamic serializers output sizes cannot be predicted ahead of time
   */
  override final val staticSize: Option[Int] = None
}

/**
 * Represents all the serializers where the size of the output data is known ahead of time.
 * @tparam T the data type that this serializer reads + writes
 */
abstract class StaticSerializer[T] extends BinarySerializer[T] {

  /**
   * must be provided by sub-classes, the max size this data type can be
   */
  val size: Int

  override final def staticSize: Option[Int] = Some(size)

  override final def getSize(elem: T) = size

  override final def canGetSize = true
}

/**
 * Implicit converters to add method calls to regular classes
 */
object BinarySerializer {

  implicit class BinaryConverter[T](any: T) {

    def binary()(implicit ser: BinarySerializer[T]): Array[Byte] = {
      val output = ser.generateOutput(any)
      ser.write(any, output)
      output.output
    }

    def byteBufferBinary()(implicit ser: BinarySerializer[T]): Array[Byte] = {
      val output = new ByteBufferOutput()
      ser.write(any, output)
      output.output
    }
  }

  implicit class FromBinary(arr: Array[Byte]) {

    def parse[T](implicit ser: BinarySerializer[T]): T = ser.read(new ByteArrayInput(arr))
  }
}

