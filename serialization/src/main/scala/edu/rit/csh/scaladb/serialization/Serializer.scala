package edu.rit.csh.scaladb.serialization

/**
 * Base class for the serializer that reads and writes to the given formats
 * @tparam T the data type that this serializer reads + writes
 * @tparam I the type of input that the serializer reads from
 * @tparam O the type of output that the serializer writes to
 */
abstract class Serializer[T, I <: Input[_], O <: Output[_, _]] {

  def read(buffer: I): T

  def write(elem: T, buffer: O): Unit
}