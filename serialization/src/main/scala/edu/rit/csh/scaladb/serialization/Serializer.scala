package edu.rit.csh.scaladb.serialization

import java.io.{OutputStream, InputStream}

/**
  * Abstract serializer used as base for all type of input and outputs
  * @tparam T the type that this serializer reads
  * @tparam O the type of output that it writes to
  * @tparam I the type of input that it reads from
  */
abstract class Serializer[T, O <: Output[_, _], I <: Input[_]] {

  def write(elem: T, output: O): Unit

  def read(i: I): T

}


/**
  * The input format for the serialization process
  *
  * @tparam E the type of the item that is returned
  */
trait Input[E] extends InputStream


/**
  * The output of the serialization process.
  * @tparam T the type of the final product
  * @tparam E the type of the item being incrementally added
  */
trait Output[T, E] extends OutputStream {
  def output: T
}