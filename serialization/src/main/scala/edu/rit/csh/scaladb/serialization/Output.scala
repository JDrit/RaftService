package edu.rit.csh.scaladb.serialization

import java.io.OutputStream

import scala.annotation.tailrec

/**
 * The output of the serialization process.
 * @tparam T the type of the final product
 * @tparam E the type of the item being incrementally added
 */
abstract class Output[T, E] extends OutputStream {
  def output: T
}






