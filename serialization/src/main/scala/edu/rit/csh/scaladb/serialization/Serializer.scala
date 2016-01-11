package edu.rit.csh.scaladb.serialization

import scala.language.experimental.macros


abstract class Serializer[T, I <: Input[_], O <: Output[_, _]] {

  def read(buffer: I): T

  def write(elem: T, buffer: O): Unit
}