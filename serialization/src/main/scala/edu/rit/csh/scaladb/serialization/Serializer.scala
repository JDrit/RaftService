package edu.rit.csh.scaladb.serialization

abstract class Serializer[T, I <: Input[_], O <: Output[_, _]] {

  def read(buffer: I): T

  def write(elem: T, buffer: O): Unit
}