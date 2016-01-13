package edu.rit.csh.scaladb.serialization.bench

import edu.rit.csh.scaladb.serialization.binary.BinarySerializer._
import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._
import org.scalameter.picklers.noPickler._

import org.scalameter.api._

trait PrimitiveGenerators {

  val length = Gen.range("length")(0, 10000, 1000)
  val characters = Gen.range("value")(Char.MinValue, Char.MaxValue, Char.MaxValue / 8).map(_.toChar)
  val integers = Gen.range("value")(Int.MinValue, Int.MaxValue, Int.MaxValue / 4)
  val doubles = integers.map(_.toDouble)
  val longs = integers.map(_.toLong)
  val floats = integers.map(_.toFloat)
  val strings = length.map { len =>
    val builder = new StringBuilder(len)
    (0 to len).foreach(_ => builder.append(" "))
    builder.toString
  }
  val booleans = Gen.enumeration("booleans")(true, false)

  val characterBytes = characters.map(_.binary())
  val integerBytes = integers.map(_.binary())
  val doubleBytes = doubles.map(_.binary())
  val longBytes = longs.map(_.binary())
  val floatBytes = floats.map(_.binary())
  val stringBytes = strings.map(_.binary())
  val booleanBytes = booleans.map(_.binary())

}
