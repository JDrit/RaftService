package edu.rit.csh.scaladb.serialization.bench

import edu.rit.csh.scaladb.serialization.binary.BinarySerializer._
import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._
import org.scalameter.picklers.noPickler._

import org.scalameter.api._

import scala.collection.mutable

trait PrimitiveGenerators {

  val length = Gen.range("length")(0, 60000, 3000)
  val characters = Gen.range("value")(Char.MinValue, Char.MaxValue, Char.MaxValue / 4).map(_.toChar)
  val integers = Gen.range("value")(Int.MinValue, Int.MaxValue, Int.MaxValue / 2)
  val doubles = integers.map(_.toDouble)
  val longs = integers.map(_.toLong)
  val floats = integers.map(_.toFloat)
  val strings = length.map { len =>
    val builder = new StringBuilder(len)
    (0 to len).foreach(_ => builder.append(" "))
    builder.toString()
  }
  val booleans = Gen.enumeration("booleans")(true, false)
  val maps = length.map { len =>
    var i = 0
    val map = mutable.Map.empty[String, Int]
    while (i < len) {
      map.put(i.toString, i)
      i += 1
    }
    map.toMap
  }
  def strLen(len: Int): String = (0 until len).foldLeft(new StringBuilder(len)) { case (builder, i) =>
    builder.append(i)
  }.toString

  val characterBytes = characters.map(_.binary())
  val integerBytes = integers.map(_.binary())
  val doubleBytes = doubles.map(_.binary())
  val longBytes = longs.map(_.binary())
  val floatBytes = floats.map(_.binary())
  val stringBytes = strings.map(_.binary())
  val booleanBytes = booleans.map(_.binary())

}
