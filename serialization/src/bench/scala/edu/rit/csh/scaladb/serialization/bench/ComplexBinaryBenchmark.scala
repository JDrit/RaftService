package edu.rit.csh.scaladb.serialization.bench

import edu.rit.csh.scaladb.serialization.binary.BinaryMacro
import edu.rit.csh.scaladb.serialization.binary.BinarySerializer._
import edu.rit.csh.scaladb.serialization.binary.BinaryMacro._
import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._

import org.scalameter.{Gen, Persistor, Aggregator}
import org.scalameter.api._
import org.scalameter.picklers.noPickler._

class ComplexBinaryBenchmark extends Bench.OfflineReport with PrimitiveGenerators {

  override def persistor: Persistor = new SerializationPersistor

  case class Person(name: String, age: Int)

  val lists = length.map(len => (0 to len).map(_.toChar).toList)
  val arrays = length.map { len =>
    val arr = new Array[Char](len)
    (0 until len).foreach { i => arr(i) = ' ' }
    arr
  }
  val sets = length.map(len => (0 to len).map(_.toChar).toSet)
  val people = Gen.crossProduct(integers, strings).map(t => Person(t._2, t._1))
  val tuples = Gen.crossProduct(integers, strings).map(t => (t._1, t._2))

  val arrayBytes = arrays.map(_.binary())
  val listBytes = lists.map(_.binary())
  val setBytes = sets.map(_.binary())
  val personBytes = people.map(_.binary())
  val tupleBytes = tuples.map(_.binary())

  performance of "Complex Serialization" in {
    measure method "arrays" in { using(arrays) in { a => a.binary() } }
    measure method "lists" in { using(lists) in { l => l.binary() } }
    measure method "sets" in { using(sets) in { s => s.binary() } }
    measure method "case classes" in { using(people) in { p => p.binary() } }
    measure method "tuples" in { using(tuples) in { t => t.binary() } }
  }

  performance of "Complex Deserialization" in {
    measure method "arrays" in { using(arrayBytes) in { a => a.parse[Array[Char]] } }
    measure method "lists" in { using(listBytes) in { l => l.parse[List[Char]] } }
    measure method "sets" in { using(setBytes) in { l => l.parse[Set[Char]] } }
    measure method "case classes" in { using(personBytes) in { p => p.parse[Person] } }
    measure method "tuples" in { using(tupleBytes) in { t => t.parse[(Int, String)] } }
  }
}
