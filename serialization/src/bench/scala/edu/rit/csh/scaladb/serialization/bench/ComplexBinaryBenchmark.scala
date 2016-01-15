package edu.rit.csh.scaladb.serialization.bench

import edu.rit.csh.scaladb.serialization.binary.BinarySerializer._
import edu.rit.csh.scaladb.serialization.binary.BinaryMacro._
import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._

import org.scalameter.{Gen, Persistor, Aggregator}
import org.scalameter.api._
import org.scalameter.picklers.noPickler._

import scala.collection.immutable

class ComplexBinaryBenchmark extends Bench.OfflineReport with PrimitiveGenerators {

  override def persistor: Persistor = new SerializationPersistor

  case class Person(name: String, age: Int)

  val intLists = length.map(len => (0 to len).toList)
  val strList = length.map { len =>
    val builder = immutable.List.newBuilder[String]
    builder.sizeHint(len)
    (0 until len).foreach { _ => builder += "this is a test string, who knows what could happen here" }
    builder.result()
  }
  val intArray = length.map { len => (0 to len).toArray }
  val sets = length.map(len => (0 to len).map(_.toChar).toSet)
  val people = Gen.crossProduct(integers, strings).map(t => Person(t._2, t._1))
  val tuples = Gen.crossProduct(integers, strings).map(t => (t._1, t._2))

  val arrayBytes = intArray.map(_.binary())
  val listBytes = intLists.map(_.binary())
  val setBytes = sets.map(_.binary())
  val personBytes = people.map(_.binary())
  val tupleBytes = tuples.map(_.binary())

  performance of "Complex Serialization" in {
    measure method "old string lists" in { using(strList) in { l => l.binary() } }
    measure method "new string lists" in { using(strList) in { l => l.binary() } }
    measure method "old int arrays" in { using(intArray) in { a => a.binary() } }
    measure method "new int arrays" in { using(intArray) in { a => a.binary() } }
    measure method "old int lists" in { using(intLists) in { l => l.binary() } }
    measure method "new int lists" in { using(intLists) in { l => l.binary() } }
    //measure method "sets" in { using(sets) in { s => s.binary() } }
    //measure method "case classes" in { using(people) in { p => p.binary() } }
    //measure method "tuples" in { using(tuples) in { t => t.binary() } }
  }

  performance of "Complex Deserialization" in {
    //measure method "arrays" in { using(arrayBytes) in { a => a.parse[Array[Int]] } }
    //measure method "arrays" in { using(arrayBytes) in { a => a.parse[Array[Int]] } }
    //measure method "lists" in { using(listBytes) in { l => l.parse[List[Char]] } }
    //measure method "sets" in { using(setBytes) in { l => l.parse[Set[Char]] } }
    //measure method "case classes" in { using(personBytes) in { p => p.parse[Person] } }
    //measure method "tuples" in { using(tupleBytes) in { t => t.parse[(Int, String)] } }
  }
}
