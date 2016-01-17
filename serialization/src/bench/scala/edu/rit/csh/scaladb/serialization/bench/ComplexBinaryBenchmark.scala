package edu.rit.csh.scaladb.serialization.bench

import edu.rit.csh.scaladb.serialization.binary.BinarySerializer._
import edu.rit.csh.scaladb.serialization.binary.BinaryMacro._
import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._

import org.scalameter.{Gen, Persistor, Aggregator}
import org.scalameter.api._
import org.scalameter.picklers.noPickler._

import scala.collection.{mutable, immutable}

class ComplexBinaryBenchmark extends Bench.OfflineReport with PrimitiveGenerators {

  override def persistor: Persistor = new SerializationPersistor

  case class Person(name: String, age: Int)

  val intLists = length.map(len => (0 to len).toList)
  val strLists = length.map { len =>
    val builder = immutable.List.newBuilder[String]
    builder.sizeHint(len)
    (0 until len).foreach { _ => builder += "this is a test string, who knows what could happen here" }
    builder.result()
  }
  val intArrays = length.map { len => (0 to len).toArray }
  val strArrays = length.map { len =>
    val builder = immutable.List.newBuilder[String]
    builder.sizeHint(len)
    (0 until len).foreach { _ => builder += "this is a test string, who knows what could happen here" }
    builder.result()
  }

  val intSets = length.map(len => (0 to len).toSet)
  val people = Gen.crossProduct(integers, strings).map(t => Person(t._2, t._1))
  val tuples = Gen.crossProduct(integers, strings).map(t => (t._1, t._2))

  val arrayBytes = intArrays.map(_.binary())
  val listBytes = intLists.map(_.binary())
  val intSetBytes = intSets.map(_.binary())
  val personBytes = people.map(_.binary())
  //val tupleBytes = tuples.map(_.binary())

  performance of "Complex Serialization" in {
    measure method "int arrays" in { using(intArrays) in { a => a.binary() } }
    measure method "byte buffer int arrays" in { using(intArrays) in { a => a.byteBufferBinary() } }

    measure method "string arrays" in { using(strArrays) in { a => a.binary() } }
    measure method "byte buffer string arrays" in { using(strArrays) in { a => a.byteBufferBinary() } }

    measure method "int lists" in { using(intLists) in { l => l.binary() } }
    measure method "byte buffer int lists" in { using(intLists) in { l => l.byteBufferBinary() } }

    measure method "string lists" in { using(strLists) in { l => l.binary() } }
    measure method "byte buffer string lists" in { using(strLists) in { l => l.byteBufferBinary() } }

    measure method "maps[String, Int]" in { using(maps) in { l => l.binary() } }
    measure method "byte buffer maps[String, Int]" in { using(maps) in { l => l.byteBufferBinary() } }

    //measure method "int sets" in { using(intSets) in { s => s.binary() } }
    //measure method "case classes" in { using(people) in { p => p.binary() } }
    //measure method "tuples" in { using(tuples) in { t => t.binary() } }
  }

  /*performance of "Complex Deserialization" in {
    measure method "int arrays" in { using(arrayBytes) in { a => a.parse[Array[Int]] } }
    measure method "int lists" in { using(listBytes) in { l => l.parse[List[Int]] } }
    measure method "sets" in { using(intSetBytes) in { l => l.parse[Set[Int]] } }
    measure method "case classes" in { using(personBytes) in { p => p.parse[Person] } }
    measure method "tuples" in { using(tupleBytes) in { t => t.parse[(Int, String)] } }
  }*/
}
