package edu.rit.csh.scaladb.raft.serialization

import edu.rit.csh.scaladb.serialization.binary._
import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._
import BinarySerializer._
import edu.rit.csh.scaladb.serialization.binary.BinaryMacro._
import org.scalatest.FunSuite

import scala.reflect.ClassTag

class BinaryTest extends FunSuite {

  def serTest[T: ClassTag](elem: T)(implicit ser: BinarySerializer[T]): Unit = {
    assert(elem === elem.binary().parse[T])
  }

  test("Macro Serialization") {
    case class Person(height: Double, age: Int)
    val person = Person(21.5, 21)
    serTest[Person](person)
    serTest[Person](person)
  }

  test("Nested Macro Serialization") {
    case class Nested(test: String)
    case class Person(name: String, age: Int, nested: Nested)
    val person = Person("jd", 21, Nested("test"))
    serTest[Person](person)
  }

  test("Byte Serialization") {
    (Byte.MinValue.toInt to Byte.MaxValue.toInt).foreach { b =>
      serTest[Byte](b.toByte)
    }
  }

  test("Char Serialization") {
    (Char.MinValue to Char.MaxValue).foreach(serTest[Char])
  }

  test("Short Serialization") {
   (Short.MinValue.toInt to Short.MaxValue.toInt).foreach(s => serTest[Short](s.toShort))
  }

  test("Int Serialization") {
    List(0, 21, -5, 500, -500, Int.MaxValue, Int.MinValue).foreach(serTest[Int])
  }

  test("Long Serialization") {
    List(5L, -5L, 0L, Long.MaxValue, Long.MinValue).foreach(serTest[Long])
  }

  test("Double Serialization") {
    List(2.0, 20.5, -20.5, Double.MaxValue, Double.MinValue).foreach(serTest[Double])
  }

  test("Float Serialization") {
    List(0f, 100f, -100f, Float.MaxValue, Float.MinValue).foreach(serTest[Float])
  }

  test("String Serialization") {
    serTest[String]("test")
  }

  test("Empty String") {
    serTest[String]("")
  }

  test("Boolean Serialization") {
    serTest[Boolean](true)
    serTest[Boolean](false)
  }

  test("Range Serialization") {
    serTest[Range](new Range(0, 200, 2))
  }

  test("Option Serialization") {
    serTest(Some(5).asInstanceOf[Option[Int]])
    serTest(None.asInstanceOf[Option[Int]])
  }

  test("Array Serialization") {
    case class Coord(x: Int, y: Int)
    case class CoordinateListCase(lst: Array[Coord])
    serTest[Array[Int]]((0 to 10).toArray)
    val arr = Array(Coord(1, 2), Coord(3, 4), Coord(5, 6))
    serTest[Array[Coord]](arr)
  }

  test("Traversable Serialization") {
    serTest[Traversable[Int]]((0 to 10).toTraversable)
  }

  test("List Serialization") {
    serTest[List[Int]](List(1,2,3,4,5,6,7,8,9,0))
  }

  test("Seq Serialization") {
    serTest[Seq[String]](Seq("this", "is", "a", "test"))
  }

  test("Set Serialization") {
    serTest[Set[Int]](Set(3,5,7,3,2,6,8))
  }

  test("Map Serialization") {
    serTest(Map(1 -> Some("one"), 2 -> Some("two"), 3 -> None))
    serTest(Map("one" -> 1, "two" -> 2, "three" -> 3))
  }

  test("Tuples Serialization") {
    serTest((1, 2))
    serTest((1, 2, 3))
    serTest((1, 2, 3, 4))
    serTest((1, 2, 3, 4, 5))
    serTest((1, 2, 3, 4, 5, 6))
  }
}
