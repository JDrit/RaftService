package edu.rit.csh.scaladb.raft.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import edu.rit.csh.scaladb.serialization.{Serializer, CommonSerializers}
import org.scalatest.FunSuite
import CommonSerializers._

class SerializationTest extends FunSuite {

  def serTest[T](elem: T)(implicit ser: Serializer[T]): Unit = {
    val bao = new ByteArrayOutputStream()
    Serializer.write(elem, bao)
    val bai = new ByteArrayInputStream(bao.toByteArray)
    assert(elem === Serializer.read(bai))
  }

  test("Macro Serialization") {
    case class Person(name: String, age: Int, lst: List[Long])
    implicit val personSerializer = Serializer.materializeSerializer[Person]
    serTest[Person](Person("jd", 21, List(4L, 5L)))
  }

  test("Byte Serialization") {
    serTest[Byte](4)
    serTest[Byte](0)
  }

  test("Int Serialization") {
    serTest[Int](21)
    serTest[Int](-5)
  }

  test("Long Serialization") {
    serTest[Long](5L)
    serTest[Long](-5L)
  }

  test("Double Serialization") {
    serTest[Double](2.0)
    serTest[Double](20.5)
    serTest[Double](-20.5)
  }

  test("String Serialization") {
    serTest[String]("this is a test")
  }

  test("Empty String") {
    serTest[String]("")
  }

  test("Boolean Serialization") {
    serTest[Boolean](true)
    serTest[Boolean](false)
  }

  test("Option Serialization") {
    serTest(Some(5).asInstanceOf[Option[Int]])
    serTest(None.asInstanceOf[Option[Int]])
  }

  test("Array Serialization") {
    serTest[Array[Int]]((0 to 10).toArray)
  }

  test("Traversable Serialization") {
    val trav: Traversable[Int] = (0 to 10).toTraversable
    serTest(trav)
  }

  test("List Serialization") {
    val lst = List(1,2,3,4,5,6,7,8,9,0)
    serTest[List[Int]](lst)
  }

  test("Set Serialization") {
    val set = Set(3,5,7,3,2,6,8)
    serTest(set)
  }

  test("Map Serialization") {
    val map = Map(1 -> Some("one"), 2 -> Some("two"), 3 -> None)
    serTest(map)
  }
}
