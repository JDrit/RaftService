package edu.rit.csh.scaladb.raft.serialization.binary

import edu.rit.csh.scaladb.serialization.binary.{BinarySerializer, ByteArrayInput, ByteArrayOutput}
import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._
import edu.rit.csh.scaladb.serialization.binary.BinaryMacro._
import org.scalatest.FunSuite

import scala.reflect.ClassTag

class BinaryTest extends FunSuite {


  def serTest[T: ClassTag](elem: T)(implicit ser: BinarySerializer[T]): Unit = {
    val output = new ByteArrayOutput
    output.serialize(elem)
    val input = new ByteArrayInput(output.output)
    assert(elem === input.deserialize[T])
    println(s"elem = $elem : ${output.output.length} bytes")
  }

  test("Macro Serialization") {
    case class Person(name: String, age: Int, lst: List[Long])
    val person = Person("jd", 21, List(4L, 5L))
    serTest[Person](person)
  }

  test("Byte Serialization") {
    serTest[Byte](4)
    serTest[Byte](0)
  }

  test("Int Serialization") {
    serTest[Int](0)
    serTest[Int](21)
    serTest[Int](-5)
    serTest[Int](500)
    serTest[Int](-500)
    serTest[Int](Int.MaxValue)
    serTest[Int](Int.MinValue)
  }

  test("Long Serialization") {
    serTest[Long](5L)
    serTest[Long](-5L)
    serTest[Long](0L)
    serTest[Long](Long.MaxValue)
    serTest[Long](Long.MinValue)
  }

  test("Double Serialization") {
    serTest[Double](2.0)
    serTest[Double](20.5)
    serTest[Double](-20.5)
    serTest[Double](Double.MaxValue)
    serTest[Double](Double.MinValue)
  }

  test("Float Serialization") {
    serTest[Float](0f)
    serTest[Float](100f)
    serTest[Float](-100f)
    serTest[Float](Float.MaxValue)
    serTest[Float](Float.MinValue)
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


  test("Function Serialization") {
    val test: String => Boolean = (str: String) => str.isEmpty
    val output = new ByteArrayOutput
    output.serialize(test)
    val input = new ByteArrayInput(output.output)
    val fun = input.deserialize[String => Boolean]
    assert(fun("") === true)
    assert(fun("fdsa") === false)

    val add: Int => (Int => Int) = (x: Int) => (y: Int) => x + y
    val output1 = new ByteArrayOutput
    output1.serialize(add)
    val input1 = new ByteArrayInput(output1.output)
    val fun1 = input1.deserialize[Int => Int => Int]
    assert(add(5)(6) === fun1(5)(6))
  }

  test("Option Serialization") {
    serTest(Some(5).asInstanceOf[Option[Int]])
    serTest(None.asInstanceOf[Option[Int]])
  }

  test("Array Serialization") {
    serTest[Array[Int]]((0 to 10).toArray)
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

  test("Tuples Serialization") {
    serTest((1, 2))
    serTest((1, 2, 3))
    serTest((1, 2, 3, 4))
    serTest((1, 2, 3, 4, 5))
    serTest((1, 2, 3, 4, 5, 6))
  }

  test("Implicit Serialization") {
    import BinarySerializer._
    val elem: (Int, Int, List[String]) = (1,2, List("String"))
    val arr = elem.binary()
    assert(arr.parse[(Int, Int, List[String])] === elem)
  }
}
