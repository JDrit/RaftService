package edu.rit.csh.scaladb.serialization.bench

import java.io.ByteArrayInputStream

import com.twitter.scrooge.{ThriftStructCodec3, ThriftStruct}
import edu.rit.csh.scaladb.serialization._
import edu.rit.csh.scaladb.serialization.binary.BinarySerializer._
import edu.rit.csh.scaladb.serialization.binary.BinaryMacro._
import org.apache.thrift.protocol.{TBinaryProtocol, TCompactProtocol}
import org.apache.thrift.transport.{TMemoryBuffer, TIOStreamTransport}
import org.scalameter.Gen

import scala.collection.mutable
import scala.collection.immutable
import scala.util.Random

trait ComparisonGenerators extends PrimitiveGenerators {

  case class StringCase(str: String)
  case class IntCase(int: Int)
  case class ArrayCase(lst: Array[Int])
  case class IntListCase(lst: List[Int])
  case class StrListCase(lst: List[String])
  case class SetCase(set: Set[Int])
  case class MapCase(map: Map[String, Int])
  case class Person(name: String, age: Int, height: Double)



  @inline
  def thriftToBytes(struct: ThriftStruct): Array[Byte] = {
    val buffer = new TMemoryBuffer(32 * 1024)
    struct.write(new TCompactProtocol(buffer))
    buffer.getArray
  }

  @inline
  def bytesToThrift[T <: ThriftStruct](obj: ThriftStructCodec3[T], b: Array[Byte]): T = {
    val buffer = new TIOStreamTransport(new ByteArrayInputStream((b)))
    obj.decode(new TCompactProtocol(buffer))
  }

  val stringThrift = strings.map(string => StringTest(string))
  val stringCase = strings.map(StringCase)
  val intThrift = integers.map(int => IntTest(int))
  val intCase = integers.map(IntCase)
  val intListThrift = length.map { len => ListTest((0 until len).toList) }
  val intListCase = length.map { len => IntListCase((0 until len).toList) }
  val strListThrift = length.map { len => StrListTest((0 until len).map(_ => strLen(20)).toList) }
  val strListCase = length.map { len => StrListCase((0 until len).map(_ => strLen(20)).toList) }
  val intArrayCase = length.map { len => ArrayCase((0 until len).toArray) }
  val intSetThrift = length.map { len => SetTest((0 until len).toSet) }
  val intSetCase= length.map { len => SetCase((0 until len).toSet) }
  val mapThrift = maps.map(map => MapTest(map))
  val mapCase = maps.map(map => MapCase(map))
  val personCase = Gen.crossProduct(strings, integers, doubles).map { case (str, int, double) =>
    Person(str, int, double)
  }
  val personThrift = Gen.crossProduct(strings, integers, doubles).map { case (str, int, double) =>
    PersonTest(str, int, double)
  }
  val coordsThrift = length.map { len =>
    val builder = List.newBuilder[CoordinateTest]
    builder.sizeHint(len)
    Random.setSeed(100)
    (0 until len).foreach { _ => builder += CoordinateTest(Random.nextInt(), Random.nextInt()) }
    CoordinateListsTest(builder.result())
  }



  val stringThriftBytes = stringThrift.map(thriftToBytes)
  val stringCaseBytes = stringCase.map(_.binary())
  val intThriftBytes = intThrift.map(thriftToBytes)
  val intCaseBytes = intCase.map(_.binary())
  val intListThriftBytes = intListThrift.map(thriftToBytes)
  val intListCaseBytes = intListCase.map(_.binary())
  val strListThriftBytes = strListThrift.map(thriftToBytes)
  val strListCaseBytes = strListCase.map(_.binary())
  val intArrayCaseBytes = intArrayCase.map(_.binary())
  val intSetThriftBytes = intSetThrift.map(thriftToBytes)
  val intSetCaseBytes = intSetCase.map(_.binary())
  val mapThriftBytes = mapThrift.map(thriftToBytes)
  val mapCaseBytes = mapCase.map(_.binary())
}
