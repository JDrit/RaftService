package edu.rit.csh.scaladb.serialization.bench

import java.io.ByteArrayInputStream

import com.twitter.scrooge.{ThriftStructCodec3, ThriftStruct}
import edu.rit.csh.scaladb.serialization._
import edu.rit.csh.scaladb.serialization.binary.BinarySerializer._
import edu.rit.csh.scaladb.serialization.binary.BinaryMacro._
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.transport.{TMemoryBuffer, TIOStreamTransport}
import org.scalameter.Gen

trait ComparisonGenerators extends PrimitiveGenerators {

  case class StringCase(str: String)
  case class IntCase(int: Int)
  case class ArrayCase(lst: Array[Int])
  case class ListCase(lst: List[Int])
  case class SetCase(set: Set[String])
  case class MapCase(map: Map[String, Int])
  case class Person(name: String, age: Int, height: Double)

  @inline
  def thriftToBytes(struct: ThriftStruct): Array[Byte] = {
    val buffer = new TMemoryBuffer(32)
    struct.write(new TCompactProtocol(buffer))
    buffer.getBuffer
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
  val listThrift = length.map { len => ListTest((0 until len).map(strLen).toList) }
  val listCase = length.map { len => ListCase((0 until len).toList) }
  val arrayCase = length.map { len => ArrayCase((0 until len).toArray) }
  val setThrift = length.map { len => SetTest((0 to len).map(strLen).toSet) }
  val setCase = length.map { len => SetTest((0 until len).map(strLen).toSet) }
  val mapThrift = maps.map(map => MapTest(map))
  val mapCase = maps.map(map => MapCase(map))
  val personCase = Gen.crossProduct(strings, integers, doubles).map { case (str, int, double) =>
    Person(str, int, double)
  }
  val personThrift = Gen.crossProduct(strings, integers, doubles).map { case (str, int, double) =>
    PersonTest(str, int, double)
  }

  val stringThriftBytes = stringThrift.map(thriftToBytes)
  val stringCaseBytes = stringCase.map(_.binary())
  val intThriftBytes = intThrift.map(thriftToBytes)
  val intCaseBytes = intCase.map(_.binary())
  val listThriftBytes = listThrift.map(thriftToBytes)
  val listCaseBytes = listCase.map(_.binary())
  val arrayCaseBytes = arrayCase.map(_.binary())
  val setThriftBytes = setThrift.map(thriftToBytes)
  //val setCaseBytes = setCase.map(_.binary())
  val mapThriftBytes = mapThrift.map(thriftToBytes)
  val mapCaseBytes = mapCase.map(_.binary())
}
