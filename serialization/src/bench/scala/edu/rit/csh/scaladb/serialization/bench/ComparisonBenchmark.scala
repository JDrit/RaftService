package edu.rit.csh.scaladb.serialization.bench

import java.io.ByteArrayInputStream

import com.twitter.scrooge.{ThriftStructCodec3, ThriftStruct}
import edu.rit.csh.scaladb.serialization.{MapTest, StringTest, IntTest, ListTest, PersonTest}
import edu.rit.csh.scaladb.serialization.binary.BinarySerializer._
import edu.rit.csh.scaladb.serialization.binary.BinaryMacro._
import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._
import org.apache.thrift.protocol.{TCompactProtocol, TBinaryProtocol}
import org.apache.thrift.transport.{TIOStreamTransport, TMemoryBuffer}
import org.scalameter.api._

case class StringCase(str: String)
case class IntCase(int: Int)
case class ArrayCase(lst: Array[Int])
case class ListCase(lst: List[Int])
case class MapCase(map: Map[String, Int])

case class Person(name: String, age: Int, height: Double)

trait ComparisonGenerators extends PrimitiveGenerators {

  def thriftToBytes(struct: ThriftStruct): Array[Byte] = {
    val buffer = new TMemoryBuffer(32)
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
  val listThrift = length.map { len => ListTest(Range(0, len, 1)) }
  val listCase = length.map { len => ListCase(Range(0, len).toList) }
  val arrayCase = length.map { len => ArrayCase(Range(0, len, 1).toArray) }
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
  val listCaseBytes = listThrift.map(thriftToBytes)
  val arrayCaseBytes = arrayCase.map(_.binary())
  val mapThriftBytes = mapThrift.map(thriftToBytes)
  val mapCaseBytes = mapCase.map(_.binary())
}

trait Serialization extends Bench.OfflineReport with ComparisonGenerators {

  performance of "Serialization" in {
    measure method "String Thrift" in { using(stringThrift) in thriftToBytes }
    measure method "String Case" in { using(stringCase) in { msg => msg.binary() } }

    measure method "Int Thrift" in { using(intThrift) in thriftToBytes }
    measure method "Int Case" in { using(intCase) in { msg => msg.binary() } }

    measure method "List Thrift" in { using(listThrift) in thriftToBytes }
    measure method "List Case" in { using(arrayCase) in { msg => msg.binary() } }
    measure method "Array Case" in { using(arrayCase) in { msg => msg.binary() } }

    measure method "Map Thrift" in { using(mapThrift) in thriftToBytes }
    measure method "Map Case" in { using(mapCase) in { msg => msg.binary() } }

    measure method "Person Thrift" in { using(personThrift) in thriftToBytes }
    measure method "Person Case" in { using(personCase) in { msg => msg.binary() } }

  }
}

trait Deserialization extends Bench.OfflineReport with ComparisonGenerators {

  performance of "Deserialization" in {
    measure method "String Thrift" in { using(stringThriftBytes) in { b => bytesToThrift(StringTest, b) } }
    measure method "String Case" in { using(stringCaseBytes) in { b => b.parse[StringCase] } }

    measure method "Int Thrift" in { using(intThriftBytes) in { b => bytesToThrift(IntTest, b) } }
    measure method "Int Case" in { using(intThriftBytes) in { b => b.parse[IntCase] } }

    measure method "List Thrift" in { using(listThriftBytes) in { b => bytesToThrift(ListTest, b) } }
    measure method "List Case" in { using(listCaseBytes) in { b => b.parse[ListCase] } }
    measure method "Array Case" in { using(arrayCaseBytes) in { b => b.parse[ArrayCase] } }

    measure method "Map Thrift" in { using(mapThriftBytes) in { b => bytesToThrift(MapTest, b) } }
    measure method "Map Case" in { using(mapCaseBytes) in { b => b.parse[MapCase] } }
  }
}

class ComparisonBenchmark extends Serialization with Deserialization
class SerializationBenchmark extends Serialization
class DeserializationBenchmark extends Deserialization
class ComparisonDiskUsage extends Serialization {
  override def measurer = new DiskUsageMeasurer()
  override def defaultConfig: Context = Context(exec.independentSamples -> 1)
}
