package edu.rit.csh.scaladb.serialization.bench

import edu.rit.csh.scaladb.serialization._
import edu.rit.csh.scaladb.serialization.binary.BinarySerializer._
import edu.rit.csh.scaladb.serialization.binary.BinaryMacro._
import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._
import org.scalameter.api._

import scala.util.Random


trait Serialization extends Bench.OfflineReport with ComparisonGenerators {

  performance of "Serialization" in {

    measure method "String Thrift" in { using(stringThrift) in thriftToBytes }
    measure method "String Case" in { using(stringCase) in { msg => msg.binary() } }

    measure method "Int Thrift" in { using(intThrift) in thriftToBytes }
    measure method "Int Case" in { using(intCase) in { msg => msg.binary() } }

    measure method "Int List Thrift" in { using(intListThrift) in thriftToBytes }
    measure method "int List Case" in { using(intListCase) in { msg => msg.binary() } }
    measure method "int Array Case" in { using(intArrayCase) in { msg => msg.binary() } }

    measure method "String List Thrift" in { using(strListThrift) in thriftToBytes }
    measure method "String List Case" in { using(strListCase) in { msg => msg.binary() } }

    measure method "Int Set Thrift" in { using(intSetThrift) in thriftToBytes }
    measure method "Int Set Case" in { using(intSetCase) in { msg => msg.binary() } }

    measure method "Map Thrift" in { using(mapThrift) in thriftToBytes }
    measure method "Map Case" in { using(mapCase) in { msg => msg.binary() } }

    measure method "Person Thrift" in { using(personThrift) in thriftToBytes }
    measure method "Person Case" in { using(personCase) in { msg => msg.binary() } }


    case class CoordinateCase(x: Int, y: Int)
    case class CoordinateListCase(lst: Array[CoordinateCase])

    val coordsCase = length.map { len =>
      val arr = new Array[CoordinateCase](len)
      Random.setSeed(100)
      (0 until len).foreach { i => arr(i) = CoordinateCase(Random.nextInt(), Random.nextInt()) }
      CoordinateListCase(arr)
    }

    measure method "Coordinate List Thrift" in { using(coordsThrift) in thriftToBytes }
    measure method "Coordinate List Case" in { using(coordsCase) in { msg => msg.binary() } }

  }
}

trait Deserialization extends Bench.OfflineReport with ComparisonGenerators {

  performance of "Deserialization" in {
    measure method "String Thrift" in { using(stringThriftBytes) in { b => bytesToThrift(StringTest, b) } }
    measure method "String Case" in { using(stringCaseBytes) in { b => b.parse[StringCase] } }

    measure method "Int Thrift" in { using(intThriftBytes) in { b => bytesToThrift(IntTest, b) } }
    measure method "Int Case" in { using(intThriftBytes) in { b => b.parse[IntCase] } }

    measure method "Int List Thrift" in { using(intListThriftBytes) in { b => bytesToThrift(ListTest, b) } }
    measure method "Int List Case" in { using(intListCaseBytes) in { b => b.parse[IntListCase] } }
    measure method "Int Array Case" in { using(intArrayCaseBytes) in { b => b.parse[ArrayCase] } }

    measure method "Int Set Thrift" in { using(intSetThriftBytes) in { b => bytesToThrift(SetTest, b) } }
    measure method "Int Set Case" in { using(intSetCaseBytes) in { b => b.parse[SetCase] } }

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
