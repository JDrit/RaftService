package edu.rit.csh.scaladb.serialization.bench

import edu.rit.csh.scaladb.serialization._
import edu.rit.csh.scaladb.serialization.binary.BinarySerializer._
import edu.rit.csh.scaladb.serialization.binary.BinaryMacro._
import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._
import org.scalameter.api._


trait Serialization extends Bench.OfflineReport with ComparisonGenerators {

  performance of "Serialization" in {
    //measure method "String Thrift" in { using(stringThrift) in thriftToBytes }
    //measure method "String Case" in { using(stringCase) in { msg => msg.oldBinary() } }
    //measure method "new String Case" in { using(stringCase) in { msg => msg.binary() } }

    //measure method "Int Thrift" in { using(intThrift) in thriftToBytes }
    //measure method "Int Case" in { using(intCase) in { msg => msg.binary() } }

    //measure method "List Thrift" in { using(listThrift) in thriftToBytes }
    measure method "old List Case" in { using(listCase) in { msg => msg.oldBinary() } }
    measure method "old Array Case" in { using(arrayCase) in { msg => msg.oldBinary() } }
    measure method "new List Case" in { using(listCase) in { msg => msg.binary() } }
    measure method "new Array Case" in { using(arrayCase) in { msg => msg.binary() } }

    //measure method "Set Thrift" in { using(setThrift) in thriftToBytes }
    //measure method "Set Case" in { using(setCase) in { msg => msg.binary() } }

    //measure method "Map Thrift" in { using(mapThrift) in thriftToBytes }
    //measure method "Map Case" in { using(mapCase) in { msg => msg.binary() } }

    //measure method "Person Thrift" in { using(personThrift) in thriftToBytes }
    //measure method "Person Case" in { using(personCase) in { msg => msg.binary() } }
    //measure method "Person Case" in { using(personCase) in { msg => msg.oldBinary() } }
  }
}

trait Deserialization extends Bench.OfflineReport with ComparisonGenerators {

  performance of "Deserialization" in {
    //measure method "String Thrift" in { using(stringThriftBytes) in { b => bytesToThrift(StringTest, b) } }
    //measure method "String Case" in { using(stringCaseBytes) in { b => b.parse[StringCase] } }

    //measure method "Int Thrift" in { using(intThriftBytes) in { b => bytesToThrift(IntTest, b) } }
    //measure method "Int Case" in { using(intThriftBytes) in { b => b.parse[IntCase] } }

    measure method "List Thrift" in { using(listThriftBytes) in { b => bytesToThrift(ListTest, b) } }
    measure method "List Case" in { using(listCaseBytes) in { b => b.parse[ListCase] } }
    measure method "Array Case" in { using(arrayCaseBytes) in { b => b.parse[ArrayCase] } }

    //measure method "Set Thrift" in { using(setThriftBytes) in { b => bytesToThrift(SetTest, b) } }
    //measure method "Set Case" in { using(setCaseBytes) in { b => b.parse[SetCase] } }

    //measure method "Map Thrift" in { using(mapThriftBytes) in { b => bytesToThrift(MapTest, b) } }
    //measure method "Map Case" in { using(mapCaseBytes) in { b => b.parse[MapCase] } }
  }
}

class ComparisonBenchmark extends Serialization with Deserialization
class SerializationBenchmark extends Serialization
class DeserializationBenchmark extends Deserialization
class ComparisonDiskUsage extends Serialization {
  override def measurer = new DiskUsageMeasurer()
  override def defaultConfig: Context = Context(exec.independentSamples -> 1)
}
