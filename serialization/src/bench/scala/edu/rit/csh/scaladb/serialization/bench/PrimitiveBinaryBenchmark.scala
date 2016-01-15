package edu.rit.csh.scaladb.serialization.bench

import edu.rit.csh.scaladb.serialization.binary.BinarySerializer._
import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._
import org.scalameter.{Persistor, Aggregator}
import org.scalameter.picklers.Implicits._
import org.scalameter.picklers.noPickler._
import org.scalameter.api._

class PrimitiveBinaryBenchmark extends Bench.OfflineRegressionReport with PrimitiveGenerators {

  override def persistor: Persistor = new SerializationPersistor

  performance of "Primitive Serialization" in {
    measure method "integers" in { using(integers) in { i => i.binary() } }
    measure method "characters" in { using(characters) in { c => c.binary() } }
    measure method "booleans" in { using(booleans) in { b => b.binary() } }
    measure method "doubles" in { using(doubles) in { d => d.binary() } }
    measure method "longs" in { using(longs) in { l => l.binary() } }
    measure method "floats" in { using(floats) in { f => f.binary() } }
    measure method "strings" in {
      using(strings) in { s => s.binary() }
    }
  }

  performance of "Primitive Deserialization" in {
    measure method "integers" in {
      using(integerBytes) in { i => i.parse[Int] }
    }
    measure method "old integers" in {
      using(integers.map(_.binary()(BasicIntSerializer))) in { i => i.parse[Int](BasicIntSerializer) }
    }
   measure method "characters" in {
      using(characterBytes) in { c => c.parse[Char] }
    }
    measure method "booleans" in {
      using(booleanBytes) in { b => b.parse[Boolean] }
    }
    measure method "doubles" in {
      using(doubleBytes) in { d => d.parse[Double] }
    }
    measure method "longs" in {
      using(longBytes) in { l => l.parse[Long] }
    }
    measure method "floats" in {
      using(floatBytes) in { f => f.parse[Float] }
    }
    measure method "strings" in {
      using(stringBytes) in { s => s.parse[String] }
    }
  }
}
