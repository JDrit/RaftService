package edu.rit.csh.scaladb.serialization.bench

import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers.ZigZagIntSerializer
import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._
import edu.rit.csh.scaladb.serialization.binary.BinaryMacro._
import edu.rit.csh.scaladb.serialization.binary.BinarySerializer._

import org.scalameter.{Parameter, Parameters, Gen}
import org.scalameter.api._
import org.scalameter.picklers.Implicits._

import scala.collection.Iterator

class DiskBenchmark extends Bench.OfflineReport {

  def exponential(axisName: String)(from: Long, until: Long, factor: Long): Gen[Long] = new Gen[Long] {
    def warmupset = Iterator.single((until - from) / 2)
    def dataset = Iterator.iterate(from)(_ * factor).takeWhile(_ <= until).map(x => Parameters(Parameter[Long](axisName) -> x))
    def generate(params: Parameters) = params[Long](axisName)
  }

  override lazy val measurer = new DiskUsageMeasurer()
  override def defaultConfig: Context = Context(exec.independentSamples -> 1)

  val integers = Gen.exponential("value")(1, Int.MaxValue / 2, 2)
  val longs = exponential("value")(1L, Long.MaxValue / 2, 2L)

  val arrays = Gen.range("length")(1, 10000, 100).map { len =>
    val arr = new Array[Byte](len)
    (0 until len).foreach { i => arr(i) = 5 }
    arr
  }

  performance of "Primitive Disk Usage" in {
    measure method "integers" in {
      using(integers) in { i => i.binary() }
    }
    measure method "longs" in {
      using(longs) in { l => l.binary() }
    }
    /*measure method "arrays" in {
      using(arrays) in { a => a.binary() }
    }*/
  }
}
