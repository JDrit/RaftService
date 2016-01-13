package edu.rit.csh.scaladb.serialization.bench

import org.scalameter._
import org.scalameter.api.Measurer

class DiskUsageMeasurer extends Measurer[Double] {
  override def name: String = "Measurer.DiskUsage"

  override def measure[T](context: Context, measurements: Int, setup: (T) => Any, tear: (T) => Any,
                          regen: () => T, snippet: (T) => Any): Seq[Quantity[Double]] = {
    val value = regen()
    setup(value)
    val output = snippet(value).asInstanceOf[Array[Byte]]
    tear(value)
    Seq(Quantity(output.length, "bytes"), Quantity(output.length, "bytes"))
  }
}
