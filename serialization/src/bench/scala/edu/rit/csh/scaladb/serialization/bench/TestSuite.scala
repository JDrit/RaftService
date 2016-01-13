package edu.rit.csh.scaladb.serialization.bench

import org.scalameter.api._

class TestSuite extends Bench.Group {

  include(new PrimitiveBinaryBenchmark {})
  include(new ComplexBinaryBenchmark {})
  include(new DiskBenchmark {})

}
