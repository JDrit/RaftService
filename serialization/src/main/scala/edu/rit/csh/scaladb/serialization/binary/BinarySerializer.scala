package edu.rit.csh.scaladb.serialization.binary

import edu.rit.csh.scaladb.serialization.Serializer

abstract class BinarySerializer[T] extends Serializer[T, ByteBufferInput, ByteBufferOutput]

