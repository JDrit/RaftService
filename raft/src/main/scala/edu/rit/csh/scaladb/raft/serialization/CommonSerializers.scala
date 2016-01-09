package edu.rit.csh.scaladb.raft.serialization

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.lang.{Double => D}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import Serializer._

object CommonSerializers {

  implicit object IntSerializer extends Serializer[Int] {
    override def read(buffer: ByteArrayInputStream): Int = {
      var result: Int = 0
      val input = new Array[Byte](4)
      buffer.read(input)
      for (i <- 0 until 4) {
        result = result << 8
        result = result | (input(i) & 0xFF)
      }
      result
    }

    override def write(i: Int, buffer: ByteArrayOutputStream): Unit = {
      var input = i
      val output = new Array[Byte](4)
      for (i <- Range(3, -1, -1)) {
        output(i) = (input & 0xFF).asInstanceOf[Byte]
        input = input >> 4
      }
      buffer.write(output)
    }
  }

  implicit object DoubleSerializer extends Serializer[Double] {
    override def read(buffer: ByteArrayInputStream): Double = D.longBitsToDouble(LongSerializer.read(buffer))

    override def write(d: Double, buffer: ByteArrayOutputStream): Unit = LongSerializer.write(D.doubleToLongBits(d), buffer)
  }

  implicit object LongSerializer extends Serializer[Long] {
    override def read(buffer: ByteArrayInputStream): Long = {
      var result: Long = 0
      val input = new Array[Byte](8)
      buffer.read(input)
      for (i <- 0 until 8) {
        result = result << 8
        result = result | (input(i) & 0xFF)
      }
      result
    }

    override def write(l: Long, buffer: ByteArrayOutputStream): Unit = {
      var input = l
      val output = new Array[Byte](8)
      for (i <- Range(7, -1, -1)) {
        output(i) = (input & 0xFF).asInstanceOf[Byte]
        input = input >> 8
      }
      buffer.write(output)
    }
  }

  implicit object StringSerializer extends Serializer[String] {
    override def read(buffer: ByteArrayInputStream): String = {
      val len = buffer.read()
      val bytes = new Array[Byte](len)
      buffer.read(bytes)
      new String(bytes, "UTF-8")
    }

    override def write(str: String, buffer: ByteArrayOutputStream): Unit = {
      buffer.write(str.length)
      buffer.write(str.getBytes)
    }
  }

  implicit object BooleanSerializer extends Serializer[Boolean] {
    override def read(buf: ByteArrayInputStream): Boolean = buf.read() == 1

    override def write(bool: Boolean, buf: ByteArrayOutputStream): Unit = {
      if (bool)
        buf.write(1.asInstanceOf[Byte])
      else
        buf.write(0.asInstanceOf[Byte])
    }
  }

  implicit def arraySerializer[T: ClassTag](implicit ser: Serializer[T]) = new Serializer[Array[T]] {
    override def read(buffer: ByteArrayInputStream): Array[T] = {
      val len = Serializer.read[Int](buffer)
      val arr = new Array[T](len)
      for (i <- 0 until len) {
        arr(i) = ser.read(buffer)
      }
      arr
    }

    override def write(elem: Array[T], buffer: ByteArrayOutputStream): Unit = {
      Serializer.write(elem.length, buffer)
      elem.foreach(e => ser.write(e, buffer))
    }
  }

  implicit def traversableSerializer[T](implicit ser: Serializer[T]) = new Serializer[Traversable[T]] {
    override def read(buffer: ByteArrayInputStream): Traversable[T] = {
      val len = Serializer.read[Int](buffer)
      val arrBuff = new ArrayBuffer[T]()
      for (i <- 0 until len) {
        arrBuff +=  ser.read(buffer)
      }
      arrBuff.toTraversable
    }

    override def write(elem: Traversable[T], buffer: ByteArrayOutputStream): Unit = {
      Serializer.write(elem.size, buffer)
      elem.foreach(e => ser.write(e, buffer))
    }
  }

  implicit def listSerializer[T](implicit ser: Serializer[Traversable[T]]) = new Serializer[List[T]] {
    override def read(buffer: ByteArrayInputStream): List[T] = ser.read(buffer).toList

    override def write(elem: List[T], buffer: ByteArrayOutputStream): Unit = ser.write(elem.toTraversable, buffer)
  }

  implicit def setSerializer[T](implicit ser: Serializer[Traversable[T]]) = new Serializer[Set[T]] {
    override def read(buffer: ByteArrayInputStream): Set[T] = ser.read(buffer).toSet

    override def write(elem: Set[T], buffer: ByteArrayOutputStream): Unit = ser.write(elem.toTraversable, buffer)
  }

  implicit def mapSerializer[K, V](implicit keySer: Serializer[K], valSer: Serializer[V]) = new Serializer[Map[K, V]] {
    override def read(buffer: ByteArrayInputStream): Map[K, V] =
      (0 until Serializer.read[Int](buffer)).map(_ =>(keySer.read(buffer), valSer.read(buffer))).toMap

    override def write(elem: Map[K, V], buffer: ByteArrayOutputStream): Unit = {
      Serializer.write[Int](elem.size, buffer)
      elem.foreach { case (k, v) =>
          keySer.write(k, buffer)
          valSer.write(v, buffer)
      }
    }
  }

  implicit def optionSerializer[T](implicit ser: Serializer[T]) = new Serializer[Option[T]] {
    override def read(buffer: ByteArrayInputStream): Option[T] = if (Serializer.read[Boolean](buffer))
      Some(ser.read(buffer))
    else
      None

    override def write(elem: Option[T], buffer: ByteArrayOutputStream): Unit = elem match {
      case Some(e) =>
        Serializer.write(true, buffer)
        ser.write(e, buffer)
      case None =>
        Serializer.write(false, buffer)
    }
  }
}
