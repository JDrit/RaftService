package edu.rit.csh.scaladb.raft.serialization

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object CommonSerializers {

  implicit object IntSerializer extends Serializer[Int] {
    override def read(implicit buffer: ByteArrayInputStream): Int = buffer.read()

    override def write(i: Int)(implicit buffer: ByteArrayOutputStream): Unit = buffer.write(i)
  }

  implicit object LongSerializer extends Serializer[Long] {
    override def read(implicit buffer: ByteArrayInputStream): Long = {
      var result: Long = 0
      val input = new Array[Byte](8)
      buffer.read(input)
      for (i <- 0 until 8) {
        result = result << 8
        result = result | (input(i) & 0xFF)
      }
      result
    }

    override def write(l: Long)(implicit buffer: ByteArrayOutputStream): Unit = {
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
    override def read(implicit buffer: ByteArrayInputStream): String = {
      val len = buffer.read()
      val bytes = new Array[Byte](len)
      buffer.read(bytes)
      new String(bytes)
    }

    override def write(str: String)(implicit buffer: ByteArrayOutputStream): Unit = {
      buffer.write(str.length)
      buffer.write(str.getBytes)
    }
  }

  implicit object BooleanSerializer extends Serializer[Boolean] {
    override def read(implicit buf: ByteArrayInputStream): Boolean = buf.read() == 1

    override def write(bool: Boolean)(implicit buf: ByteArrayOutputStream): Unit = {
      if (bool)
        buf.write(1.asInstanceOf[Byte])
      else
        buf.write(0.asInstanceOf[Byte])
    }
  }

  implicit def arraySerializer[T: ClassTag](implicit ser: Serializer[T]) = new Serializer[Array[T]] {
    override def read(implicit buffer: ByteArrayInputStream): Array[T] = {
      val len = implRead[Int]
      val arr = new Array[T](len)
      for (i <- 0 until len) {
        arr(i) = ser.read
      }
      arr
    }

    override def write(elem: Array[T])(implicit buffer: ByteArrayOutputStream): Unit = {
      implWrite(elem.length)
      elem.foreach(e => ser.write(e))
    }
  }

  implicit def traversableSerializer[T](implicit ser: Serializer[T]) = new Serializer[Traversable[T]] {
    override def read(implicit buffer: ByteArrayInputStream): Traversable[T] = {
      val len = implRead[Int]
      val arrBuff = new ArrayBuffer[T]()
      for (i <- 0 until len) {
        arrBuff +=  ser.read
      }
      arrBuff.toTraversable
    }

    override def write(elem: Traversable[T])(implicit buffer: ByteArrayOutputStream): Unit = {
      implWrite(elem.size)
      elem.foreach(e => ser.write(e))
    }
  }

  implicit def mapSerializer[K, V](implicit keySer: Serializer[K], valSer: Serializer[V]) = new Serializer[Map[K, V]] {
    override def read(implicit buffer: ByteArrayInputStream): Map[K, V] = (0 until implRead[Int])
      .map(_ =>(keySer.read, valSer.read)).toMap

    override def write(elem: Map[K, V])(implicit buffer: ByteArrayOutputStream): Unit = {
      implWrite[Int](elem.size)
      elem.foreach { case (k, v) =>
          keySer.write(k)
          valSer.write(v)
      }
    }
  }

  implicit def optionSerializer[T](implicit ser: Serializer[T]) = new Serializer[Option[T]] {
    override def read(implicit buffer: ByteArrayInputStream): Option[T] = if (implRead[Boolean])
      Some(ser.read)
    else
      None

    override def write(elem: Option[T])(implicit buffer: ByteArrayOutputStream): Unit = elem match {
      case Some(e) =>
        implWrite(true)
        ser.write(e)
      case None =>
        implWrite(false)
    }
  }
}
