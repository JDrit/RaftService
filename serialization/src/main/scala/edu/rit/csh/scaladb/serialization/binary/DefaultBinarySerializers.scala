package edu.rit.csh.scaladb.serialization.binary

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.lang.{Double => D, Float => F}

import scala.reflect.ClassTag
import scala.collection.immutable
import scala.collection.mutable

object DefaultBinarySerializers {


  implicit object ByteSerializer extends BinarySerializer[Byte] {
    override def read(buffer: ByteArrayInput): Byte = buffer.read.toByte

    override def write(b: Byte, buffer: ByteArrayOutput): Unit = buffer.write(b)
  }

  object BasicIntSerializer extends BinarySerializer[Int] {
    override def read(buffer: ByteArrayInput): Int = {
      val bytes = new Array[Byte](4)
      buffer.read(bytes)
      bytes(0) << 24 | (bytes(1) & 0xFF) << 16 | (bytes(2) & 0xFF) << 8 | (bytes(3) & 0xFF)
    }

    override def write(elem: Int, buffer: ByteArrayOutput): Unit = {
      val arr = new Array[Byte](4)
      arr(0) = (elem >> 24).toByte
      arr(1) = (elem >> 16).toByte
      arr(2) = (elem >> 8).toByte
      arr(3) = elem.toByte
      buffer.write(arr)
    }
  }

  implicit object ZigZagIntSerializer extends BinarySerializer[Int] {
    override def read(buffer: ByteArrayInput): Int = {
      var value = 0
      var i = 0
      var b: Int = buffer.read()
      while ((b & 0x80) != 0) {
        value |= (b & 0x7F) << i
        i += 7
        b = buffer.read()
      }
      val raw = value | (b << i)
      ((((raw << 31) >> 31) ^ raw) >> 1) ^ (raw & (1 << 31))
    }

    override def write(i: Int, buffer: ByteArrayOutput): Unit = {
      var value = (i << 1) ^ (i >> 31)
      while ((value & 0xFFFFFF80) != 0L) {
        buffer.write((value & 0x7F) | 0x80)
        value = value >>> 7
      }
      buffer.write(value & 0x7F)
    }
  }

  implicit object CharSerializer extends BinarySerializer[Char] {
    override def read(buffer: ByteArrayInput): Char = buffer.deserialize[Int].toChar

    override def write(elem: Char, buffer: ByteArrayOutput): Unit = buffer.serialize[Int](elem)
  }

  implicit object ZigZagLongSerializer extends BinarySerializer[Long] {
    override def read(buffer: ByteArrayInput): Long = {
      var value = 0L
      var i = 0
      var b: Long = buffer.read()
      while ((b & 0x80L) != 0) {
        value |= (b & 0x7F) << i
        i += 7
        b = buffer.read()
      }
      val raw = value | (b << i)
      ((((raw << 63) >> 63) ^ raw) >> 1) ^ (raw & (1L << 63))
    }

    override def write(l: Long, buffer: ByteArrayOutput): Unit = {
      var value = (l << 1) ^ (l >> 63)
      while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
        buffer.write((value & 0x7F).asInstanceOf[Int] | 0x80)
        value >>>= 7
      }
      buffer.write((value & 0x7F).asInstanceOf[Int])
    }
  }

  implicit object DoubleSerializer extends BinarySerializer[Double] {
    override def read(buffer: ByteArrayInput): Double = D.longBitsToDouble(ZigZagLongSerializer.read(buffer))

    override def write(d: Double, buffer: ByteArrayOutput): Unit = ZigZagLongSerializer.write(D.doubleToLongBits(d), buffer)
  }

  implicit object FloatSerializer extends BinarySerializer[Float] {
    override def read(buffer: ByteArrayInput): Float = F.intBitsToFloat(ZigZagIntSerializer.read(buffer))

    override def write(f: Float, buffer: ByteArrayOutput): Unit = ZigZagIntSerializer.write(F.floatToIntBits(f), buffer)
  }

  implicit object StringSerializer extends BinarySerializer[String] {
    override def read(buffer: ByteArrayInput): String = {
      val len = buffer.deserialize[Int]
      val str = new String(buffer.buffer, buffer.index, len, "UTF-8")
      buffer.index += len
      str
    }

    override def write(str: String, buffer: ByteArrayOutput): Unit = {
      buffer.serialize[Int](str.length)
      buffer.write(str.getBytes)
    }
  }

  implicit object BooleanSerializer extends BinarySerializer[Boolean] {
    override def read(buf: ByteArrayInput): Boolean = buf.read() == 1

    override def write(bool: Boolean, buf: ByteArrayOutput): Unit = buf.write(if (bool) 1 else 0)
  }

  implicit def function1Serializer[A, R] = new BinarySerializer[A => R] {
    override def read(buffer: ByteArrayInput): (A) => R = {
      new ObjectInputStream(buffer).readObject().asInstanceOf[A => R]
    }

    override def write(elem: (A) => R, buffer: ByteArrayOutput): Unit = {
      new ObjectOutputStream(buffer).writeObject(elem)
    }
  }

  implicit def function2Serializer[A1, A2, R] = new BinarySerializer[(A1, A2) => R] {
    override def read(buffer: ByteArrayInput): (A1, A2) => R = {
      new ObjectInputStream(buffer).readObject().asInstanceOf[(A1, A2) => R]
    }

    override def write(elem: (A1, A2) => R, buffer: ByteArrayOutput): Unit = {
      new ObjectOutputStream(buffer).writeObject(elem)
    }
  }

  implicit def function3Serializer[A1, A2, A3, R] = new BinarySerializer[(A1, A2, A3) => R] {
    override def read(buffer: ByteArrayInput): (A1, A2, A3) => R = {
      new ObjectInputStream(buffer).readObject().asInstanceOf[(A1, A2, A3) => R]
    }

    override def write(elem: (A1, A2, A3) => R, buffer: ByteArrayOutput): Unit = {
      new ObjectOutputStream(buffer).writeObject(elem)
    }
  }

  implicit def function4Serializer[A1, A2, A3, A4, R] = new BinarySerializer[(A1, A2, A3, A4) => R] {
    override def read(buffer: ByteArrayInput): (A1, A2, A3, A4) => R = {
      new ObjectInputStream(buffer).readObject().asInstanceOf[(A1, A2, A3, A4) => R]
    }

    override def write(elem: (A1, A2, A3, A4) => R, buffer: ByteArrayOutput): Unit = {
      new ObjectOutputStream(buffer).writeObject(elem)
    }
  }

  implicit def function5Serializer[A1, A2, A3, A4, A5, R] = new BinarySerializer[(A1, A2, A3, A4, A5) => R] {
    override def read(buffer: ByteArrayInput): (A1, A2, A3, A4, A5) => R = {
      new ObjectInputStream(buffer).readObject().asInstanceOf[(A1, A2, A3, A4, A5) => R]
    }

    override def write(elem: (A1, A2, A3, A4, A5) => R, buffer: ByteArrayOutput): Unit = {
      new ObjectOutputStream(buffer).writeObject(elem)
    }
  }

  implicit def function6Serializer[A1, A2, A3, A4, A5, A6, R] = new BinarySerializer[(A1, A2, A3, A4, A5, A6) => R] {
    override def read(buffer: ByteArrayInput): (A1, A2, A3, A4, A5, A6) => R = {
      new ObjectInputStream(buffer).readObject().asInstanceOf[(A1, A2, A3, A4, A5, A6) => R]
    }

    override def write(elem: (A1, A2, A3, A4, A5, A6) => R, buffer: ByteArrayOutput): Unit = {
      new ObjectOutputStream(buffer).writeObject(elem)
    }
  }

  implicit object RangeSerializer extends BinarySerializer[Range] {
    override def read(buffer: ByteArrayInput): Range = {
      new Range(buffer.deserialize[Int], buffer.deserialize[Int], buffer.deserialize[Int])
    }

    override def write(elem: Range, buffer: ByteArrayOutput): Unit = {
      buffer.serialize(elem.start)
      buffer.serialize(elem.end)
      buffer.serialize(elem.step)
    }
  }

  implicit def optionSerializer[T](implicit ser: BinarySerializer[T]) = new BinarySerializer[Option[T]] {
    override def read(buffer: ByteArrayInput): Option[T] = if (buffer.deserialize[Boolean])
      Some(buffer.deserialize[T])
    else
      None

    override def write(elem: Option[T], buffer: ByteArrayOutput): Unit = elem match {
      case Some(e) =>
        buffer.serialize[Boolean](true)
        buffer.serialize[T](e)
      case None =>
        buffer.serialize[Boolean](false)
    }
  }

  implicit def arraySerializer[T: ClassTag](implicit ser: BinarySerializer[T]) = new BinarySerializer[Array[T]] {
    override def read(buffer: ByteArrayInput): Array[T] = {
      val len = buffer.deserialize[Int]
      val arr = new Array[T](len)
      var i = 0
      while (i < len) {
        arr(i) = ser.read(buffer)
        i += 1
      }
      arr
    }

    override def write(elem: Array[T], buffer: ByteArrayOutput): Unit = {
      buffer.serialize[Int](elem.length)
      var i = 0
      val len = elem.length
      while (i < len) {
        ser.write(elem(i), buffer)
        i += 1
      }
    }
  }

  abstract class TraversableSerializer[T, Col <: Traversable[T]] extends BinarySerializer[Col] {
    def newBuilder(): mutable.Builder[T, Col]
    val ser: BinarySerializer[T]

    override def read(buffer: ByteArrayInput): Col = {
      val len = buffer.deserialize[Int]
      val builder = newBuilder()
      builder.sizeHint(len)
      var i = 0
      while (i < len) {
        builder += ser.read(buffer)
        i += 1
      }
      builder.result()
    }

    override def write(elem: Col, buffer: ByteArrayOutput): Unit = {
      buffer.serialize[Int](elem.size)
      elem.foreach(e => ser.write(e, buffer))
    }
  }



  implicit def traversableSerializer[T: ClassTag](implicit s: BinarySerializer[T]) = new TraversableSerializer[T, Traversable[T]] {
    override def newBuilder() = immutable.Traversable.newBuilder
    override val ser = s
  }

  implicit def listSerializer[T](implicit s: BinarySerializer[T]) = new TraversableSerializer[T, List[T]] {
    override def newBuilder() = immutable.List.newBuilder
    override val ser = s
  }

  implicit def seqSerializer[T](implicit s: BinarySerializer[T]) = new TraversableSerializer[T, Seq[T]] {
    override def newBuilder() = immutable.Seq.newBuilder
    override val ser = s
  }

  implicit def setSerializer[T](implicit s: BinarySerializer[T]) = new TraversableSerializer[T, Set[T]] {
    override def newBuilder() = immutable.Set.newBuilder
    override val ser = s
  }

  implicit def mapSerializer[K, V](implicit s: BinarySerializer[(K, V)]) = new TraversableSerializer[(K, V), Map[K, V]] {
    override def newBuilder() = immutable.Map.newBuilder
    override val ser = s
  }

  implicit def tuple2Serializer[T1, T2](implicit t1Ser: BinarySerializer[T1], t2Ser: BinarySerializer[T2]) = new BinarySerializer[(T1, T2)] {
    override def read(buffer: ByteArrayInput): (T1, T2) = (t1Ser.read(buffer), t2Ser.read(buffer))

    override def write(elem: (T1, T2), buffer: ByteArrayOutput): Unit = {
      t1Ser.write(elem._1, buffer)
      t2Ser.write(elem._2, buffer)
    }
  }

  implicit def tuple3Serializer[T1, T2, T3](implicit t1Ser: BinarySerializer[T1],
                                            t2Ser: BinarySerializer[T2],
                                            t3Ser: BinarySerializer[T3]) = new BinarySerializer[(T1, T2, T3)] {
    override def read(buffer: ByteArrayInput): (T1, T2, T3) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3), buffer: ByteArrayOutput): Unit = {
      t1Ser.write(elem._1, buffer)
      t2Ser.write(elem._2, buffer)
      t3Ser.write(elem._3, buffer)
    }
  }

  implicit def tuple4Serializer[T1, T2, T3, T4](implicit t1Ser: BinarySerializer[T1],
                                                t2Ser: BinarySerializer[T2],
                                                t3Ser: BinarySerializer[T3],
                                                t4Ser: BinarySerializer[T4]) = new BinarySerializer[(T1, T2, T3, T4)] {
    override def read(buffer: ByteArrayInput): (T1, T2, T3, T4) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer), t4Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3, T4), buffer: ByteArrayOutput): Unit = {
      t1Ser.write(elem._1, buffer)
      t2Ser.write(elem._2, buffer)
      t3Ser.write(elem._3, buffer)
      t4Ser.write(elem._4, buffer)
    }
  }

  implicit def tuple5Serializer[T1, T2, T3, T4, T5](implicit t1Ser: BinarySerializer[T1],
                                                    t2Ser: BinarySerializer[T2],
                                                    t3Ser: BinarySerializer[T3],
                                                    t4Ser: BinarySerializer[T4],
                                                    t5Ser: BinarySerializer[T5]) = new BinarySerializer[(T1, T2, T3, T4, T5)] {
    override def read(buffer: ByteArrayInput): (T1, T2, T3, T4, T5) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer), t4Ser.read(buffer), t5Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3, T4, T5), buffer: ByteArrayOutput): Unit = {
      t1Ser.write(elem._1, buffer)
      t2Ser.write(elem._2, buffer)
      t3Ser.write(elem._3, buffer)
      t4Ser.write(elem._4, buffer)
      t5Ser.write(elem._5, buffer)
    }
  }

  implicit def tuple6Serializer[T1, T2, T3, T4, T5, T6](implicit t1Ser: BinarySerializer[T1],
                                                        t2Ser: BinarySerializer[T2],
                                                        t3Ser: BinarySerializer[T3],
                                                        t4Ser: BinarySerializer[T4],
                                                        t5Ser: BinarySerializer[T5],
                                                        t6Ser: BinarySerializer[T6]) = new BinarySerializer[(T1, T2, T3, T4, T5, T6)] {
    override def read(buffer: ByteArrayInput): (T1, T2, T3, T4, T5, T6) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer), t4Ser.read(buffer), t5Ser.read(buffer), t6Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3, T4, T5, T6), buffer: ByteArrayOutput): Unit = {
      t1Ser.write(elem._1, buffer)
      t2Ser.write(elem._2, buffer)
      t3Ser.write(elem._3, buffer)
      t4Ser.write(elem._4, buffer)
      t5Ser.write(elem._5, buffer)
      t6Ser.write(elem._6, buffer)
    }
  }
}
