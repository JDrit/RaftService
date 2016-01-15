package edu.rit.csh.scaladb.serialization.binary

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.lang.{Double => D, Float => F}

import edu.rit.csh.scaladb.serialization.binary.StaticSerializer

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.collection.immutable
import scala.collection.mutable

object DefaultBinarySerializers {


  implicit object ByteSerializer extends StaticSerializer[Byte] {

    override val size = 1

    override def read(buffer: ByteArrayInput): Byte = buffer.read().toByte

    override def write(b: Byte, offset: Int, buffer: Array[Byte]): Int = {
      buffer(offset) = b
      offset + 1
    }
  }

  object BasicIntSerializer extends StaticSerializer[Int] {
    override val size = 4
    override def read(buffer: ByteArrayInput): Int = {
      val bytes = new Array[Byte](4)
      buffer.read(bytes)
      bytes(0) << 24 | (bytes(1) & 0xFF) << 16 | (bytes(2) & 0xFF) << 8 | (bytes(3) & 0xFF)
    }

    override def write(elem: Int, offset: Int, buffer: Array[Byte]): Int = {
      buffer(offset) = (elem >> 24).toByte
      buffer(offset + 1) = (elem >> 16).toByte
      buffer(offset + 2) = (elem >> 8).toByte
      buffer(offset + 3) = elem.toByte
      offset + 4
    }
  }

  implicit object ZigZagIntSerializer extends StaticSerializer[Int] {

    override val size = 5

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


    def write(i: Int, offset: Int, buffer: Array[Byte]): Int = {
      var index = offset
      var value = (i << 1) ^ (i >> 31)
      while ((value & 0xFFFFFF80) != 0L) {
        buffer(index) = ((value & 0x7F) | 0x80).toByte
        index += 1
        value = value >>> 7
      }
      buffer(index) = (value & 0x7F).toByte
      index + 1
    }

  }

  implicit object ShortSerializer extends StaticSerializer[Short] {
    override val size = 2

    override def read(buffer: ByteArrayInput): Short = {
      ((buffer.read() << 8) + (buffer.read() & 255)).asInstanceOf[Short]
    }

    override def write(s: Short, offset: Int, buffer: Array[Byte]): Int = {
      buffer(offset) = (s >>> 8).asInstanceOf[Byte]
      buffer(offset + 1) = s.asInstanceOf[Byte]
      offset + 2
    }
  }

  implicit object CharSerializer extends StaticSerializer[Char] {
    override val size = ShortSerializer.size
    override def read(buffer: ByteArrayInput): Char = ShortSerializer.read(buffer).toChar
    override def write(c: Char, offset: Int, buffer: Array[Byte]): Int = ShortSerializer.write(c.toShort, offset, buffer)
  }

  implicit object ZigZagLongSerializer extends StaticSerializer[Long] {
    override val size = 10

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

    override def write(l: Long, offset: Int, buffer: Array[Byte]): Int = {
      var index = offset
      var value = (l << 1) ^ (l >> 63)
      while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
        buffer(index) = ((value & 0x7F).asInstanceOf[Int] | 0x80).toByte
        index += 1
        value >>>= 7
      }
      buffer(index) = (value & 0x7F).toByte
      index + 1
    }
  }

  implicit object DoubleSerializer extends StaticSerializer[Double] {
    override val size = 10
    override def read(buffer: ByteArrayInput): Double = D.longBitsToDouble(ZigZagLongSerializer.read(buffer))
    override def write(d: Double, offset: Int, buffer: Array[Byte]): Int = ZigZagLongSerializer.write(D.doubleToLongBits(d), offset, buffer)
  }

  implicit object FloatSerializer extends StaticSerializer[Float] {
    override val size = 10
    override def read(buffer: ByteArrayInput): Float = F.intBitsToFloat(ZigZagIntSerializer.read(buffer))
    override def write(f: Float, offset: Int, buffer: Array[Byte]): Int = ZigZagLongSerializer.write(F.floatToIntBits(f), offset, buffer)
  }

  implicit object StringSerializer extends DynamicSerializer[String] {

    val intSer = implicitly[StaticSerializer[Int]]
    override def size(elem: String): Int = intSer.size + elem.length

    override def read(buffer: ByteArrayInput): String = {
      val len = intSer.read(buffer)
      val str = new String(buffer.buffer, buffer.index, len, "UTF-8")
      buffer.index += len
      str
    }

    override def write(str: String, offset: Int, buffer: Array[Byte]): Int = {
      val newOffset = intSer.write(str.length, offset, buffer)
      val strBytes = str.getBytes
      val len = strBytes.length
      System.arraycopy(strBytes, 0, buffer, newOffset, len)
      newOffset + len
    }
  }

  implicit object BooleanSerializer extends StaticSerializer[Boolean] {

    override val size = 1

    override def read(buf: ByteArrayInput): Boolean = buf.read() == 1

    override def write(bool: Boolean, offset: Int, buffer: Array[Byte]): Int = {
      if (bool) {
        buffer(offset) = 1
      } else {
        buffer(offset) = 0
      }
      offset + 1
    }
  }

  implicit object RangeSerializer extends StaticSerializer[Range] {

    val intSer = implicitly[StaticSerializer[Int]]
    override val size = intSer.size * 3

    override def read(buffer: ByteArrayInput): Range = {
      new Range(intSer.read(buffer), intSer.read(buffer), intSer.read(buffer))
    }

    override def write(range: Range, offset: Int, buffer: Array[Byte]): Int = {
      var newOffset = intSer.write(range.start, offset, buffer)
      newOffset = intSer.write(range.end, newOffset, buffer)
      intSer.write(range.step, newOffset, buffer)
    }
  }

  implicit def optionSerializer[T](implicit ser: BinarySerializer[T]) = new DynamicSerializer[Option[T]] {

    val boolSer = implicitly[BinarySerializer[Boolean]]
    override def size(elem: Option[T]): Int = if (elem.isDefined) 1 + ser.size(elem.get) else 1

    override def read(buffer: ByteArrayInput): Option[T] = if (boolSer.read(buffer))
      Some(ser.read(buffer))
    else
      None

    override def write(elem: Option[T], offset: Int, buffer: Array[Byte]): Int = elem match {
      case Some(e) =>
        val newOffset = boolSer.write(true, offset, buffer)
        ser.write(e, newOffset, buffer)
      case None =>
        boolSer.write(false, offset, buffer)
    }
  }

  implicit def arraySerializer[T: ClassTag](implicit ser: BinarySerializer[T]) = new DynamicSerializer[Array[T]] {

    val intSer = implicitly[StaticSerializer[Int]]
    override def size(elem: Array[T]): Int = ser match {
      case ser: StaticSerializer[T] => intSer.size + elem.length * ser.size
      case ser: DynamicSerializer[T] =>
        var size = 0
        var i = 0
        val len = elem.length
        while (i < len) {
          size += ser.size(elem(i))
          i += 1
        }
        size + intSer.size
    }

    override def read(buffer: ByteArrayInput): Array[T] = {
      val len = intSer.read(buffer)
      val arr = new Array[T](len)
      var i = 0
      while (i < len) {
        arr(i) = ser.read(buffer)
        i += 1
      }
      arr
    }

    override def write(elem: Array[T], offset: Int, buffer: Array[Byte]): Int = {
      val elemLength = elem.length
      var newOffset = intSer.write(elemLength, offset, buffer)
      var i = 0
      while (i < elemLength) {
        newOffset = ser.write(elem(i), newOffset, buffer)
        i += 1
      }
      newOffset
    }
  }

  abstract class TraversableSerializer[T, Col <: Traversable[T]] extends DynamicSerializer[Col] {
    def newBuilder(): mutable.Builder[T, Col]
    val ser: BinarySerializer[T]
    val intSer = implicitly[StaticSerializer[Int]]

    override def size(elem: Col): Int = ser match {
      case ser: StaticSerializer[T] => intSer.size + elem.size * ser.size
      case ser: DynamicSerializer[T] =>
        var size = intSer.size
        elem.foreach(e => size += ser.size(e))
        size
    }

    override def read(buffer: ByteArrayInput): Col = {
      val len = intSer.read(buffer)
      val builder = newBuilder()
      builder.sizeHint(len)
      var i = 0
      while (i < len) {
        builder += ser.read(buffer)
        i += 1
      }
      builder.result()
    }

    override def write(elem: Col, offset: Int, buffer: Array[Byte]): Int = {
      var newOffset = intSer.write(elem.size, offset, buffer)
      val itr = elem.toIterator
      while (itr.hasNext) {
        newOffset = ser.write(itr.next(), newOffset, buffer)
      }
      newOffset
    }
  }

  implicit def traversableSerializer[T: ClassTag](implicit s: BinarySerializer[T]) = new TraversableSerializer[T, Traversable[T]] {
    override def newBuilder() = immutable.Traversable.newBuilder
    override val ser = s
  }

  implicit def listSerializer[T](implicit s: BinarySerializer[T]) = new TraversableSerializer[T, List[T]] {
    override def newBuilder() = immutable.List.newBuilder
    override val ser = s

    @tailrec
    private def loop(lst: List[T], offset: Int, buffer: Array[Byte]): Int = lst match {
      case x :: xs => loop(xs, ser.write(x, offset, buffer), buffer)
      case Nil => offset
    }

    override def write(lst: List[T], offset: Int, buffer: Array[Byte]): Int = {
      loop(lst, intSer.write(lst.size, offset, buffer), buffer)
    }
  }

  implicit def seqSerializer[T](implicit s: BinarySerializer[T]) = new TraversableSerializer[T, Seq[T]] {
    override def newBuilder() = immutable.Seq.newBuilder
    override val ser = s
  }

  implicit def setSerializer[T](implicit s: BinarySerializer[T]) = new TraversableSerializer[T, Set[T]] {
    override def newBuilder() = immutable.Set.newBuilder
    override val ser = s
  }

  implicit def mapSerializer[K, V](implicit keySer: BinarySerializer[K], valSer: BinarySerializer[V]) = new DynamicSerializer[Map[K, V]] {
    val intSer = implicitly[StaticSerializer[Int]]

    override def size(elem: Map[K, V]): Int = (keySer, valSer) match {
      case (keySer: DynamicSerializer[K], valSer: DynamicSerializer[V]) =>
        val itr = elem.iterator
        var size = intSer.size
        while (itr.hasNext) {
          val (k, v) = itr.next()
          size += keySer.size(k) + valSer.size(v)
        }
        size
      case (keySer: StaticSerializer[K], valSer: DynamicSerializer[V]) =>
        val itr = elem.valuesIterator
        var size = intSer.size + keySer.size * elem.size
        while (itr.hasNext) {
          size += valSer.size(itr.next())
        }
        size
      case (keySer: DynamicSerializer[K], valSer: StaticSerializer[V]) =>
        val itr = elem.keysIterator
        var size = intSer.size + valSer.size * elem.size
        while (itr.hasNext) {
          size += keySer.size(itr.next())
        }
        size
      case (keySer: StaticSerializer[K], valSer: StaticSerializer[V]) =>
        intSer.size + (keySer.size + valSer.size) * elem.size
    }

    override def read(buffer: ByteArrayInput): Map[K, V] = {
      val len = intSer.read(buffer)
      val map = new mutable.HashMap[K, V]
      map.sizeHint(len)

      var i = 0
      while (i < len) {
        map.put(keySer.read(buffer), valSer.read(buffer))
        i += 1
      }
      map.toMap
    }

    override def write(elem: Map[K, V], offset: Int, buffer: Array[Byte]): Int = {
      var newOffset = intSer.write(elem.size, offset, buffer)
      val itr = elem.iterator
      while (itr.hasNext) {
        val (key, value) = itr.next()
        newOffset = keySer.write(key, newOffset, buffer)
        newOffset = valSer.write(value, newOffset, buffer)
      }
      newOffset
    }
  }

  implicit def tuple2Serializer[T1, T2](implicit t1Ser: BinarySerializer[T1], t2Ser: BinarySerializer[T2]) = new DynamicSerializer[(T1, T2)] {

    override def size(elem: (T1, T2)): Int = t1Ser.size(elem._1) + t2Ser.size(elem._2)

    override def read(buffer: ByteArrayInput): (T1, T2) = (t1Ser.read(buffer), t2Ser.read(buffer))

    override def write(elem: (T1, T2), offset: Int, buffer: Array[Byte]): Int = {
      t2Ser.write(elem._2, t1Ser.write(elem._1, offset, buffer), buffer)
    }
  }

  implicit def tuple3Serializer[T1, T2, T3](implicit t1Ser: BinarySerializer[T1],
                                            t2Ser: BinarySerializer[T2],
                                            t3Ser: BinarySerializer[T3]) = new BinarySerializer[(T1, T2, T3)] {

    override def size(elem: (T1, T2, T3)): Int = t1Ser.size(elem._1) + t2Ser.size(elem._2) + t3Ser.size(elem._3)

    override def read(buffer: ByteArrayInput): (T1, T2, T3) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3), offset: Int, buffer: Array[Byte]): Int = {
      var newOffset = t1Ser.write(elem._1, offset, buffer)
      newOffset = t2Ser.write(elem._2, newOffset, buffer)
      t3Ser.write(elem._3, newOffset, buffer)
    }
  }

  implicit def tuple4Serializer[T1, T2, T3, T4](implicit t1Ser: BinarySerializer[T1],
                                                t2Ser: BinarySerializer[T2],
                                                t3Ser: BinarySerializer[T3],
                                                t4Ser: BinarySerializer[T4]) = new BinarySerializer[(T1, T2, T3, T4)] {

    override def size(elem: (T1, T2, T3, T4)): Int = t1Ser.size(elem._1) + t2Ser.size(elem._2) +
      t3Ser.size(elem._3) + t4Ser.size(elem._4)

    override def read(buffer: ByteArrayInput): (T1, T2, T3, T4) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer), t4Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3, T4), offset: Int, buffer: Array[Byte]): Int = {
      var newOffset = t1Ser.write(elem._1, offset, buffer)
      newOffset = t2Ser.write(elem._2, newOffset, buffer)
      newOffset = t3Ser.write(elem._3, newOffset, buffer)
      t4Ser.write(elem._4, newOffset, buffer)
    }
  }

  implicit def tuple5Serializer[T1, T2, T3, T4, T5](implicit t1Ser: BinarySerializer[T1],
                                                    t2Ser: BinarySerializer[T2],
                                                    t3Ser: BinarySerializer[T3],
                                                    t4Ser: BinarySerializer[T4],
                                                    t5Ser: BinarySerializer[T5]) = new BinarySerializer[(T1, T2, T3, T4, T5)] {

    override def size(elem: (T1, T2, T3, T4, T5)): Int = t1Ser.size(elem._1) + t2Ser.size(elem._2) +
      t3Ser.size(elem._3) + t4Ser.size(elem._4) + t5Ser.size(elem._5)

    override def read(buffer: ByteArrayInput): (T1, T2, T3, T4, T5) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer), t4Ser.read(buffer), t5Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3, T4, T5), offset: Int, buffer: Array[Byte]): Int = {
      var newOffset = t1Ser.write(elem._1, offset, buffer)
      newOffset = t2Ser.write(elem._2, newOffset, buffer)
      newOffset = t3Ser.write(elem._3, newOffset, buffer)
      newOffset = t4Ser.write(elem._4, newOffset, buffer)
      t5Ser.write(elem._5, newOffset, buffer)
    }
  }

  implicit def tuple6Serializer[T1, T2, T3, T4, T5, T6](implicit t1Ser: BinarySerializer[T1],
                                                        t2Ser: BinarySerializer[T2],
                                                        t3Ser: BinarySerializer[T3],
                                                        t4Ser: BinarySerializer[T4],
                                                        t5Ser: BinarySerializer[T5],
                                                        t6Ser: BinarySerializer[T6]) = new BinarySerializer[(T1, T2, T3, T4, T5, T6)] {
    override def size(elem: (T1, T2, T3, T4, T5, T6)): Int = t1Ser.size(elem._1) + t2Ser.size(elem._2) +
      t3Ser.size(elem._3) + t4Ser.size(elem._4) + t5Ser.size(elem._5) + t6Ser.size(elem._6)

    override def read(buffer: ByteArrayInput): (T1, T2, T3, T4, T5, T6) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer), t4Ser.read(buffer), t5Ser.read(buffer), t6Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3, T4, T5, T6), offset: Int, buffer: Array[Byte]): Int = {
      var newOffset = t1Ser.write(elem._1, offset, buffer)
      newOffset = t2Ser.write(elem._2, newOffset, buffer)
      newOffset = t3Ser.write(elem._3, newOffset, buffer)
      newOffset = t4Ser.write(elem._4, newOffset, buffer)
      newOffset = t5Ser.write(elem._5, newOffset, buffer)
      t6Ser.write(elem._6, newOffset, buffer)
    }
  }
}
