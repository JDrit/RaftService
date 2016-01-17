package edu.rit.csh.scaladb.serialization.binary

import java.lang.{Double => D, Float => F}

import scala.reflect.ClassTag
import scala.collection.immutable
import scala.collection.mutable

object DefaultBinarySerializers {

  implicit object ByteSerializer extends StaticSerializer[Byte] {
    override val size = 1

    override def read(buffer: ByteArrayInput): Byte = buffer.read().toByte
    override def write(b: Byte, output: BinaryOutput): Unit = output.write(b)
  }

  object BasicIntSerializer extends StaticSerializer[Int] {
    override val size = 4

    override def read(buffer: ByteArrayInput): Int = {
      val bytes = new Array[Byte](4)
      buffer.read(bytes)
      bytes(0) << 24 | (bytes(1) & 0xFF) << 16 | (bytes(2) & 0xFF) << 8 | (bytes(3) & 0xFF)
    }

    override def write(elem: Int, output: BinaryOutput): Unit = {
      output.write(elem >> 24)
      output.write(elem >> 16)
      output.write(elem >> 8)
      output.write(elem)
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
    def write(i: Int, output: BinaryOutput): Unit = {
      var value = (i << 1) ^ (i >> 31)
      while ((value & 0xFFFFFF80) != 0L) {
        output.write((value & 0x7F) | 0x80)
        value = value >>> 7
      }
      output.write(value & 0x7F)
    }
  }

  implicit object ShortSerializer extends StaticSerializer[Short] {
    override val size = 2
    override def read(buffer: ByteArrayInput): Short = {
      ((buffer.read() << 8) + (buffer.read() & 255)).asInstanceOf[Short]
    }
    override def write(s: Short, output: BinaryOutput): Unit = {
      output.write(s >>> 8)
      output.write(s)
    }
  }

  implicit object CharSerializer extends StaticSerializer[Char] {
    override val size = ShortSerializer.size
    override def read(buffer: ByteArrayInput): Char = ShortSerializer.read(buffer).toChar
    override def write(c: Char, output: BinaryOutput): Unit = ShortSerializer.write(c.toShort, output)
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

    override def write(l: Long, output: BinaryOutput): Unit = {
      var value = (l << 1) ^ (l >> 63)
      while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
        output.write((value & 0x7F).asInstanceOf[Int] | 0x80)
        value >>>= 7
      }
      output.write((value & 0x7F).toInt)
    }
  }

  implicit object DoubleSerializer extends StaticSerializer[Double] {
    override val size = ZigZagLongSerializer.size
    override def read(buffer: ByteArrayInput): Double = D.longBitsToDouble(ZigZagLongSerializer.read(buffer))
    override def write(d: Double, output: BinaryOutput): Unit = ZigZagLongSerializer.write(D.doubleToLongBits(d), output)
  }

  implicit object FloatSerializer extends StaticSerializer[Float] {
    override val size = ZigZagIntSerializer.size
    override def read(buffer: ByteArrayInput): Float = F.intBitsToFloat(ZigZagIntSerializer.read(buffer))
    override def write(f: Float, output: BinaryOutput): Unit = ZigZagLongSerializer.write(F.floatToIntBits(f), output)
  }

  implicit object BooleanSerializer extends StaticSerializer[Boolean] {
    override val size = 1
    override def read(buf: ByteArrayInput): Boolean = buf.read() == 1
    override def write(bool: Boolean, output: BinaryOutput): Unit = if (bool) {
      output.write(1)
    } else {
      output.write(0)
    }
  }

  implicit object StringSerializer extends DynamicSerializer[String] {
    val intSer = implicitly[StaticSerializer[Int]]

    override def getSize(str: String): Int = intSer.size + str.length

    override def canGetSize: Boolean = false

    override def read(buffer: ByteArrayInput): String = {
      val len = intSer.read(buffer)
      val str = new String(buffer.buffer, buffer.index, len, "UTF-8")
      buffer.index += len
      str
    }

    override def write(str: String, output: BinaryOutput): Unit = {
      intSer.write(str.length, output)
      output.write(str.getBytes)
    }
  }



  implicit object RangeSerializer extends StaticSerializer[Range] {
    val intSer = implicitly[StaticSerializer[Int]]
    override val size = intSer.size * 3

    override def read(buffer: ByteArrayInput): Range = {
      new Range(intSer.read(buffer), intSer.read(buffer), intSer.read(buffer))
    }

    override def write(range: Range, output: BinaryOutput): Unit = {
      intSer.write(range.start, output)
      intSer.write(range.end, output)
      intSer.write(range.step, output)
    }
  }

  implicit def optionSerializer[T](implicit ser: BinarySerializer[T]) = new BinarySerializer[Option[T]] {
    val boolSer = implicitly[StaticSerializer[Boolean]]

    override val staticSize: Option[Int] = ser.staticSize.map(i => i + boolSer.size)

    override val canGetSize: Boolean = ser.canGetSize

    override def getSize(elem: Option[T]): Int = elem match {
      case Some(e) => ser.getSize(e) + boolSer.size
      case None => boolSer.size
    }

    override def read(buffer: ByteArrayInput): Option[T] = if (boolSer.read(buffer)) Some(ser.read(buffer)) else None

    override def write(elem: Option[T], output: BinaryOutput): Unit = elem match {
      case Some(e) => boolSer.write(true, output) ; ser.write(e, output)
      case None => boolSer.write(false, output)
    }
  }

  implicit def arraySerializer[T: ClassTag](implicit ser: BinarySerializer[T]) = new DynamicSerializer[Array[T]] {
    val intSer = implicitly[StaticSerializer[Int]]

    override def canGetSize = ser.canGetSize

    override def getSize(arr: Array[T]): Int = ser.staticSize match {
      case Some(i) => i * arr.length + intSer.size
      case None =>
        var size = intSer.size
        var i = 0
        val len = arr.length
        while (i < len) {
          size += ser.getSize(arr(i))
          i += 1
        }
        size
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

    override def write(elem: Array[T], output: BinaryOutput): Unit = {
      val len = elem.length
      intSer.write(len, output)
      var i = 0
      while (i < len) {
        ser.write(elem(i), output)
        i += 1
      }
    }
  }

  abstract class TraversableSerializer[T, Col <: Traversable[T]] extends DynamicSerializer[Col] {
    def newBuilder(): mutable.Builder[T, Col]
    val ser: BinarySerializer[T]
    val intSer = implicitly[StaticSerializer[Int]]

    override def canGetSize = ser.canGetSize

    override def getSize(trav: Col): Int = ser.staticSize match {
      case Some(i) => i * trav.size + intSer.size
      case None =>
        var size = intSer.size
        trav.foreach(e => size += ser.getSize(e))
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

    override def write(elem: Col, output: BinaryOutput): Unit = {
      intSer.write(elem.size, output)
      elem.foreach(e => ser.write(e, output))
    }
  }

  implicit def traversableSerializer[T](implicit s: BinarySerializer[T]) = new TraversableSerializer[T, Traversable[T]] {
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

  implicit def mapSerializer[K, V](implicit keySer: BinarySerializer[K], valSer: BinarySerializer[V]) = new DynamicSerializer[Map[K, V]] {
    val intSer = implicitly[StaticSerializer[Int]]
    val keyTrav = traversableSerializer[K]
    val valTrav = traversableSerializer[V]

    override def canGetSize = keySer.canGetSize && valSer.canGetSize

    override def getSize(map: Map[K,V]): Int = {
      var size = intSer.size
      map.foreach(entry => size += keySer.getSize(entry._1) + valSer.getSize(entry._2))
      size
    }

    override def read(input: ByteArrayInput): Map[K, V] = {
      val builder = immutable.Map.newBuilder[K, V]
      val len = intSer.read(input)
      builder.sizeHint(len)
      var i = 0
      while (i < len) {
        builder += ((keySer.read(input), valSer.read(input)))
        i += 1
      }
      builder.result()
    }

    override def write(map: Map[K, V], output: BinaryOutput): Unit = {
      intSer.write(map.size, output)
      map.foreach { entry =>
        keySer.write(entry._1, output)
        valSer.write(entry._2, output)
      }
    }
  }

  /*

  implicit def tuple2Serializer[T1, T2](implicit t1Ser: BinarySerializer[T1],
                                        t2Ser: BinarySerializer[T2]) = new BinarySerializer[(T1, T2)] {

    override val staticSize: Option[Int] = for {
      size1 <- t1Ser.staticSize
      size2 <- t2Ser.staticSize
    } yield {
      size1 + size2
    }

    override def canTestSize = t1Ser.canTestSize && t2Ser.canTestSize

    override def getSize(elem: (T1, T2)): Int = staticSize.getOrElse(t1Ser.getSize(elem._1) + t2Ser.getSize(elem._2))

    override def read(buffer: ByteArrayInput): (T1, T2) = (t1Ser.read(buffer), t2Ser.read(buffer))

    override def write(elem: (T1, T2), output: BinaryOutput): Unit = {
      t1Ser.write(elem._1, output)
      t2Ser.write(elem._2, output)
    }
  }


  implicit def tuple3Serializer[T1, T2, T3](implicit t1Ser: BinarySerializer[T1],
                                            t2Ser: BinarySerializer[T2],
                                            t3Ser: BinarySerializer[T3]) = new BinarySerializer[(T1, T2, T3)] {

    override val staticSize: Option[Int] = for {
      size1 <- t1Ser.staticSize
      size2 <- t2Ser.staticSize
      size3 <- t3Ser.staticSize
    } yield {
      size1 + size2 + size3
    }

    override def getSize(elem: (T1, T2, T3)): Int = staticSize.getOrElse(t1Ser.getSize(elem._1) +
      t2Ser.getSize(elem._2) + t3Ser.getSize(elem._3))


    override def read(buffer: ByteArrayInput): (T1, T2, T3) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3), output: BinaryOutput): Unit = {
      t1Ser.write(elem._1, output)
      t2Ser.write(elem._2, output)
      t3Ser.write(elem._3, output)
    }
  }


  implicit def tuple4Serializer[T1, T2, T3, T4](implicit t1Ser: BinarySerializer[T1],
                                                t2Ser: BinarySerializer[T2],
                                                t3Ser: BinarySerializer[T3],
                                                t4Ser: BinarySerializer[T4]) = new BinarySerializer[(T1, T2, T3, T4)] {

    override val staticSize: Option[Int] = for {
      size1 <- t1Ser.staticSize
      size2 <- t2Ser.staticSize
      size3 <- t3Ser.staticSize
      size4 <- t4Ser.staticSize
    } yield {
      size1 + size2 + size3 + size4
    }

    override def getSize(elem: (T1, T2, T3, T4)): Int = staticSize.getOrElse(t1Ser.getSize(elem._1) +
      t2Ser.getSize(elem._2) + t3Ser.getSize(elem._3) + t4Ser.getSize(elem._4))

    override def read(buffer: ByteArrayInput): (T1, T2, T3, T4) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer), t4Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3, T4), output: BinaryOutput): Unit = {
      t1Ser.write(elem._1, output)
      t2Ser.write(elem._2, output)
      t3Ser.write(elem._3, output)
      t4Ser.write(elem._4, output)
    }
  }

  implicit def tuple5Serializer[T1, T2, T3, T4, T5](implicit t1Ser: BinarySerializer[T1],
                                                    t2Ser: BinarySerializer[T2],
                                                    t3Ser: BinarySerializer[T3],
                                                    t4Ser: BinarySerializer[T4],
                                                    t5Ser: BinarySerializer[T5]) = new BinarySerializer[(T1, T2, T3, T4, T5)] {

    override val staticSize: Option[Int] = for {
      size1 <- t1Ser.staticSize
      size2 <- t2Ser.staticSize
      size3 <- t3Ser.staticSize
      size4 <- t4Ser.staticSize
      size5 <- t5Ser.staticSize
    } yield {
      size1 + size2 + size3 + size4 + size5
    }

    override def getSize(elem: (T1, T2, T3, T4, T5)): Int = staticSize.getOrElse(t1Ser.getSize(elem._1) +
      t2Ser.getSize(elem._2) + t3Ser.getSize(elem._3) + t4Ser.getSize(elem._4) + t5Ser.getSize(elem._5))

    override def read(buffer: ByteArrayInput): (T1, T2, T3, T4, T5) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer), t4Ser.read(buffer), t5Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3, T4, T5), output: BinaryOutput): Unit = {
      t1Ser.write(elem._1, output)
      t2Ser.write(elem._2, output)
      t3Ser.write(elem._3, output)
      t4Ser.write(elem._4, output)
      t5Ser.write(elem._5, output)
    }
  }

  implicit def tuple6Serializer[T1, T2, T3, T4, T5, T6](implicit t1Ser: BinarySerializer[T1],
                                                        t2Ser: BinarySerializer[T2],
                                                        t3Ser: BinarySerializer[T3],
                                                        t4Ser: BinarySerializer[T4],
                                                        t5Ser: BinarySerializer[T5],
                                                        t6Ser: BinarySerializer[T6]) = new BinarySerializer[(T1, T2, T3, T4, T5, T6)] {

    override val staticSize: Option[Int] = for {
      size1 <- t1Ser.staticSize
      size2 <- t2Ser.staticSize
      size3 <- t3Ser.staticSize
      size4 <- t4Ser.staticSize
      size5 <- t5Ser.staticSize
      size6 <- t6Ser.staticSize
    } yield {
        size1 + size2 + size3 + size4 + size5 + size6
    }

    override def getSize(elem: (T1, T2, T3, T4, T5, T6)): Int = staticSize.getOrElse(t1Ser.getSize(elem._1) +
      t2Ser.getSize(elem._2) + t3Ser.getSize(elem._3) + t4Ser.getSize(elem._4) + t5Ser.getSize(elem._5) +
      t6Ser.getSize(elem._6))


    override def read(buffer: ByteArrayInput): (T1, T2, T3, T4, T5, T6) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer), t4Ser.read(buffer), t5Ser.read(buffer), t6Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3, T4, T5, T6), output: BinaryOutput): Unit = {
      t1Ser.write(elem._1, output)
      t2Ser.write(elem._2, output)
      t3Ser.write(elem._3, output)
      t4Ser.write(elem._4, output)
      t5Ser.write(elem._5, output)
      t6Ser.write(elem._6, output)
    }
  }*/

}
