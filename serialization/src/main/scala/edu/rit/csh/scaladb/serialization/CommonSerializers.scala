package edu.rit.csh.scaladb.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang.{Double => D, Float => F}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object CommonSerializers {

  implicit object ByteSerializer extends Serializer[Byte] {
    override def read(buffer: ByteArrayInputStream): Byte = buffer.read().asInstanceOf[Byte]

    override def write(b: Byte, buffer: ByteArrayOutputStream): Unit = buffer.write(b)
  }

  implicit object IntSerializer extends Serializer[Int] {
    override def read(buffer: ByteArrayInputStream): Int = {
      var value = 0
      var i = 0
      var b: Int = buffer.read()
      while ((b & 0x80) != 0) {
        value |= (b & 0x7F) << i
        i += 7
        if (i > 35) {
          throw new IllegalArgumentException("Variable length quantity is too long")
        }
        b = buffer.read()
      }
      val raw = value | (b << i)
      val temp = (((raw << 31) >> 31) ^ raw) >> 1
      temp ^ (raw & (1 << 31))
    }

    override def write(i: Int, buffer: ByteArrayOutputStream): Unit = {
      var value = (i << 1) ^ (i >> 31)
      while ((value & 0xFFFFFF80) != 0L) {
        buffer.write((value & 0x7F) | 0x80)
        value = value >>> 7
      }
      buffer.write(value & 0x7F)
    }
  }

  implicit object DoubleSerializer extends Serializer[Double] {
    override def read(buffer: ByteArrayInputStream): Double = D.longBitsToDouble(LongSerializer.read(buffer))

    override def write(d: Double, buffer: ByteArrayOutputStream): Unit = LongSerializer.write(D.doubleToLongBits(d), buffer)
  }

  implicit object FloatSerializer extends Serializer[Float] {
    override def read(buffer: ByteArrayInputStream): Float = F.intBitsToFloat(IntSerializer.read(buffer))

    override def write(f: Float,  buffer: ByteArrayOutputStream): Unit = IntSerializer.write(F.floatToIntBits(f), buffer)
  }

  implicit object LongSerializer extends Serializer[Long] {
    override def read(buffer: ByteArrayInputStream): Long = {
      var value = 0L
      var i = 0
      var b: Long = buffer.read()
      while ((b & 0x80L) != 0) {
        value |= (b & 0x7F) << i
        i += 7
        if (i > 63) {
          throw new IllegalArgumentException("Variable length quantity is too long")
        }
        b = buffer.read()
      }
      val raw = value | (b << i)
      val temp = (((raw << 63) >> 63) ^ raw) >> 1
      temp ^ (raw & (1L << 63))
    }

    override def write(l: Long, buffer: ByteArrayOutputStream): Unit = {
      var value = (l << 1) ^ (l >> 63)
      while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
        buffer.write((value & 0x7F).asInstanceOf[Int] | 0x80)
        value >>>= 7
      }
      buffer.write((value & 0x7F).asInstanceOf[Int])
    }
  }

  implicit object StringSerializer extends Serializer[String] {
    override def read(buffer: ByteArrayInputStream): String = {
      val len = Serializer.read[Int](buffer)
      val bytes = new Array[Byte](len)
      buffer.read(bytes)
      new String(bytes, "UTF-8")
    }

    override def write(str: String, buffer: ByteArrayOutputStream): Unit = {
      Serializer.write[Int](str.length, buffer)
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

  implicit object RangeSerializer extends Serializer[Range] {
    override def read(buffer: ByteArrayInputStream): Range = {
      new Range(Serializer.read[Int](buffer), Serializer.read[Int](buffer), Serializer.read[Int](buffer))
    }

    override def write(elem: Range, buffer: ByteArrayOutputStream): Unit = {
      Serializer.write[Int](elem.start, buffer)
      Serializer.write[Int](elem.end, buffer)
      Serializer.write[Int](elem.step, buffer)
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

  implicit def tuple2Serializer[T1, T2](implicit t1Ser: Serializer[T1],
                                        t2Ser:Serializer[T2]) = new Serializer[(T1, T2)] {
    override def read(buffer: ByteArrayInputStream): (T1, T2) = (t1Ser.read(buffer), t2Ser.read(buffer))

    override def write(elem: (T1, T2), buffer: ByteArrayOutputStream): Unit = {
      t1Ser.write(elem._1, buffer)
      t2Ser.write(elem._2, buffer)
    }
  }

  implicit def tuple3Serializer[T1, T2, T3](implicit t1Ser: Serializer[T1],
                                            t2Ser: Serializer[T2],
                                            t3Ser: Serializer[T3]) = new Serializer[(T1, T2, T3)] {
    override def read(buffer: ByteArrayInputStream): (T1, T2, T3) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3), buffer: ByteArrayOutputStream): Unit = {
      t1Ser.write(elem._1, buffer)
      t2Ser.write(elem._2, buffer)
      t3Ser.write(elem._3, buffer)
    }
  }

  implicit def tuple4Serializer[T1, T2, T3, T4](implicit t1Ser: Serializer[T1],
                                                t2Ser: Serializer[T2],
                                                t3Ser: Serializer[T3],
                                                t4Ser: Serializer[T4]) = new Serializer[(T1, T2, T3, T4)] {
    override def read(buffer: ByteArrayInputStream): (T1, T2, T3, T4) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer), t4Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3, T4), buffer: ByteArrayOutputStream): Unit = {
      t1Ser.write(elem._1, buffer)
      t2Ser.write(elem._2, buffer)
      t3Ser.write(elem._3, buffer)
      t4Ser.write(elem._4, buffer)
    }
  }

  implicit def tuple5Serializer[T1, T2, T3, T4, T5](implicit t1Ser: Serializer[T1],
                                                    t2Ser: Serializer[T2],
                                                    t3Ser: Serializer[T3],
                                                    t4Ser: Serializer[T4],
                                                    t5Ser: Serializer[T5]) = new Serializer[(T1, T2, T3, T4, T5)] {
    override def read(buffer: ByteArrayInputStream): (T1, T2, T3, T4, T5) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer), t4Ser.read(buffer), t5Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3, T4, T5), buffer: ByteArrayOutputStream): Unit = {
      t1Ser.write(elem._1, buffer)
      t2Ser.write(elem._2, buffer)
      t3Ser.write(elem._3, buffer)
      t4Ser.write(elem._4, buffer)
      t5Ser.write(elem._5, buffer)
    }
  }
}
