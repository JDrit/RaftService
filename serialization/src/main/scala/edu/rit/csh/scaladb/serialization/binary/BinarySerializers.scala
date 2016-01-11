package edu.rit.csh.scaladb.serialization.binary

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.lang.{Double => D, Float => F}

import scala.reflect.ClassTag

object BinarySerializers {

  implicit object ByteSerializer extends BinarySerializer[Byte] {
    override def read(buffer: ByteBufferInput): Byte = buffer.next.asInstanceOf[Byte]

    override def write(b: Byte, buffer: ByteBufferOutput): Unit = buffer.add(b)
  }

  implicit object IntSerializer extends BinarySerializer[Int] {
    override def read(buffer: ByteBufferInput): Int = {
      var value = 0
      var i = 0
      var b: Int = buffer.next
      while ((b & 0x80) != 0) {
        value |= (b & 0x7F) << i
        i += 7
        if (i > 35) {
          throw new IllegalArgumentException("Variable length quantity is too long")
        }
        b = buffer.next()
      }
      val raw = value | (b << i)
      val temp = (((raw << 31) >> 31) ^ raw) >> 1
      temp ^ (raw & (1 << 31))
    }

    override def write(i: Int, buffer: ByteBufferOutput): Unit = {
      var value = (i << 1) ^ (i >> 31)
      while ((value & 0xFFFFFF80) != 0L) {
        buffer.add((value & 0x7F) | 0x80)
        value = value >>> 7
      }
      buffer.add(value & 0x7F)
    }
  }

  implicit object LongSerializer extends BinarySerializer[Long] {
    override def read(buffer: ByteBufferInput): Long = {
      var value = 0L
      var i = 0
      var b: Long = buffer.next()
      while ((b & 0x80L) != 0) {
        value |= (b & 0x7F) << i
        i += 7
        if (i > 63) {
          throw new IllegalArgumentException("Variable length quantity is too long")
        }
        b = buffer.next()
      }
      val raw = value | (b << i)
      val temp = (((raw << 63) >> 63) ^ raw) >> 1
      temp ^ (raw & (1L << 63))
    }

    override def write(l: Long, buffer: ByteBufferOutput): Unit = {
      var value = (l << 1) ^ (l >> 63)
      while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
        buffer.add((value & 0x7F).asInstanceOf[Int] | 0x80)
        value >>>= 7
      }
      buffer.add((value & 0x7F).asInstanceOf[Int])
    }
  }

  implicit object DoubleSerializer extends BinarySerializer[Double] {
    override def read(buffer: ByteBufferInput): Double = D.longBitsToDouble(LongSerializer.read(buffer))

    override def write(d: Double, buffer: ByteBufferOutput): Unit = LongSerializer.write(D.doubleToLongBits(d), buffer)
  }

  implicit object FloatSerializer extends BinarySerializer[Float] {
    override def read(buffer: ByteBufferInput): Float = F.intBitsToFloat(IntSerializer.read(buffer))

    override def write(f: Float,  buffer: ByteBufferOutput): Unit = IntSerializer.write(F.floatToIntBits(f), buffer)
  }

  implicit object StringSerializer extends BinarySerializer[String] {
    override def read(buffer: ByteBufferInput): String = {
      //val len = Serializer.read[Int](buffer)
      val len = buffer.deserialize[Int]
      val bytes = new Array[Int](len)
      buffer.nextArray(bytes)
      new String(bytes.map(_.toByte), "UTF-8")
    }

    override def write(str: String, buffer: ByteBufferOutput): Unit = {
      //Serializer.write[Int](str.length, buffer)
      //buffer.write(str.getBytes)
      buffer.serialize[Int](str.length)
      buffer.addArray(str.getBytes.map(_.toInt))
    }
  }

  implicit object BooleanSerializer extends BinarySerializer[Boolean] {
    override def read(buf: ByteBufferInput): Boolean = buf.next() == 1

    override def write(bool: Boolean, buf: ByteBufferOutput): Unit = buf.add(if (bool) 1 else 0)
  }

  implicit def function1Serializer[A, R] = new BinarySerializer[A => R] {
    override def read(buffer: ByteBufferInput): (A) => R = {
      new ObjectInputStream(buffer).readObject().asInstanceOf[A => R]
    }

    override def write(elem: (A) => R, buffer: ByteBufferOutput): Unit = {
      new ObjectOutputStream(buffer).writeObject(elem)
    }
  }

  /*implicit def function2Serializer[A1, A2, R] = new BinarySerializer[(A1, A2) => R] {
    override def read(buffer: ByteBufferInput): (A1, A2) => R = {
      new ObjectInputStream(buffer).readObject().asInstanceOf[(A1, A2) => R]
    }

    override def write(elem: (A1, A2) => R, buffer: ByteBufferOutput): Unit = {
      new ObjectOutputStream(buffer).writeObject(elem)
    }
  }*/

  implicit object RangeSerializer extends BinarySerializer[Range] {
    override def read(buffer: ByteBufferInput): Range = {
      new Range(buffer.deserialize[Int], buffer.deserialize[Int], buffer.deserialize[Int])
    }

    override def write(elem: Range, buffer: ByteBufferOutput): Unit = {
      buffer.serialize(elem.start)
      buffer.serialize(elem.end)
      buffer.serialize(elem.step)
    }
  }

  implicit def arraySerializer[T: ClassTag](implicit ser: BinarySerializer[T]) = new BinarySerializer[Array[T]] {
    override def read(buffer: ByteBufferInput): Array[T] = {
      val len = buffer.deserialize[Int]
      val arr = new Array[T](len)
      (0 until len).foreach(i => arr(i) = ser.read(buffer))
      arr
    }

    override def write(elem: Array[T], buffer: ByteBufferOutput): Unit = {
      buffer.serialize(elem.length)
      elem.foreach(e => ser.write(e, buffer))
    }
  }

  implicit def optionSerializer[T](implicit ser: BinarySerializer[T]) = new BinarySerializer[Option[T]] {
    override def read(buffer: ByteBufferInput): Option[T] = if (buffer.deserialize[Boolean])
      Some(buffer.deserialize[T])
    else
      None

    override def write(elem: Option[T], buffer: ByteBufferOutput): Unit = elem match {
      case Some(e) =>
        buffer.serialize[Boolean](true)
        buffer.serialize[T](e)
      case None =>
        buffer.serialize[Boolean](false)
    }
  }

  implicit def traversableSerializer[T: ClassTag](implicit ser: BinarySerializer[T]) = new BinarySerializer[Traversable[T]] {
    override def read(buffer: ByteBufferInput): Traversable[T] = {
      val len = buffer.deserialize[Int]
      val arr = new Array[T](len)
      (0 until len).foreach(i => arr(i) = ser.read(buffer))
      arr.toTraversable
    }

    override def write(elem: Traversable[T], buffer: ByteBufferOutput): Unit = {
      buffer.serialize[Int](elem.size)
      elem.foreach(e => ser.write(e, buffer))
    }
  }

  implicit def listSerializer[T](implicit ser: BinarySerializer[Traversable[T]]) = new BinarySerializer[List[T]] {
    override def read(buffer: ByteBufferInput): List[T] = ser.read(buffer).toList

    override def write(elem: List[T], buffer: ByteBufferOutput): Unit = ser.write(elem.toTraversable, buffer)
  }


  implicit def setSerializer[T](implicit ser: BinarySerializer[Traversable[T]]) = new BinarySerializer[Set[T]] {
    override def read(buffer: ByteBufferInput): Set[T] = ser.read(buffer).toSet

    override def write(elem: Set[T], buffer: ByteBufferOutput): Unit = ser.write(elem.toTraversable, buffer)
  }

  implicit def mapSerializer[K, V](implicit keySer: BinarySerializer[K], valSer: BinarySerializer[V]) = new BinarySerializer[Map[K, V]] {
    override def read(buffer: ByteBufferInput): Map[K, V] =
      (0 until buffer.deserialize[Int]).map(_ =>(keySer.read(buffer), valSer.read(buffer))).toMap

    override def write(elem: Map[K, V], buffer: ByteBufferOutput): Unit = {
      buffer.serialize[Int](elem.size)
      elem.foreach { case (k, v) =>
          keySer.write(k, buffer)
          valSer.write(v, buffer)
      }
    }
  }

  implicit def tuple2Serializer[T1, T2](implicit t1Ser: BinarySerializer[T1],
                                        t2Ser: BinarySerializer[T2]) = new BinarySerializer[(T1, T2)] {
    override def read(buffer: ByteBufferInput): (T1, T2) = (t1Ser.read(buffer), t2Ser.read(buffer))

    override def write(elem: (T1, T2), buffer: ByteBufferOutput): Unit = {
      t1Ser.write(elem._1, buffer)
      t2Ser.write(elem._2, buffer)
    }
  }

  implicit def tuple3Serializer[T1, T2, T3](implicit t1Ser: BinarySerializer[T1],
                                            t2Ser: BinarySerializer[T2],
                                            t3Ser: BinarySerializer[T3]) = new BinarySerializer[(T1, T2, T3)] {
    override def read(buffer: ByteBufferInput): (T1, T2, T3) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3), buffer: ByteBufferOutput): Unit = {
      t1Ser.write(elem._1, buffer)
      t2Ser.write(elem._2, buffer)
      t3Ser.write(elem._3, buffer)
    }
  }

  implicit def tuple4Serializer[T1, T2, T3, T4](implicit t1Ser: BinarySerializer[T1],
                                                t2Ser: BinarySerializer[T2],
                                                t3Ser: BinarySerializer[T3],
                                                t4Ser: BinarySerializer[T4]) = new BinarySerializer[(T1, T2, T3, T4)] {
    override def read(buffer: ByteBufferInput): (T1, T2, T3, T4) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer), t4Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3, T4), buffer: ByteBufferOutput): Unit = {
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
    override def read(buffer: ByteBufferInput): (T1, T2, T3, T4, T5) = {
      (t1Ser.read(buffer), t2Ser.read(buffer), t3Ser.read(buffer), t4Ser.read(buffer), t5Ser.read(buffer))
    }

    override def write(elem: (T1, T2, T3, T4, T5), buffer: ByteBufferOutput): Unit = {
      t1Ser.write(elem._1, buffer)
      t2Ser.write(elem._2, buffer)
      t3Ser.write(elem._3, buffer)
      t4Ser.write(elem._4, buffer)
      t5Ser.write(elem._5, buffer)
    }
  }
}
