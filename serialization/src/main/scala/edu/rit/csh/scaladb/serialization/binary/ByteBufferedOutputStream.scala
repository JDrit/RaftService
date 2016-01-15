package edu.rit.csh.scaladb.serialization.binary

import java.io.OutputStream
import java.nio.ByteBuffer

class ByteBufferedOutputStream(size: Int = 32) extends OutputStream {

  private var buffer: ByteBuffer = ByteBuffer.allocate(size)
  private var n = 0

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    ensureSize(len - off)
    buffer.put(b, off, len)
  }

  override def write(b: Int) {
    if (!buffer.hasRemaining) {
      n += 1
      increase(buffer.capacity() * math.pow(2, n).toInt)
    }
    buffer.put(b.toByte)
  }

  protected def increase(newCapacity: Int) {
    buffer.limit(buffer.position())
    buffer.rewind()
    var newBuffer: ByteBuffer = null
    newBuffer = ByteBuffer.allocate(newCapacity)
    newBuffer.put(buffer)
    buffer.clear()
    buffer = newBuffer
  }

  def capacity(): Long = buffer.capacity()

  @inline
  def ensureSize(len: Int): Unit = {
    val position = buffer.position()
    val limit = buffer.limit()
    val newTotal = position + len
    if (newTotal > limit) {
      val curCapacity = buffer.capacity()
      n = (math.log(newTotal / curCapacity) / math.log(2)).toInt + 1
      val capacity = curCapacity * math.pow(2, n).toInt
      increase(capacity)
    }
  }

  /**
   * Returns a byte array of the written data so far. This is a new copy of the data
   * @return
   */
  def array(): Array[Byte] = buffer.array()
}