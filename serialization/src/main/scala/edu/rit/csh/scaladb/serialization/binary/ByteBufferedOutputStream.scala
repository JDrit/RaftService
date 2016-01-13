package edu.rit.csh.scaladb.serialization.binary

import java.io.OutputStream
import java.nio.ByteBuffer

import ByteBufferedOutputStream._

object ByteBufferedOutputStream {
  val DEFAULT_INCREASING_FACTOR = 1.5f
}

class ByteBufferedOutputStream(size: Int, increasingBy: Float, private var onHeap: Boolean) extends OutputStream {

  var buffer: ByteBuffer = if (onHeap) ByteBuffer.allocate(size) else ByteBuffer.allocateDirect(size)

  private val increasing: Float = DEFAULT_INCREASING_FACTOR

  if (increasingBy <= 1) {
    throw new IllegalArgumentException("Increasing Factor must be greater than 1.0")
  }

  def this(size: Int) {
    this(size, DEFAULT_INCREASING_FACTOR, false)
  }

  def this(size: Int, onHeap: Boolean) {
    this(size, DEFAULT_INCREASING_FACTOR, onHeap)
  }

  def this(size: Int, increasingBy: Float) {
    this(size, increasingBy, false)
  }

  override def write(b: Array[Byte], off: Int, len: Int) {
    val position = buffer.position()
    val limit = buffer.limit()
    val newTotal = position + len
    if (newTotal > limit) {
      var capacity = (buffer.capacity() * increasing).toInt
      while (capacity <= newTotal) {
        capacity = (capacity * increasing).toInt
      }
      increase(capacity)
    }
    buffer.put(b, 0, len)
  }

  override def write(b: Int) {
    if (!buffer.hasRemaining()) {
      increase((buffer.capacity() * increasing).toInt)
    }
    buffer.put(b.toByte)
  }

  protected def increase(newCapacity: Int) {
    buffer.limit(buffer.position())
    buffer.rewind()
    var newBuffer: ByteBuffer = null
    newBuffer = if (onHeap) ByteBuffer.allocate(newCapacity) else ByteBuffer.allocateDirect(newCapacity)
    newBuffer.put(buffer)
    buffer.clear()
    buffer = newBuffer
  }

  def capacity(): Long = buffer.capacity()

  def array(): Array[Byte] = buffer.array()
}