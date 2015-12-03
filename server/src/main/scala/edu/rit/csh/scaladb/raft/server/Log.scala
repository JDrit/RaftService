package edu.rit.csh.scaladb.raft.server

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Class representing the replicated log. This is used to store all the logs that have been
 * replicated in the system. It is thread-safe. Only appending and updating of logs.
 */
class Log[T: ClassTag] extends mutable.AbstractBuffer[T] {

  private val log = new java.util.ArrayList[T]()
  private val lock = new Object()


  /**
   * Returns a sequence representing the range of the log
   * @param start the index to start at inclusive
   * @param end the index to stop at exclusive
   * @return the sequence of the range
   */
  def range(start: Int, end: Int): Array[T] = lock.synchronized {
    var count = 0
    val arr = new Array[T](end - start)
    while (start < end) {
      arr(count) = log.get(start)
      count += 1
    }
    arr
  }

  override def apply(n: Int): T = lock.synchronized(log.get(n))

  override def update(n: Int, newelem: T): Unit = lock.synchronized(log.set(n, newelem))

  override def clear(): Unit = lock.synchronized(log.clear())

  override def length: Int = lock.synchronized(log.size())

  override def remove(n: Int): T =
    throw new NotImplementedError("removing of the log is not allowed, only appending")

  override def +=:(elem: T): Log.this.type =
    throw new NotImplementedError("prepending of the log is not allowed, only appending")

  override def +=(elem: T): Log.this.type = lock.synchronized { log.add(elem) ; this }

  override def +=(elem1 : T, elem2 : T, elems : T*): Log.this.type = lock.synchronized {
    log.add(elem1) ; log.add(elem2) ; log.addAll(elems) ; this
  }

  override def ++=(xs: TraversableOnce[T]): Log.this.type = lock.synchronized {
    xs.foreach(log.add) ; this
  }

  override def insertAll(n: Int, elems: Traversable[T]): Unit =
    throw new NotImplementedError("inserting of elements is not allowed, only appending")

  override def iterator: Iterator[T] = lock.synchronized(log.iterator())
}
