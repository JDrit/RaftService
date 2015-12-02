package edu.rit.csh.scaladb.raft.server

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Class representing the replicated log. This is used to store all the logs that have been
 * replicated in the system. It is thread-safe. Only appending and updating of logs.
 */
class Log extends mutable.AbstractBuffer[LogEntry] {

  private val log = new java.util.ArrayList[LogEntry]()
  private val lock = new Object()

  override def apply(n: Int): LogEntry = lock.synchronized(log.get(n))

  override def update(n: Int, newelem: LogEntry): Unit = lock.synchronized(log.set(n, newelem))

  override def clear(): Unit = lock.synchronized(log.clear())

  override def length: Int = lock.synchronized(log.size())

  override def remove(n: Int): LogEntry =
    throw new NotImplementedError("removing of the log is not allowed, only appending")

  override def +=:(elem: LogEntry): Log.this.type =
    throw new NotImplementedError("prepending of the log is not allowed, only appending")

  override def +=(elem: LogEntry): Log.this.type = lock.synchronized { log.add(elem) ; this }

  override def +=(elem1 : LogEntry, elem2 : LogEntry, elems : LogEntry*): Log.this.type = lock.synchronized {
    log.add(elem1) ; log.add(elem2) ; log.addAll(elems) ; this
  }

  override def ++=(xs: TraversableOnce[LogEntry]): Log.this.type = lock.synchronized {
    xs.foreach(log.add) ; this
  }

  override def insertAll(n: Int, elems: Traversable[LogEntry]): Unit =
    throw new NotImplementedError("inserting of elements is not allowed, only appending")

  override def iterator: Iterator[LogEntry] = lock.synchronized(log.iterator())
}
