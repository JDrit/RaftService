package edu.rit.csh.scaladb.raft

import scala.collection.mutable

class PeerMap extends mutable.Map[String, Peer] {

  private val map = mutable.Map.empty[String, Peer]

  override def +=(kv: (String, Peer)): PeerMap.this.type = this.synchronized {
    map += kv
    this
  }

  override def -=(key: String): PeerMap.this.type = this.synchronized {
    map -= key
    this
  }

  override def get(key: String): Option[Peer] = this.synchronized(map.get(key))

  override def empty: PeerMap = new PeerMap

  override def iterator: Iterator[(String, Peer)] = this.synchronized(map.iterator)

  override def seq = this.synchronized(map.seq)

  override def foreach[U](f: ((String, Peer)) => U): Unit = this.synchronized(map.foreach(f))

  override def size: Int = this.synchronized(map.size)

  def changeConfiguration(peers: Seq[Peer]): Unit = {
    map.clear()
    peers.foreach(peer => map += ((peer.address, peer)))
  }
}
