package edu.rit.csh.scaladb.raft.server.util

import com.twitter.util.LruMap
import edu.rit.csh.scaladb.raft.server.{OpResult, util}

/**
 * Uses nested LRU caches per clients to keep track of already processed requests
 * @param size the size of the nested LRU caches to use
 */
class ClientCache(size: Int) {

  private val cache = new LruMap[String, LruMap[Int, OpResult]](size)

  def get(client: String, id: Int): Option[OpResult] = this.synchronized {
    cache.get(client).flatMap(_.get(id))
  }

  def put(client: String, id: Int, response: OpResult): Unit = this.synchronized {
    cache.getOrElseUpdate(client, new LruMap[Int, OpResult](size)).put(id, response)
  }
}
