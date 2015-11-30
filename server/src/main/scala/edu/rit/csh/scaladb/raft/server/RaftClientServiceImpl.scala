package edu.rit.csh.scaladb.raft.server

import com.twitter.util.{LruMap, Await, SynchronizedLruMap, Future}
import com.twitter.conversions.time._
import com.typesafe.scalalogging.LazyLogging
import edu.rit.csh.scaladb.raft.client._

/**
 * The service that the client connects to. This then forwards requests to the raft server, which
 * updates the state machine
 * @param raftServer the raft server to send requests to
 */
class RaftClientServiceImpl(raftServer: RaftServer[Operation, OpResult])
  extends ClientOperations.FutureIface with LazyLogging {

  /**
   * clients assign unique serial numbers to every command. Then, the state machine tracks
   * the latest serial number processed for each client, along with the associated response.
   * If it receives a command whose serial number has already been executed, it responds
   * immediately without re-executing the request
   */
  private val cache = new ClientCache(100)

  @throws[NotLeader]
  def get(get: GetRequest): Future[GetResponse] = Future {
    cache.get(get.clientId, get.commandId) match {
      case Some(response) =>
        GetResponse(response.asInstanceOf[GetResult].value)
      case None =>
        try {
          val future = raftServer.submitCommand[Get, GetResult](Get(get.commandId, get.key))
          val getResult = Await.result(future, 2.seconds)
          val getResponse = GetResponse(getResult.value)
          cache.put(get.clientId, get.commandId, getResult)
          getResponse
        } catch {
          case ex: NotLeaderException =>
            logger.debug(s"client service get not leader, ${ex.address}")
            throw new NotLeader(ex.address)
        }
    }
  }

  @throws[NotLeader]
  def put(put: PutRequest): Future[PutResponse] = ???

  @throws[NotLeader]
  def cas(cas: CASRequest): Future[CASResponse] = ???

  @throws[NotLeader]
  def delete(delete: DeleteRequest): Future[DeleteResponse] = ???

}

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
