package edu.rit.csh.scaladb.raft.server

import com.twitter.util.{Await, Future}
import com.twitter.conversions.time._
import com.typesafe.scalalogging.LazyLogging
import edu.rit.csh.scaladb.raft.client._
import edu.rit.csh.scaladb.raft.server.util.ClientCache

import scala.collection.convert.decorateAsScala._
import scala.reflect.ClassTag
import scala.reflect._

import java.util.concurrent.ConcurrentHashMap

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
   *
   * Each request type has a different cache
   */
  private val cache = new ConcurrentHashMap[ClassTag[_ <: OpResult], ClientCache]().asScala

  /**
   * Process the client's request, checking first to see if the same request has not already
   * been processed
   * @param clientId the unique ID of the client
   * @param commandId the unique ID per the command that the client is sending
   * @param fun the function the processes the request if it command has not already been run
   * @tparam R the type of the resulting operator
   * @return returns the cached result or the result of running the function
   */
  private def process[R <: OpResult: ClassTag](clientId: String, commandId: Int, fun: () => R): R = {
    val functionCache = cache.getOrElseUpdate(classTag[R], new ClientCache(100))
    functionCache.get(clientId, commandId) match {
      case Some(result) =>
        logger.debug(s"client cache hit: $clientId, $commandId")
        result.asInstanceOf[R]
      case None =>
        val result = fun()
        functionCache.put(clientId, commandId, result)
        result
    }
  }

  @throws[NotLeader]
  def get(get: GetRequest): Future[GetResponse] = Future {
    logger.debug("received a client GET request")

    GetResponse(process(get.clientId, get.commandId, { () =>
      try {
        val future: Future[GetResult] = raftServer.submitCommand(Get(get.commandId, get.key))
        Await.result(future, 2.seconds)
      } catch {
        case ex: NotLeaderException =>
          logger.debug(s"client service get not leader, ${ex.address}")
          throw new NotLeader(ex.address)
      }
    }).value)
  }

  @throws[NotLeader]
  def put(put: PutRequest): Future[PutResponse] = ???

  @throws[NotLeader]
  def cas(cas: CASRequest): Future[CASResponse] = ???

  @throws[NotLeader]
  def delete(delete: DeleteRequest): Future[DeleteResponse] = ???

}
