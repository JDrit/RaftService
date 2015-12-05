package edu.rit.csh.scaladb.raft.server

import com.twitter.util.Future
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
class RaftClientServiceImpl(raftServer: RaftServer) extends ClientOperations.FutureIface with LazyLogging {

  /**
   * clients assign unique serial numbers to every command. Then, the state machine tracks
   * the latest serial number processed for each client, along with the associated response.
   * If it receives a command whose serial number has already been executed, it responds
   * immediately without re-executing the request
   *
   * Each request type has a different cache
   */
  private val cache = new ConcurrentHashMap[ClassTag[_ <: Result], ClientCache]().asScala

  private def process[R <: Result: ClassTag](client: String, id: Int, fun: () => Either[String, Future[R]]): Future[R] = {
    val functionCache = cache.getOrElseUpdate(classTag[R], new ClientCache(100))
    functionCache.get(client, id) match {
      case Some(result) => Future.value(result.asInstanceOf[R])
      case None => fun() match {
        case Left(address) => Future.exception(new NotLeader(address))
        case Right(futures) => futures.map { result =>
          functionCache.put(client, id, result)
          result
        }.rescue { case ex: Exception =>
          logger.debug("exception processing request")
          ex.printStackTrace()
          null
        }
      }
    }
  }

  @throws[NotLeader]
  def get(get: GetRequest): Future[GetResponse] = process(get.clientId, get.commandId, { () =>
    raftServer.submitCommand(Get(get.commandId, get.key))
  }).map(result => GetResponse(result.asInstanceOf[GetResult].value))

  @throws[NotLeader]
  def put(put: PutRequest): Future[PutResponse] = process(put.clientId, put.commandId, { () =>
    raftServer.submitCommand(Put(put.commandId, put.key, put.value))
  }).map(result => PutResponse(result.asInstanceOf[PutResult].overrided))

  @throws[NotLeader]
  def cas(cas: CASRequest): Future[CASResponse] = process(cas.clientId, cas.commandId, { () =>
    raftServer.submitCommand(CAS(cas.commandId, cas.key, cas.curValue, cas.newValue))
  }).map(result => CASResponse(result.asInstanceOf[CASResult].replaced))

  @throws[NotLeader]
  def delete(delete: DeleteRequest): Future[DeleteResponse] = process(delete.clientId, delete.commandId, { () =>
    raftServer.submitCommand(Delete(delete.commandId, delete.key))
  }).map(result => DeleteResponse(result.asInstanceOf[DeleteResult].deleted))

}
