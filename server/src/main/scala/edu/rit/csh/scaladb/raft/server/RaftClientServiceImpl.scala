package edu.rit.csh.scaladb.raft.server

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging
import edu.rit.csh.scaladb.raft.client._
import edu.rit.csh.scaladb.raft.server.internal._

/**
 * The service that the client connects to. This then forwards requests to the raft server, which
 * updates the state machine
 * @param raftServer the raft server to send requests to
 */
class RaftClientServiceImpl(raftServer: RaftServer) extends ClientOperations.FutureIface with LazyLogging {

  private def process[R](command: Command, convert: Result => R) = raftServer.submit(command) match {
    case SuccessResult(futures) => futures.flatMap {
      case Left(highest) => Future.exception(new AlreadySeen(highest))
      case Right(result) => Future.value(convert(result))
    }
    case NotLeaderResult(leader) => Future.exception(new NotLeader(leader))
  }

  @throws[NotLeader]
  @throws[AlreadySeen]
  override def get(get: GetRequest): Future[GetResponse] = process(Get(get.clientId, get.commandId,
    get.key), { result => GetResponse(result.asInstanceOf[GetResult].value) })

  @throws[NotLeader]
  @throws[AlreadySeen]
  override def put(put: PutRequest): Future[PutResponse] = process(Put(put.clientId, put.commandId,
    put.key, put.value), { result => PutResponse(result.asInstanceOf[PutResult].overrided) })

  @throws[NotLeader]
  @throws[AlreadySeen]
  override  def cas(cas: CASRequest): Future[CASResponse] = process(CAS(cas.clientId, cas.commandId,
    cas.key, cas.curValue, cas.newValue), { result => CASResponse(cas.asInstanceOf[CASResult].replaced) })

  @throws[NotLeader]
  @throws[AlreadySeen]
  override def delete(delete: DeleteRequest): Future[DeleteResponse] = process(Delete(delete.clientId,
    delete.commandId, delete.key), { result =>
      DeleteResponse(result.asInstanceOf[DeleteResult].deleted)
    })

  @throws[NotLeader]
  @throws[AlreadySeen]
  override def append(append: AppendRequest): Future[AppendResponse] = process(
    Append(append.clientId, append.commandId, append.key, append.value), { result =>
      AppendResponse(result.asInstanceOf[AppendResult].newValue)
    })
}
