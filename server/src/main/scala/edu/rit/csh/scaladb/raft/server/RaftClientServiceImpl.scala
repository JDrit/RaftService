package edu.rit.csh.scaladb.raft.server

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging
import edu.rit.csh.scaladb.raft.client._

/**
 * The service that the client connects to. This then forwards requests to the raft server, which
 * updates the state machine
 * @param raftServer the raft server to send requests to
 * @tparam C the type of commands to send
 * @tparam R the type of results of the commands
 */
class RaftClientServiceImpl[C <: Command, R <: Result](raftServer: RaftServer[C, R])
  extends ClientOperations.FutureIface with LazyLogging {

  def get(get: GetRequest): Future[GetResponse] = Future {
    println(s"received Get RPC: $get")
    GetResponse(Some("value"))
  }

  def put(put: PutRequest): Future[PutResponse] = ???

  def cas(cas: CASRequest): Future[CASResponse] = ???

  def delete(delete: DeleteRequest): Future[DeleteResponse] = ???

}
