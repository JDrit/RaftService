package edu.rit.csh.scaladb.raft.server

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging
import edu.rit.csh.scaladb.raft.client._

class RaftClientService extends ClientOperations.FutureIface with LazyLogging {

  def get(get: GetRequest): Future[GetResponse] = Future {
    println(s"received Get RPC: $get")
    GetResponse(Some("value"))
  }

  def put(put: PutRequest): Future[PutResponse] = ???

  def cas(cas: CASRequest): Future[CASResponse] = ???

  def delete(delete: DeleteRequest): Future[DeleteResponse] = ???

}
