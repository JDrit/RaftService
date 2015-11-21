package edu.rit.csh.scaladb.raft.server

import java.net.InetSocketAddress

import com.twitter.conversions.time._
import com.twitter.finagle.Thrift
import com.twitter.finagle.filter.MaskCancelFilter
import com.twitter.finagle.service.{TimeoutFilter, RetryPolicy, RetryExceptionsFilter}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Future

import State.State

object ServerType extends Enumeration {
  type ServerType = Value
  val Follower, Leader, Candidate = Value
}

object State extends Enumeration {
  type State = Value
  val cOld, cOldNew = Value
}

/**
 * Server configuration for all members of the cluster
 * @param id the unique ID of the server
 * @param address the address to send requests to
 * @param nextIndex index of the next log entry to send to that server
 *                  (initialized to leader last log index + 1)
 * @param matchIndex index of highest log entry known to be replicated on server
 *                   (initialized to 0, increases monotonically)
 */
case class Peer(id: Int, address: InetSocketAddress, var nextIndex: Int, var matchIndex: Int) {

  /**
   * Creates the standard client connection that is used to communicate to other Raft servers
   * @param fun the function that creates the request, which generates a Future for the response
   *            from the remote server
   * @tparam I the type of the request
   * @tparam O the type of the remote server's response
   * @return the function that sends the request to the server, returning a Future response
   */
  private def createClient[I, O](fun: I => Future[O]): I => Future[O] = {
    val retry = new RetryExceptionsFilter[I, O](RetryPolicy.tries(3), DefaultTimer.twitter)
    val timeout = new TimeoutFilter[I, O](3.seconds, DefaultTimer.twitter)
    val maskCancel = new MaskCancelFilter[I, O]()
    retry andThen timeout andThen maskCancel andThen fun
  }

  val voteClient: RequestVote => Future[VoteResponse] = createClient(
    Thrift.newIface[RaftService.FutureIface](address.getHostName + ":" + address.getPort).vote)

  val appendClient: AppendEntries => Future[AppendResponse] = createClient(
    Thrift.newIface[RaftService.FutureIface](address.getHostName + ":" + address.getPort).append)

  override def toString: String =
    s"""peer {
       |  server id: $id
       |  address: ${address.toString}
       |  nextIndex: $nextIndex
       |  matchIndex: $matchIndex
       |}""".stripMargin
}

/**
 * configuration represents the sets of peers and behaviors required to
 * implement joint-consensus.
 * @param state determines if it is the new or old configuration
 * @param cOldPeers the list of old servers
 * @param cNewPeers the list of new servers
 */
case class RaftConfiguration(state: State, cOldPeers: Array[Peer], cNewPeers: Array[Peer]) {

  override def toString: String =
    s"""configuration {
       |  state: $state
       |  old peers: [${cOldPeers.mkString(", ")}]
       |  new peers: [${cNewPeers.mkString(", ")}]
       |}""".stripMargin
}

/**
 * Base class for commands send to the client. The State machine being used has to extend this.
 * The state machine will get called with every new command
 * @param id each command needs to have an unique ID so that the server can make sure
 *           that the same command will not be executed twice
 */
abstract class Command(id: String)

/**
 * Base class for the results of commands that have been sent to the server.
 * @param id the ID of the command that this is a result to
 */
abstract class Result(id: String)

/**
 * Entry in the Raft server's log
 * @param term the term that this entry was added
 * @param index the global index in the log of this entry
 * @param cmd the command or configuration change of this server
 */
case class LogEntry(term: Int, index: Int, cmd: Either[String, RaftConfiguration]) {

  override def toString: String =
    s"""entry {
       |  term: $term
       |  index: $index,
       |  cmd: $cmd
       |}""".stripMargin
}
