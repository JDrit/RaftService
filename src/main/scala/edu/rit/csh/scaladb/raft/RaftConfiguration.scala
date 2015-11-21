package edu.rit.csh.scaladb.raft


import java.net.InetSocketAddress

import com.twitter.conversions.time._
import com.twitter.finagle.Thrift
import com.twitter.finagle.filter.MaskCancelFilter
import com.twitter.finagle.service.{TimeoutFilter, RetryPolicy, RetryExceptionsFilter}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Future

import edu.rit.csh.scaladb.raft.State.State

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
 * mplement joint-consensus.
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

case class LogEntry(term: Int, index: Int, cmd: Either[String, RaftConfiguration]) {

  override def toString: String =
    s"""entry {
       |  term: $term
       |  index: $index,
       |  cmd: $cmd
       |}""".stripMargin
}

object RaftConfiguration {

  implicit def PeerToThrift(peer: Peer): Server = Server(peer.id, peer.address.getAddress.getHostAddress)

  implicit def RaftConfigToThrift(config: RaftConfiguration): Configuration = ???

  implicit def logEntryTothrift(entry: LogEntry): Entry = entry.cmd match {
    case Left(cmd) =>
      Entry(entry.term, entry.index, EntryType.Command, command = Some(cmd))
    case Right(config) =>
      Entry(entry.term, entry.index, EntryType.Configuration, configuration = Some(config))
  }

  implicit def thriftToLogEntry(entry: Entry): LogEntry = (entry.command, entry.configuration) match {
    case (Some(cmd), None) => LogEntry(entry.term, entry.index, Left(cmd))
    case (None, Some(config)) => LogEntry(entry.term, entry.index, Right(config))
    case _ => throw new RuntimeException(s"could not parse Entry to LogEntry $entry")
  }

  implicit def thriftToLogEntries(entries: Seq[Entry]): Seq[LogEntry] = entries.map(thriftToLogEntry)

  implicit def thriftToRaftConfig(config: Configuration): RaftConfiguration = ???

  implicit def entriesToThrift(entires: Seq[LogEntry]): Seq[Entry] = entires.map(logEntryTothrift)
}