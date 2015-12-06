package edu.rit.csh.scaladb.raft.server.internal

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.filter.MaskCancelFilter
import com.twitter.finagle.thrift.{ThriftClientFramedCodec, ThriftClientRequest}
import com.twitter.util.Future
import edu.rit.csh.scaladb.raft.server.internal.State.State
import org.apache.thrift.protocol.TBinaryProtocol

private[internal] object ServerType extends Enumeration {
  type ServerType = Value
  val Follower, Leader, Candidate = Value
}

private[internal] object State extends Enumeration {
  type State = Value
  val cOld, cOldNew = Value
}

/**
 * Server configuration for all members of the cluster
 * @param id the unique ID of the server
 * @param inetAddress the address to send requests to
 */
private[internal] class Peer(val id: Int, val inetAddress: InetSocketAddress) {

  val address = inetAddress.getHostName + ":" + inetAddress.getPort

  private  val service:  Service[ThriftClientRequest, Array[Byte]] = ClientBuilder()
    .codec(ThriftClientFramedCodec())
    .hosts(address)
    .timeout(3.seconds)
    .hostConnectionLimit(1000)
    .tcpConnectTimeout(2.second)
    .retries(3)
    .failFast(false)
    .build()

  private val client = new RaftService.FinagledClient(new MaskCancelFilter andThen service, new TBinaryProtocol.Factory())

  // index of the next log entry to send to that server (initialized to
  // leader last log index + 1)
  private val nextIndex = new AtomicInteger(0)
  // index of highest log entry known to be replicated on server
  // (initialized to 0, increases monotonically)
  private val matchIndex = new AtomicInteger(RaftServer.BASE_INDEX)

  val voteClient: RequestVote => Future[VoteResponse] = client.vote

  val appendClient: AppendEntries => Future[AppendEntriesResponse] = client.append

  val getNextIndex: () => Int = nextIndex.get

  val incNextIndex: () => Int = nextIndex.incrementAndGet

  val decNextIndex: () => Int = nextIndex.decrementAndGet

  val setNextIndex: Int => Unit = nextIndex.set

  val setMatchIndex: Int => Unit = matchIndex.set

  val incMatchIndex: () => Int = matchIndex.incrementAndGet

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
private[internal] case class RaftConfiguration(state: State, cOldPeers: Array[Peer], cNewPeers: Array[Peer]) {

  override def toString: String =
    s"""configuration {
       |  state: $state
       |  old peers: [${cOldPeers.mkString(", ")}]
       |  new peers: [${cNewPeers.mkString(", ")}]
       |}""".stripMargin
}

/**
 * The result of submitting a command to the server. It either returns a future for the
 * successful completion of the command, the address of the true leader, or a message
 * telling that the command has already been processed
 */
sealed abstract class SubmitResult
final case class SuccessResult(future: Future[Either[Int, Result]]) extends SubmitResult
final case class NotLeaderResult(leader: String) extends SubmitResult

/**
 * Base class for commands send to the client. The State machine being used has to extend this.
 * The state machine will get called with every new command
 * @param id each command needs to have an unique ID so that the server can make sure
 *           that the same command will not be executed twice
 */
abstract class Command(val client: String, val id: Int)

/**
 * Base class for the results of commands that have been sent to the server.
 * @param id the ID of the command that this is a result to
 */
abstract class Result(val id: Int)

/**
 * This is the serializer / deserializer that is used to format the commands to send to the
 * state machine
 * @tparam C the type of the command to work on
 */
trait MessageSerializer[C <: Command] {

  def serialize(command: C): String

  def deserialize(str: String): C
}

/**
 * Entry in the Raft server's log
 * @param term the term that this entry was added
 * @param index the global index in the log of this entry
 * @param cmd the command or configuration change of this server
 */
private[internal] case class LogEntry(term: Int, index: Int, cmd: Either[String, Configuration]) {

  override def toString: String =
    s"""entry {
       |  term: $term
       |  index: $index,
       |  cmd: $cmd
       |}""".stripMargin
}

private[internal] object MessageConverters {

  def thriftToLogEntry(entry: Entry): LogEntry = (entry.command, entry.configuration) match {
    case (Some(cmd), None) => LogEntry(entry.term, entry.index, Left(cmd))
    case (None, Some(config)) => LogEntry(entry.term, entry.index, Right(config))
    case _ => throw new RuntimeException(s"could not parse Entry to LogEntry $entry")
  }

  def logEntryToThrift(logEntry: LogEntry): Entry = logEntry.cmd match {
    case Left(cmd) => Entry(logEntry.term, logEntry.index, EntryType.Command, Some(cmd), None)
    case Right(config) => Entry(logEntry.term, logEntry.index, EntryType.Configuration, None, Some(config))
  }
}
