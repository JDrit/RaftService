package edu.rit.csh.scaladb.raft

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.Objects

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.filter.MaskCancelFilter
import com.twitter.finagle.thrift.{ThriftClientFramedCodec, ThriftClientRequest}
import com.twitter.util.Future
import edu.rit.csh.scaladb.raft.admin.Server
import edu.rit.csh.scaladb.raft.StateMachine.CommandResult
import org.apache.thrift.protocol.TBinaryProtocol

private[raft] object ServerType extends Enumeration {
  type ServerType = Value
  val Follower, Leader, Candidate = Value
}

/**
 * Server configuration for all members of the cluster
 * @param inetAddress the address to send requests to
 */
private[raft] class Peer(val inetAddress: InetSocketAddress, val clientAddr: InetSocketAddress) {

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

  private val client = new InternalService.FinagledClient(new MaskCancelFilter andThen service, new TBinaryProtocol.Factory())

  // index of the next log entry to send to that server (initialized to
  // leader last log index + 1)
  val nextIndex = new AtomicInteger(0)
  // index of highest log entry known to be replicated on server
  // (initialized to 0, increases monotonically)
  val matchIndex = new AtomicInteger(RaftServer.BASE_INDEX)

  def voteClient(request: RequestVote): Future[VoteResponse] = client.vote(request)

  def appendClient(request: AppendEntries): Future[AppendEntriesResponse] = client.append(request)

  override def toString: String =
    s"""peer {
       |  address: ${address.toString}
       |  next_index: $nextIndex
       |  match_index: $matchIndex
       |}""".stripMargin

  override def equals(other: Any): Boolean = other match {
    case other: Peer => other.inetAddress.equals(inetAddress) && other.clientAddr.equals(clientAddr)
    case _ => false
  }

  override def hashCode: Int = Objects.hash(inetAddress, clientAddr)
}

/**
 * Base class for commands send to the client. The State machine being used has to extend this.
 * The state machine will get called with every new command
 * @param id each command needs to have an unique ID so that the server can make sure
 *           that the same command will not be executed twice
 */
abstract class Command(val client: String, val id: Int) extends Serializable

/**
 * Base class for the results of commands that have been sent to the server.
 * @param id the ID of the command that this is a result to
 */
abstract class Result(val id: Int)

sealed trait FutureMonad[A] {
  type M[A] <: FutureMonad[A]

  def flatMap[B](f: Future[A] => M[B]): M[B]

  def unit[B](a: Future[B]): M[B]

  def map[B](f: Future[A] => Future[B]): M[B] = flatMap(x => unit(f(x)))
}

object SubmitMonad {

  sealed abstract class SubmitMonad[A] extends FutureMonad[A] {
    type M[A] = SubmitMonad[A]

    def unit[B](a: Future[B]) = SuccessMonad(a)

    override def flatMap[B](f: (Future[A]) => SubmitMonad[B]): SubmitMonad[B] = this match {
      case SuccessMonad(future) => f(future)
      case NotLeaderMonad(leader) => NotLeaderMonad(leader)
    }

    def isSuccess: Boolean = this match {
      case SuccessMonad(_) => true
      case _ => false
    }
  }

  case class SuccessMonad[A](future: Future[A]) extends SubmitMonad[A]

  case class NotLeaderMonad[A](leader: String) extends SubmitMonad[A]
}

/**
 * Entry in the Raft server's log
 * @param term the term that this entry was added
 * @param index the global index in the log of this entry
 * @param cmd the command or configuration change of this server
 */
private[raft] case class LogEntry(term: Int, index: Int, cmd: Either[ByteBuffer, Seq[Peer]]) {

  override def toString: String =
    s"""entry {
       |  term: $term
       |  index: $index,
       |  cmd: $cmd
       |}""".stripMargin
}

private[raft] object MessageConverters {

  def thriftToPeer(server: Server): Peer =
    new Peer(new InetSocketAddress(server.raftAddress, server.raftPort),
      new InetSocketAddress(server.clientAddress, server.clientPort))

  def peerToThrift(peer: Peer): Server = Server(peer.inetAddress.getHostName,
    peer.inetAddress.getPort, peer.clientAddr.getHostName, peer.clientAddr.getPort)


  def thriftToLogEntry(entry: Entry): LogEntry = (entry.command, entry.newConfiguration) match {
    case (Some(cmd), None) => LogEntry(entry.term, entry.index, Left(cmd))
    case (None, Some(config)) => LogEntry(entry.term, entry.index, Right(config.map(thriftToPeer)))
    case _ => throw new RuntimeException(s"could not parse Entry to LogEntry $entry")
  }

  def logEntryToThrift(logEntry: LogEntry): Entry = logEntry.cmd match {
    case Left(cmd) => Entry(logEntry.term, logEntry.index, EntryType.Command, Some(cmd), None)
    case Right(config) => Entry(logEntry.term, logEntry.index, EntryType.Configuration, None, Some(config.map(peerToThrift)))
  }
}
