package edu.rit.csh.scaladb.raft

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import com.twitter.conversions.time._
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.twitter.logging.{Logger, Logging}
import com.twitter.server.TwitterServer
import com.twitter.util._
import edu.rit.csh.scaladb.raft.InternalService.FinagledService
import edu.rit.csh.scaladb.raft.StateMachine.CommandResult
import edu.rit.csh.scaladb.raft.SubmitMonad._
import edu.rit.csh.scaladb.raft.serialization.Serializer
import edu.rit.csh.scaladb.raft.util.Scala2Java8._
import org.apache.thrift.protocol.TBinaryProtocol.Factory

import scala.collection.JavaConversions._
import scala.util.Random

//                                  times out,
//                                 new election
//     |                             .-----.
//     |                             |     |
//     v         times out,          |     v     receives votes from
// +----------+  starts election  +-----------+  majority of servers  +--------+
// | Follower |------------------>| Candidate |---------------------->| Leader |
// +----------+                   +-----------+                       +--------+
//     ^ ^                              |                                 |
//     | |    discovers current leader  |                                 |
//     | |                 or new term  |                                 |
//     | '------------------------------'                                 |
//     |                                                                  |
//     |                               discovers server with higher term  |
//     '------------------------------------------------------------------'
//

object RaftServer {
  private[raft] final val BASE_INDEX: Int = -1
  private val log = Logger.get(getClass)

  def peers(peers: Seq[Peer]): PeerMap = {
    val map = new PeerMap
    for (peer <- peers) {
      map += ((peer.address, peer))
    }
    map
  }

  def apply(stateMachine: StateMachine, serializer: Serializer[Command], address: InetSocketAddress, client: InetSocketAddress,
           raftAddrs: Seq[InetSocketAddress], serverAddrs: Seq[InetSocketAddress],
            closeables: Seq[Closable] = Seq.empty): RaftServer = {
    val self = new Peer(address, client)
    val servers = raftAddrs.zip(serverAddrs)
      .map { case (raft, server) => new Peer(raft, server) }
      .:+(self)

    val raftServer = new RaftServer(self, peers(servers), stateMachine, serializer, closeables)
    val internalImpl = new RaftInternalServiceImpl(raftServer)
    val internalService = new FinagledService(internalImpl, new Factory())

    log.info(s"listening on $address, cluster: ${servers.mkString(", ")}")

    ServerBuilder()
      .bindTo(self.inetAddress)
      .codec(ThriftServerFramedCodec())
      .name("raft_internal_service")
      .build(internalService)
    raftServer
  }
}

/**
 * The actual state of the raft consensus algorithm. This holds all the information and executes
 * commands. The raft server should not care about the type of messages being sent, just
 * relay them to the state machine.
 * @param self the peer that stores the information about this given server
 * @param peers the list of all peers in the system, including itself
 * @param stateMachine the state machine that is used to execute commands.
 * @param closeables the list of objects to close when the server is shutting down
 */
class RaftServer private(private[raft] val self: Peer,
                         private[raft] val peers: PeerMap,
                         stateMachine: StateMachine,
                         serializer: Serializer[Command],
                         closeables: Seq[Closable]) extends TwitterServer with Logging {

  private val appendCounter = statsReceiver.scope("raft_service").counter("log_appends")
  private val commitCounter = statsReceiver.scope("raft_service").counter("log_commits")
  private val electionCounter = statsReceiver.scope("raft_service").counter("elections")

  // When servers start up, they begin as followers. A server remains in follower state
  // as long as it receives valid RPCs from a leader or candidate
  private[raft] val status = new AtomicReference(ServerType.Follower)

  //
  // persistent stats on all servers
  //

  // latest term server has seen (initialized to 0 on first boot, increases monotonically)
  private val currentTerm = new AtomicInteger(0)
  // candidateId that received vote in current term
  private val votedFor = new AtomicReference[Option[String]](None)
  // log entries; each entry contains command for state machine, and term when entry
  // as received by leader (first index is 1)
  private val raftLog: Log[LogEntry] = new Log[LogEntry]()

  //
  // volatile state on all servers
  //

  // index of highest log entry known to be committed
  private val commitIndex = new AtomicInteger(RaftServer.BASE_INDEX)
  // index of highest log entry applied to state machine
  private val lastApplied = new AtomicInteger(RaftServer.BASE_INDEX)

  // the last time the server has received a message from the server
  private val lastHeartbeat = new AtomicLong(0)

  // the ID of the current leader, so followers can redirect clients
  private val leaderId = new AtomicReference[Option[String]](None)

  // reference to the futures that are sending the vote requests to the other servers.
  // a reference is kept so that they can be canceled when a heartbeat comes in from a leader.
  private val electionFuture = new AtomicReference[Option[Future[Seq[VoteResponse]]]](None)

  private val electionThread = new ElectionThread(this)
  private val heartThread = new HeartbeatThread(this)

  electionThread.start()
  heartThread.start()

  closeOnExit(stateMachine)
  closeables.foreach(closeOnExit)

  private[raft] def getCurrentTerm(): Int = currentTerm.get

  /**
   * Updates the current term if the given value is larger than the current value.
   * The updated value is returned
   */
  private[raft] def getCurrentTerm(term: Int): Int = currentTerm
    .updateAndGet((value: Int) => if (value < term) term else value)

  private def incrementTerm(): Int = currentTerm.incrementAndGet

  private[raft] def setTerm(term: Int): Unit = currentTerm.set(term)

  private[raft] def getVotedFor(): Option[String] = votedFor.get

  private[raft] def setVotedFor(id: String): Unit = votedFor.set(Some(id))

  private def clearVotedFor(): Unit = votedFor.set(None)

  private[raft] def getCommitIndex(): Int = commitIndex.get

  private[raft] def toFollower(): Unit = status.set(ServerType.Follower)

  def getLeaderId(): Option[String] = leaderId.get()

  def getLeader(): Option[Peer] = leaderId.get().flatMap(id => peers.get(id))

  private[raft] def isLeader(): Boolean = leaderId.get().contains(self.address)

  private[raft] def setLeaderId(id: String): Unit = leaderId.set(Some(id))

  private def clearLeaderId(): Unit = leaderId.set(None)

  /**
   * Keeps track of the outputs of all the commands run. This is a mapping from the log
   * index ID to the output of the command. Elements need to be removed once they are
   * returned to the client to make sure OOM do not happen
   */
  private val results = new ConcurrentHashMap[Int, CommandResult]()

  /**
   * Updates the commit index to the given index, only if it is bigger than the current value.
   * It then applies all the log elements up to that point. All results are stored so that
   * they can be returned to the client.
   * @param index the new index
   * @return an option of the result of the log at the given index, or None if the commit
   *         index was not updated
   */
  private[raft] def setCommitIndex(index: Int): Option[CommandResult] = this.synchronized {
    var result: CommandResult = null
    val old = commitIndex.get()
    if (old < index) {
      commitIndex.set(index)
      log.info(s"incrementing commit index: $old - $index")

      (old + 1 to index).foreach { i =>
        commitCounter.incr()
        log.info(s"applying log entry #$i: ${raftLog.get(i)}")
        raftLog.get(i).cmd match {
          case Left(cmd) =>
            result = stateMachine.process(serializer.read(cmd))
            results.put(i, result)
          case Right(servers) =>
            peers.changeConfiguration(servers)
            if (!peers.contains(self.address)) {
              log.info("Self has been removed from the cluster, shutting down...")
            }
        }
      }
    }
    Option(result)
  }

  /**
   * Gets the result of the log entry at the given position, computing it if necessary.
   * It makes sure that the command is only computed once by checking to see the cache
   * of already computed results. It removes the results of the commands from the command
   * to keep memory usage low
   * @param index the index to get the result for
   * @return the result of the command
   */
  private def commitLog(index: Int): CommandResult = this.synchronized {
    val result = setCommitIndex(index).getOrElse(results.get(index))
    results.remove(index)
    result
  }

  private[raft] def getLogEntry(index: Int): Option[LogEntry] = raftLog.lift(index)

  private[raft] def getLastIndex: Int = raftLog.size - 1

  /**
   * While waiting for votes, a candidate may receive an  AppendEntries RPC from another server
   * claiming to be leader. If the leader’s term (included in its RPC) is at least as large
   * as the candidate’s current term, then the candidate recognizes the leader as legitimate
   * and returns to follower state. If the term in the RPC is smaller than the candidate’s
   * current term, then the candidate rejects the RPC and continues in candidate state
   */
  private[raft] def setHeartbeat(term: Int): Unit = {
    if (status.get() == ServerType.Candidate) {
      if (term >= currentTerm.get()) {
        status.set(ServerType.Follower)
        // cancels the election process
        electionFuture.get().foreach(_.raise(new FutureCancelledException))
      }
    }
    lastHeartbeat.set(System.nanoTime())
  }

  private[raft] final val minTime = 150
  private[raft] final val maxTime = 300

  /**
   * Gets the duration since the last heartbeat has occurred
   */
  private[raft] def getLastHeartbeat: Duration = (System.nanoTime() - lastHeartbeat.get()).nanoseconds

  /**
   * If there has been a heartbeat from a leader within the minimum election timeout period
   */
  private[raft] def minTimeout(): Boolean = getLastHeartbeat > minTime.milliseconds

  /**
   * The timeout used for when the server should start the election process.
   * Raft uses randomized election timeouts to ensure that split votes are rare and that
   * they are resolved quickly. To prevent split votes in the first place, election
   * timeouts are chosen randomly from a fixed interval
   * @return the time till the next election should take place
   */
  private[raft] def electionTimeout: Duration = (minTime + Random.nextInt(maxTime - minTime + 1)).milliseconds

  private[raft] def getPrevInfo: (Int, Int) = raftLog.lastOption
    .map(entry => (entry.index, entry.term))
    .getOrElse((RaftServer.BASE_INDEX, RaftServer.BASE_INDEX))

  /**
   * Adds the entries to the log, overwriting new ones.
   * The entries given must be inorder
   * If an existing entry conflicts with a new one (same index but different terms),
   * delete the existing entry and all that follow it (§5.3)
   */
  private[raft] def appendLog(entry: LogEntry): Unit = {
    appendCounter.incr()
    log.info(s"appending log entry: $entry")
    raftLog.lift(entry.index)
      .map(_ => raftLog.set(entry.index, entry))
      .getOrElse(raftLog += entry)

    // Once a given server adds the new configuration entry to its log, it uses that
    // configuration for all future decisions (a server always uses the latest configuration
    // in its log, regardless of whether the entry is committed).
    entry.cmd.right.foreach { newServers =>
      log.info("appending new configuration")
    }
  }

  def exit(msg: String): Unit = exitOnError(msg)

  /**f
   * Sends an append request to the peer, retying till the peer returns success.
   * Resents the request if there is a failure sending it, like network partition.
   * If the returns success = false, then the next index of that client is decremented
   * and the request is sent again
   * @param peer the peer to send the message to
   * @param latch used to detect how many peers have already responded, this way only
   *              a majority have to respond
   * @param delay the length of the delay when an exception occurs when talking to a peer
   * @return a future for the successful response to the request
   */
  private def appendRequest(peer: Peer, latch: CountDownLatch, delay: Long = 100): Future[AppendEntriesResponse] = {
    val prevLogIndex = peer.nextIndex.get - 1 // index of log entry immediately preceding new ones
    val prevLogTerm = raftLog.lift(prevLogIndex).map(_.term).getOrElse(RaftServer.BASE_INDEX) // term of prevLogIndex entry
    val entries = raftLog.range(peer.nextIndex.get, raftLog.length).map(MessageConverters.logEntryToThrift)
    val request = AppendEntries(currentTerm.get, self.address, prevLogIndex, prevLogTerm, entries, commitIndex.get)
    peer.appendClient(request)
      .onSuccess { response =>
        log.debug("success")
        if (response.success) {
          latch.countDown()
          peer.nextIndex.set(entries.last.index + 1)
          peer.matchIndex.set(entries.last.index + 1)
        } else {
          log.info(s"peer ${peer.address} failed the request, trying again")
          response.lastIndex match {
            case Some(index) => peer.nextIndex.set(index)
            case None => peer.nextIndex.decrementAndGet()
          }
          getCurrentTerm(response.term)
          appendRequest(peer, latch, delay) // try again with lower commit index
        }
      }.rescue { case ex =>
        if (latch.isZero) { // returns a bad result once a majority of the nodes respond
          Future.value(AppendEntriesResponse(getCommitIndex(), false))
        } else {
          log.debug(s"exception occurred while sending an append request to ${peer.address}, delaying for $delay ms\n$ex")
          Thread.sleep(delay)
          appendRequest(peer, latch, if (delay < 1000 * 5) delay * 2 else delay)
        }
      }
  }


  /**
   * Broadcasts a log entry with the new configuration of nodes in the system. This should
   * only be called after successfully replicating a log entry with the joint consensus.
   */
  private def newConfiguration(servers: Seq[Peer]): SubmitMonad[CommandResult] = this.synchronized {
    val index = raftLog.lastOption.map(_.index).getOrElse(RaftServer.BASE_INDEX) + 1
    val logEntry = LogEntry(currentTerm.get, index, Right(servers))
    broadcastEntry(logEntry)
  }

  /**
   * The leader first creates the Cold,new configuration entry in its log and commits it to
   * Cold,new (a majority of Cold and a majority of Cnew). Then it creates the Cnew entry and
   * commits it to a majority of Cnew. There is no point in time in which Cold and Cnew can both
   * make decisions independently.
   * @param servers
   * @return
   */
  private[raft] def jointConfiguration(servers: Seq[Peer]): SubmitMonad[CommandResult] = this.synchronized {
    log.info(
      s"""current configuration: ${peers.values.map(_.address).mkString(", ")}
         |new configuration: ${servers.mkString(", ")}""".stripMargin)
    val index = raftLog.lastOption.map(_.index).getOrElse(RaftServer.BASE_INDEX) + 1
    val jointPeers = (servers ++ peers.values).distinct
    val logEntry = LogEntry(currentTerm.get, index, Right(jointPeers))

    // When the leader receives a request to change the configuration from Cold
    // to Cnew, it stores the configuration for joint consensus (Cold,new in the figure) as a
    // log entry and replicates that entry


    broadcastEntry(logEntry).map(_.onSuccess(_ => newConfiguration(servers)))

    // Once a given server adds the new configuration entry to its log, it uses that
    // configuration for all future decisions (a server always uses the latest configuration
    // in its log, regardless of whether the entry is committed
  }

  /**
   * Submits a command to be processed by the replicated state machine. This takes the command,
   * and if this node is the leader, returns a future for the completion of the task. This
   * has to be synchronized to make sure that log entries are added correctly. If the future
   * returns a result, then it is guaranteed to be committed and to be seen by every following
   * command.
   * @param command the command to run on the replicated state machine
   * @return the future with the result of the computation.
   */
  def submit(command: Command): SubmitMonad[CommandResult] = this.synchronized {
    val index = raftLog.lastOption.map(_.index).getOrElse(RaftServer.BASE_INDEX) + 1
    val buffer = new ByteArrayOutputStream()
    serializer.write(command, buffer)
    val logEntry = LogEntry(currentTerm.get, index, Left(ByteBuffer.wrap(buffer.toByteArray)))
    broadcastEntry(logEntry)
  }

  private def broadcastEntry(logEntry: LogEntry): SubmitMonad[CommandResult] = {
    if (!isLeader()) {
      NotLeaderMonad(peers(getLeaderId().get).address)
    } else {
      // If command received from client: append entry to local log, respond after
      // entry applied to state machine
      appendLog(logEntry)
      val latch = new CountDownLatch(peers.size / 2 + 1)
      val futures = peers.values
        .map(peer => appendRequest(peer, latch))
        .toSeq
      SuccessMonad(Future.collect(futures).map(_ => commitLog(logEntry.index)))
    }
  }

  /**
   * Starts the election process (5.2).
   */
  private[raft] def startElection(): Unit = {
    electionCounter.incr()
    log.info("starting election process")

    // To begin an election, a follower increments its current term
    val term = incrementTerm()
    // and transitions to candidate state
    status.set(ServerType.Candidate)

    clearLeaderId()

    val index = commitIndex.get()
    val requestVote = RequestVote(term, self.address, index, term)

    val electionPeers = peers.values

    // sends requests for vote to all other servers
    val futures = Future.collect(electionPeers.map { peer =>
      log.debug(s"sending RequestVote to ${peer.address}")
      // returns a failed vote response when the server is unreachable
      peer.voteClient(requestVote).rescue { case _ => Future { VoteResponse(term, false) } }
    }.toList)
    electionFuture.set(Some(futures)) // sets the futures so that they can be cancelled
    try {
      val result = Await.result(futures, 100.milliseconds)
      // the number of severs that vote yes for this server, plus 1 for voting for yourself
      val votesYes = result.count(_._2)
      log.debug(s"received $votesYes yes vote(s)")

      // A candidate wins an election if it receives votes from a majority of the
      // servers in the full cluster for the same term
      if (votesYes > electionPeers.size / 2) {
        // Once a candidate wins an election, it becomes leader.
        status.set(ServerType.Leader)
        setLeaderId(self.address)

        val (lastIndex, lastTerm) = getPrevInfo

        log.info(
          s"""Received enough votes to become leader:
             |new leader config: $this
           """.stripMargin)

        // It then sends heartbeat messages to all of the other servers to establish its
        // authority and to prevent new elections.

        electionPeers.foreach { peer =>
          peer.appendClient(AppendEntries(term, self.address, lastIndex, lastTerm, Seq.empty, index))
            .onFailure { _ => log.debug(s"Exception sending out initial heartbeat to ${peer.address}") }
        }

        // When a leader first comes to power, it initializes all nextIndex values to the index
        // just after the last one in its log
        electionPeers.foreach(peer => peer.nextIndex.set(lastIndex + 1))

        // starting heartbeat thread
        heartThread.synchronized(heartThread.notifyAll())
      } else { // If election timeout elapses: start new election
        log.debug("did not receive enough votes to become leader, restarting election...")
        val maxResponse = result.map(_.term).max
        // updates the term to the highest response it got
        currentTerm.updateAndGet((curr: Int) => if (curr < maxResponse) maxResponse else curr)
      }
    } catch {
      case ex: TimeoutException =>
        log.debug("timeout exception during election process, restarting election processes")
      case ex: FutureCancelledException =>
        log.debug("election process cancelled by other leader")
    } finally {
      clearVotedFor()
      electionFuture.set(None)
    }
  }

  override def toString(): String =
    s"""raft {
       |  current_term: ${currentTerm.get()}
       |  state: ${status.get()}
       |  commit_index: ${commitIndex.get()}
       |  last_log_index: ${getPrevInfo._1}
       |  leader_id: ${leaderId.get().getOrElse("<no one>")}
       |  voted_for: ${votedFor.get().getOrElse("<no one>")}
       |} ${peers.values.mkString(" ")}""".stripMargin
}
