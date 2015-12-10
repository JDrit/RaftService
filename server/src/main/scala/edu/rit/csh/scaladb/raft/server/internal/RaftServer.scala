package edu.rit.csh.scaladb.raft.server.internal

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import com.twitter.conversions.time._
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.twitter.util._
import com.typesafe.scalalogging.LazyLogging
import edu.rit.csh.scaladb.raft.server.internal.StateMachine.CommandResult
import edu.rit.csh.scaladb.raft.server.internal.RaftService.FinagledService
import edu.rit.csh.scaladb.raft.server.util.Scala2Java8._
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
  private[internal] final val BASE_INDEX: Int = -1

  /**
   * Constructs the server and starts listening on the given address
   * @param stateMachine the state machine to use as the system to process commands
   * @param id the unique identifier for this node
   * @param address the address for the internal raft service to listen on
   * @param others the list of the other peers currently in the network
   * @return the raft server that has been constructed
   */
  def apply(stateMachine: StateMachine, id: Int, address: InetSocketAddress,
            others: Seq[(Int, InetSocketAddress)]): RaftServer = {
    val self = new Peer(id, address)
    val servers = others.map { case (oId, oAddress) => new Peer(oId, oAddress) }
      .:+(self)
      .map { peer => (peer.id, peer) }
      .toMap
    val raftServer = new RaftServer(self, servers, stateMachine)
    val internalImpl = new RaftInternalServiceImpl(raftServer)
    val internalService = new FinagledService(internalImpl, new Factory())

    ServerBuilder()
      .bindTo(self.inetAddress)
      .codec(ThriftServerFramedCodec())
      .name("Raft Internal Service")
      .build(internalService)
    raftServer.start()
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
 */
class RaftServer private(private[internal] val self: Peer,
                         private[internal] val peers: Map[Int, Peer],
                         stateMachine: StateMachine) extends LazyLogging {

  // When servers start up, they begin as followers. A server remains in follower state
  // as long as it receives valid RPCs from a leader or candidate
  private[internal] val status = new AtomicReference(ServerType.Follower)

  //
  // persistent stats on all servers
  //

  // latest term server has seen (initialized to 0 on first boot, increases monotonically)
  private val currentTerm = new AtomicInteger(0)
  // candidateId that received vote in current term
  private val votedFor = new AtomicReference[Option[Int]](None)
  // log entries; each entry contains command for state machine, and term when entry
  // as received by leader (first index is 1)
  private val log: Log[LogEntry] = new Log[LogEntry]()

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
  private val leaderId = new AtomicReference[Option[Int]](None)

  // reference to the futures that are sending the vote requests to the other servers.
  // a reference is kept so that they can be canceled when a heartbeat comes in from a leader.
  private val electionFuture = new AtomicReference[Option[Future[Seq[VoteResponse]]]](None)

  private val electionThread = new ElectionThread(this)
  private val heartThread = new HeartbeatThread(this)

  private def start(): Unit = {
    electionThread.start()
    heartThread.start()
  }

  private[internal] def getCurrentTerm(): Int = currentTerm.get

  /**
   * Updates the current term if the given value is larger than the current value.
   * The updated value is returned
   */
  private[internal] def getCurrentTerm(term: Int): Int = currentTerm
    .updateAndGet((value: Int) => if (value < term) term else value)

  private def incrementTerm(): Int = currentTerm.incrementAndGet

  private[internal] def setTerm(term: Int): Unit = currentTerm.set(term)

  private[internal] def getVotedFor(): Option[Int] = votedFor.get

  private[internal] def setVotedFor(id: Int): Unit = votedFor.set(Some(id))

  private def clearVotedFor(): Unit = votedFor.set(None)

  private[internal] def getCommitIndex(): Int = commitIndex.get

  private[internal] def toFollower(): Unit = status.set(ServerType.Follower)

  private[internal] def getLeaderId(): Option[Int] = leaderId.get()

  private[internal] def isLeader(): Boolean = leaderId.get().contains(self.id)

  private[internal] def setLeaderId(id: Int): Unit = leaderId.set(Some(id))

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
  private[internal] def setCommitIndex(index: Int): Option[CommandResult] = this.synchronized {
    var result: CommandResult = null
    val old = commitIndex.get()
    if (old < index) {
      commitIndex.set(index)
      logger.debug(s"incrementing commit index: $old - $index")

      (old + 1 to index).foreach { i =>
        logger.info(s"applying log entry #$i")
        log.get(i).cmd match {
          case Left(cmd) =>
            result = stateMachine.process(stateMachine.parser.deserialize(cmd))
            results.put(i, result)
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

  private[internal] def getLogEntry(index: Int): Option[LogEntry] = log.lift(index)

  /**
   * While waiting for votes, a candidate may receive an  AppendEntries RPC from another server
   * claiming to be leader. If the leader’s term (included in its RPC) is at least as large
   * as the candidate’s current term, then the candidate recognizes the leader as legitimate
   * and returns to follower state. If the term in the RPC is smaller than the candidate’s
   * current term, then the candidate rejects the RPC and continues in candidate state
   */
  private[internal] def setHeartbeat(term: Int): Unit = {
    if (status.get() == ServerType.Candidate) {
      if (term >= currentTerm.get()) {
        status.set(ServerType.Follower)
        // cancels the election process
        electionFuture.get().foreach(_.raise(new FutureCancelledException))
      }
    }
    lastHeartbeat.set(System.nanoTime())
  }

  /**
   * Gets the duration since the last heartbeat has occurred
   */
  private[internal] def getLastHeartbeat(): Duration = (System.nanoTime() - lastHeartbeat.get()).nanoseconds

  /**
   * The timeout used for when the server should start the election process.
   * Raft uses randomized election timeouts to ensure that split votes are rare and that
   * they are resolved quickly. To prevent split votes in the first place, election
   * timeouts are chosen randomly from a fixed interval
   * @return the time till the next election should take place
   */
  private[internal] def electionTimeout(): Duration = (150 + Random.nextInt(300 - 150 + 1)).milliseconds

  private[internal] def getPrevInfo(): (Int, Int) = log.lastOption
    .map(entry => (entry.index, entry.term))
    .getOrElse((RaftServer.BASE_INDEX, RaftServer.BASE_INDEX))

  /**
   * Adds the entries to the log, overwriting new ones.
   * The entries given must be inorder
   * If an existing entry conflicts with a new one (same index but different terms),
   * delete the existing entry and all that follow it (§5.3)
   */
  private[internal] def appendLog(entry: LogEntry): Unit = log.lift(entry.index)
    .map(_ => log.set(entry.index, entry))
    .getOrElse(log += entry)

  /**
   * Sends an append request to the peer, retying till the peer returns success.
   * Resents the request if there is a failure sending it, like network partition.
   * If the returns success = false, then the next index of that client is decremented
   * and the request is sent again
   * @param peer the peer to send the message to
   * @return a future for the successful response to the request
   */
  private def appendRequest(peer: Peer): Future[AppendEntriesResponse] = {
    val prevLogIndex = peer.getNextIndex() - 1 // index of log entry immediately preceding new ones
    val prevLogTerm = log.lift(prevLogIndex).map(_.term).getOrElse(RaftServer.BASE_INDEX) // term of prevLogIndex entry
    val entries = log.range(peer.getNextIndex(), log.length).map(MessageConverters.logEntryToThrift)
    val request = AppendEntries(currentTerm.get, self.id, prevLogIndex, prevLogTerm, entries, commitIndex.get)
    peer.appendClient(request)
      .onSuccess { response =>
        if (response.success) {
          peer.setNextIndex(entries.last.index + 1)
          peer.setMatchIndex(entries.last.index + 1)
        } else {
          logger.debug(s"peer ${peer.address} failed the request, trying again")
          peer.decNextIndex()
          appendRequest(peer) // try again with lower commit index
        }
      }.rescue { case ex =>
        logger.debug(s"exception occurred while sending an append request to ${peer.address}\n$ex")
        Future { AppendEntriesResponse(getCommitIndex(), false) }
      }
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
  def submit(command: Command): SubmitResult = this.synchronized {
    try {
      if (!getLeaderId().contains(self.id)) {
        NotLeaderResult(peers(getLeaderId().get).address)
      } else {
        val index = log.lastOption.map(_.index).getOrElse(RaftServer.BASE_INDEX) + 1
        val logEntry = LogEntry(currentTerm.get, index, Left(stateMachine.parser.serialize(command)))
        logger.debug(s"log entry: $logEntry")
        // If command received from client: append entry to local log, respond after
        // entry applied to state machine
        appendLog(logEntry)

        SuccessResult(Future.collect(peers.values.map(appendRequest).toList).map(_ => commitLog(index)))
      }
    } catch {
      case ex: Exception =>
        println("error")
        ex.printStackTrace()
        NotLeaderResult(peers(getLeaderId().get).address)
    }
  }

  /**
   * Starts the election process (5.2).
   */
  private[internal] def startElection(): Unit = {
    logger.info("starting election process")

    // To begin an election, a follower increments its current term
    val term = incrementTerm()
    // and transitions to candidate state
    status.set(ServerType.Candidate)

    clearLeaderId()

    val index = commitIndex.get()
    val requestVote = RequestVote(term, self.id, index, term)

    // sends requests for vote to all other servers
    val futures = Future.collect(peers.values.map { peer =>
      logger.debug(s"sending RequestVote to ${peer.address}")
      // returns a failed vote response when the server is unreachable
      peer.voteClient(requestVote).rescue { case _ => Future { VoteResponse(term, false) } }
    }.toList)
    electionFuture.set(Some(futures)) // sets the futures so that they can be cancelled
    try {
      val result = Await.result(futures, 100.milliseconds)
      // the number of severs that vote yes for this server, plus 1 for voting for yourself
      val votesYes = result.count(_._2)
      logger.debug(s"received $votesYes yes vote(s)")

      // A candidate wins an election if it receives votes from a majority of the
      // servers in the full cluster for the same term
      if (votesYes > peers.size / 2) {
        // Once a candidate wins an election, it becomes leader.
        status.set(ServerType.Leader)
        setLeaderId(self.id)

        val (lastIndex, lastTerm) = getPrevInfo()

        logger.debug(
          s"""Received enough votes to become leader:
             |new leader config: $this
           """.stripMargin)

        // It then sends heartbeat messages to all of the other servers to establish its
        // authority and to prevent new elections.

        peers.values.foreach { peer =>
          peer.appendClient(AppendEntries(term, self.id, lastIndex, lastTerm, Seq.empty, index))
            .onFailure { _ => logger.debug(s"Exception sending out initial heartbeat to ${peer.address}") }
        }

        // When a leader first comes to power, it initializes all nextIndex values to the index
        // just after the last one in its log
        peers.values.foreach(peer => peer.setNextIndex(lastIndex + 1))

        // starting heartbeat thread
        heartThread.synchronized(heartThread.notifyAll())
      } else { // If election timeout elapses: start new election
        logger.debug("did not receive enough votes to become leader, restarting election...")
        val maxResponse = result.map(_.term).max
        // updates the term to the highest response it got
        currentTerm.updateAndGet((curr: Int) => if (curr < maxResponse) maxResponse else curr)
      }
    } catch {
      case ex: TimeoutException =>
        logger.debug("timeout exception during election process, restarting election processes")
      case ex: FutureCancelledException =>
        logger.debug("election process cancelled by other leader")
    } finally {
      clearVotedFor()
      electionFuture.set(None)
    }
  }

  override def toString(): String =
    s"""raft {
       |  current term: ${currentTerm.get()}
       |  state: ${status.get()}
       |  commit index: ${commitIndex.get()}
       |  leader id: ${leaderId.get().getOrElse("<no one>")}
       |  voted for: ${votedFor.get().getOrElse("<no one>")}
       |}""".stripMargin
}
