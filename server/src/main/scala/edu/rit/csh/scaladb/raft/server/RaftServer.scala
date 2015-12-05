package edu.rit.csh.scaladb.raft.server


import com.twitter.conversions.time._
import com.twitter.util._
import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference, AtomicLong}
import edu.rit.csh.scaladb.raft.server.util.Scala2Java8._

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
  final val BASE_INDEX: Int = -1
}

/**
 * The actual state of the raft consensus algorithm. This holds all the information and executes
 * commands. The raft server should not care about the type of messages being sent, just
 * relay them to the state machine.
 * @param self the peer that stores the information about this given server
 * @param peers the list of all peers in the system, including itself
 * @param stateMachine the state machine that is used to execute commands.
 */
class RaftServer(self: Peer, peers: Map[Int, Peer], stateMachine: StateMachine) extends LazyLogging {

  private final val DEBUG_INCREASE = 1

  // When servers start up, they begin as followers. A server remains in follower state
  // as long as it receives valid RPCs from a leader or candidate
  private val status = new AtomicReference(ServerType.Follower)

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
  // a reference is kept so that they can be cancled when a heartbeat comes in from a leader.
  private val electionFuture = new AtomicReference[Option[Future[Seq[VoteResponse]]]](None)

  new Thread(new Runnable {
    override def run(): Unit = {
      while (true) {
        val sleep = electionTimeout()
        Thread.sleep(sleep.inMilliseconds)
        if (status.get() != ServerType.Leader) {
          // If a follower receives no communication over a period of time
          // called the election timeout, then it assumes there is no viable
          // leader and begins an election to choose a new leader
          if (getLastHeartbeat() > sleep) {
            startElection()
          }
        }
      }
    }
  }).start()

  // the monitor to use to notify the leader's heartbeat thread to send messages
  private val heartMonitor: Object = new Object()
  new Thread(new Runnable {
    override def  run(): Unit = {
      while (true) {
        heartMonitor.synchronized {
          while (status.get() != ServerType.Leader) {
            heartMonitor.wait()
          }
          val (prevLogIndex, prevLogTerm) = getPrevInfo()
          val append = AppendEntries(getCurrentTerm(), self.id, prevLogIndex,
            prevLogTerm, Seq.empty, commitIndex.get)
          peers.values.map(_.appendClient(append))
        }
        val sleep = 100.milliseconds * DEBUG_INCREASE
        Thread.sleep(sleep.inMilliseconds)
      }
    }
  }).start()

  def getCurrentTerm(): Int = currentTerm.get

  /**
   * Updates the current term if the given value is larger than the current value.
   * The updated value is returned
   */
  def getCurrentTerm(term: Int): Int = currentTerm
    .updateAndGet((value: Int) => if (value < term) term else value)

  def incrementTerm(): Int = currentTerm.incrementAndGet

  def setTerm(term: Int): Unit = currentTerm.set(term)

  def getVotedFor(): Option[Int] = votedFor.get

  def setVotedFor(id: Int): Unit = votedFor.set(Some(id))

  def clearVotedFor(): Unit = votedFor.set(None)

  def getCommitIndex(): Int = commitIndex.get

  def toFollower(): Unit = status.set(ServerType.Follower)

  def getLeaderId(): Option[Int] = leaderId.get()

  def setLeaderId(id: Int): Unit = leaderId.set(Some(id))

  def clearLeaderId(): Unit = leaderId.set(None)

  /**
   * Updates the commit index, applying all entries in the log up to, and including,
   * the index given
   * @param index the new commit index for this server
   */
  def setCommitIndex(index: Int): Unit = {
    val bottomIndex = commitIndex.get() + 1
    commitIndex.set(index)
    // If commitIndex > lastApplied: increment lastApplied, apply
    // log[lastApplied] to state machine (§5.3)
    if (commitIndex.get() > lastApplied.get()) {
      lastApplied.set(commitIndex.get())
    }

    for (i <- bottomIndex to index) {
      logger.info(s"applying log entry #$i")
      log.get(i).cmd match {
        case Left(cmd) => try {
          stateMachine.applyLog(stateMachine.parser.deserialize(cmd))
        } catch {
          case ex: Exception => logger.error("ERROR while applying the log to the state machine")
        }
        case Right(config) => throw new NotImplementedError("configuration change not implemented yet")
      }
    }
  }

  def getLogEntry: Int => Option[LogEntry] = log.lift

  /**
   * While waiting for votes, a candidate may receive an  AppendEntries RPC from another server
   * claiming to be leader. If the leader’s term (included in its RPC) is at least as large
   * as the candidate’s current term, then the candidate recognizes the leader as legitimate
   * and returns to follower state. If the term in the RPC is smaller than the candidate’s
   * current term, then the candidate rejects the RPC and continues in candidate state
   */
  def setHeartbeat(term: Int): Unit = {
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
  private def getLastHeartbeat(): Duration = (System.nanoTime() - lastHeartbeat.get()).nanoseconds

  /**
   * The timeout used for when the server should start the election process.
   * Raft uses randomized election timeouts to ensure that split votes are rare and that
   * they are resolved quickly. To prevent split votes in the first place, election
   * timeouts are chosen randomly from a fixed interval
   * @return the time till the election should take place
   */
  private def electionTimeout(): Duration = (150 + Random.nextInt(300 - 150 + 1)).milliseconds * DEBUG_INCREASE

  private def getPrevInfo(): (Int, Int) = log.lastOption
    .map(entry => (entry.index, entry.term))
    .getOrElse((RaftServer.BASE_INDEX, RaftServer.BASE_INDEX))

  /**
   * Adds the entries to the log, overwriting new ones.
   * The entries given must be inorder
   * If an existing entry conflicts with a new one (same index but different terms),
   * delete the existing entry and all that follow it (§5.3)
   */
  def appendLog(entry: LogEntry): Unit = log.lift(entry.index)
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
  private def appendRequest(peer: Peer): Future[AppendResponse] = {
    val prevLogIndex = peer.getNextIndex() - 1 // index of log entry immediately preceding new ones
    val prevLogTerm = log.lift(prevLogIndex).map(_.term).getOrElse(RaftServer.BASE_INDEX) // term of prevLogIndex entry
    val entries = log.range(peer.getNextIndex(), log.length).map(MessageConverters.logEntryToThrift)
    logger.debug(s"log entries being sent: ${entries.mkString(", ")} ")
    val request = AppendEntries(currentTerm.get, self.id, prevLogIndex, prevLogTerm, entries, commitIndex.get)
    peer.appendClient(request)
      .onSuccess { response =>
        if (response.success) {
          logger.debug(s"received successful update from ${peer.address}, ${entries.last.index + 1}")
          peer.setNextIndex(entries.last.index + 1)
          peer.setMatchIndex(entries.last.index + 1)
        } else {
          logger.debug(s"peer ${peer.address} failed the request, trying again")
          peer.decNextIndex()
          appendRequest(peer) // try again with lower commit index
        }
      }.rescue { case ex =>
        logger.debug(s"exception occurred while sending an append request to ${peer.address}\n$ex")
        Future { AppendResponse(getCommitIndex(), false) }
      }
  }



  /**
   * Log replication (5.3)
   * @param command the command to run through the system
   * @return Left: the address of the leader to talk to
   *         Right: the future for the completion of the request
   */
  def submitCommand(command: Command): Either[String, Future[Result]] = {
    logger.debug(s"got command $command")
    if (!getLeaderId().contains(self.id)) {
      logger.debug(s"leader id: ${getLeaderId()}")
      Left(peers(getLeaderId().get).address)
    } else {
      val index = log.lastOption.map(_.index + 1).getOrElse(0)
      val logEntry = LogEntry(currentTerm.get, index, Left(stateMachine.parser.serialize(command)))
      logger.debug(s"log entry: $logEntry")
      // If command received from client: append entry to local log, respond after
      // entry applied to state machine
      appendLog(logEntry)

      Right(Future.collect(peers.values.map(appendRequest).toList).map { _ =>
        setCommitIndex(index)
        stateMachine.applyLog(command)
      })
    }
  }

  /**
   * Starts the election process (5.2).
   */
  private def startElection(): Unit = {
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
        heartMonitor.synchronized(heartMonitor.notifyAll())
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
