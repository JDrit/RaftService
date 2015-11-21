package edu.rit.csh.scaladb.raft.server

import java.util.concurrent.LinkedBlockingQueue

import com.twitter.conversions.time._
import com.twitter.util._
import com.typesafe.scalalogging.LazyLogging

import java.util
import java.util.Collections
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference, AtomicLong}
import RaftConfiguration._
import Scala2Java8._
import edu.rit.csh.scaladb.raft.server.storage.Storage

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
class RaftServer[K, V](self: Peer, peers: Array[Peer], storage: Storage[K, V]) extends LazyLogging {

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
  private val log = Collections.synchronizedList(new util.ArrayList[LogEntry]())

  //
  // volatile state on all servers
  //

  // index of highest log entry known to be committed
  private val commitIndex = new AtomicInteger(0)
  // index of highest log entry applied to state machine
  private val lastApplied = new AtomicInteger(0)

  // the last time the server has received a message from the server
  private val lastHeartbeat = new AtomicLong(0)

  // the ID of the current leader, so followers can redirect clients
  private val leaderId = new AtomicReference[Option[Int]](None)

  // reference to the futures that are sending the vote requests to the other servers.
  // a reference is kept so that they can be cancled when a heartbeat comes in from a leader.
  private val electionFuture = new AtomicReference[Option[Future[Seq[VoteResponse]]]](None)

  private val entiresToSend = new LinkedBlockingQueue[LogEntry]()

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
          val (prevLogIndex, prevLogTerm) = log.lastOption
            .map(entry => (entry.index, entry.term))
            .getOrElse((0, 0))
          val append = AppendEntries(getCurrentTerm(), self.id, prevLogIndex,
            prevLogTerm, Seq.empty, commitIndex.get)
          peers.map(peer => peer.appendClient(append))
        }
        val sleep = 100.milliseconds
        Thread.sleep(sleep.inMilliseconds)
      }
    }
  }).start()

  def getCurrentTerm(): Int = currentTerm.get

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
   * Increments the commit index.
   * If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied]
   * to state machine
   */
  def incrementCommitIndex(): Unit = {
    val index = commitIndex.incrementAndGet()
    if (index > lastApplied.get()) {
      applyLog(log.get(lastApplied.getAndIncrement()))
    }
  }

  def setCommitIndex(index: Int): Unit = commitIndex.set(index)

  def getLogEntry(index: Int): Option[LogEntry] = {
    val entry = log.get(index)
    if (entry == null) Some(entry)
    else None
  }

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
  def getLastHeartbeat(): Duration = (System.nanoTime() - lastHeartbeat.get()).nanoseconds

  /**
   * The timeout used for when the server should start the election process.
   * Raft uses randomized election timeouts to ensure that split votes are rare and that
   * they are resolved quickly. To prevent split votes in the first place, election
   * timeouts are chosen randomly from a fixed interval
   * @return the time till the election should take place
   */
  private def electionTimeout(): Duration = {
    val min = 150
    val max = 300
    (min + Random.nextInt(max - min + 1)).milliseconds
  }

  /**
   * Called when a log has been applied to the replicated state machine
   */
  private def applyLog(log: LogEntry): Unit = {
    // TODO actually finish this
    logger.info(s"applying log $log")
  }

  /**
   * Adds the entries to the log, overwriting new ones
   */
  def appendLog(entries: Seq[LogEntry]): Unit = { // TODO will break when inserting new ones
    entries.foreach { entry => log.set(entry.index, entry) }
    logger.debug(s"appending ${entries.length} entries to the log")
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
    val futures = Future.collect(peers.map { peer =>
      logger.debug(s"sending RequestVote to ${peer.address}")
      // returns a failed vote response when the server is unreachable
      peer.voteClient(requestVote).rescue { case _ => Future { VoteResponse(term, false) } }
    })
    electionFuture.set(Some(futures)) // sets the futures so that they can be cancelled
    try {
      val result = Await.result(futures, 100.milliseconds)
      // the number of severs that vote yes for this server, plus 1 for voting for yourself
      val votesYes = result.count(_._2)
      logger.debug(s"received $votesYes yes vote(s)")

      // A candidate wins an election if it receives votes from a majority of the
      // servers in the full cluster for the same term
      if (votesYes > peers.length / 2) {
        // Once a candidate wins an election, it becomes leader.
        status.set(ServerType.Leader)
        setLeaderId(self.id)

        val (lastIndex, lastTerm) = log.lastOption
          .map(entry => (entry.index, entry.term))
          .getOrElse((0, 0))

        logger.debug("received enough votes to become leader")

        // It then sends heartbeat messages to all of the other servers to establish its
        // authority and to prevent new elections.
        val heartBeat = AppendEntries(term, self.id, lastIndex, lastTerm, Seq.empty, index)
        peers.foreach { peer =>
          peer.appendClient(heartBeat).onFailure { exception =>
            println(s"exception sending out initial heartbeat to ${peer.address}")
          }
        }

        // When a leader first comes to power, it initializes all nextIndex values to the index
        // just after the last one in its log
        peers.foreach(peer => peer.nextIndex = lastIndex + 1)

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
