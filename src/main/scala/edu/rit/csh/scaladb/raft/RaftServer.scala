package edu.rit.csh.scaladb.raft

import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue

import com.twitter.conversions.time._
import com.twitter.finagle.Thrift
import com.twitter.finagle.filter.MaskCancelFilter
import com.twitter.finagle.service.{TimeoutFilter, RetryExceptionsFilter, RetryPolicy}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import com.typesafe.scalalogging.LazyLogging

import java.util
import java.util.Collections
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference, AtomicLong}

import edu.rit.csh.scaladb.raft.storage.Storage
import edu.rit.csh.scaladb.raft.RaftConfiguration._

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
//
class RaftServer[K, V](self: Peer, peers: Array[Peer], storage: Storage[K, V]) extends LazyLogging {

  val timeoutThread = new Thread(new Runnable {
    override def run(): Unit = {
      while (status.get() != ServerType.Leader) {
        val sleep = electionTimeout()
        Thread.sleep(sleep.inMilliseconds)
        // If a follower receives no communication over a period of time
        // called the election timeout, then it assumes there is no viable
        // leader and begins an election to choose a new leader
        if (getLastHeartbeat() > sleep) {
          startElection()
        }
      }
    }
  })
  timeoutThread.start()

  val heartbeatThread = new Thread(new Runnable {
    override def run(): Unit = {
      while (status.get() == ServerType.Leader) {
        val sleep = 100.milliseconds
        Thread.sleep(sleep.inMilliseconds)
        val lst = Seq.empty[LogEntry]
        entiresToSend.drainTo(lst)
        logger.debug(s"sending append RPC with ${lst.size} entries")
        val (prevLogIndex, prevLogTerm)  = lst match {
          case x :: xs => (lst.get(x.index - 1).index, lst.get(x.index - 1).term)
          case Nil => (0, 0)
        }
        val append = AppendEntries(
          term = getCurrentTerm,
          leaderId = self.id,
          prevLogIndex = prevLogIndex,
          prevLogTerm = prevLogTerm,
          entires = lst.map(logEntryTotThrift),
          leaderCommit = commitIndex.get)
        peers.map { peer =>
          appendClient(peer.address)
        }

      }
    }
  })

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

  // the ID of the current leader
  private val leaderId = new AtomicReference[Option[Int]](None)

  // reference to the futures that are sending the vote requests to the other servers.
  // a reference is kept so that they can be cancled when a heartbeat comes in from a leader.
  private val electionFuture = new AtomicReference[Option[Future[Seq[VoteResponse]]]](None)

  private val entiresToSend = new LinkedBlockingQueue[LogEntry]()

  def getCurrentTerm: Int = currentTerm.get

  def incrementTerm(): Int = currentTerm.incrementAndGet

  def setTerm(term: Int): Unit = currentTerm.set(term)

  def getVotedFor: Option[Int] = votedFor.get

  def setVotedFor(id: Int): Unit = votedFor.set(Some(id))

  def clearVotedFor: Unit = votedFor.set(None)

  def getCommitIndex: Int = commitIndex.get

  def toFollower(): Unit = status.set(ServerType.Follower)

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
    val time = (min + Random.nextInt(max - min + 1)).milliseconds * 10
    logger.debug(s"election timeout: $time")
    time
  }

  /**
   * Called when a log has been applied to the replicated state machine
   * @param log
   */
  private def applyLog(log: LogEntry): Unit = {
    // TODO actually finish this
    System.out.println(log)

  }

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

  private def voteClient(address: String): RequestVote => Future[VoteResponse] =
    createClient(Thrift.newIface[RaftService.FutureIface](address).vote)

  private def appendClient(address: String): AppendEntries => Future[AppendResponse] =
    createClient(Thrift.newIface[RaftService.FutureIface](address).append)

  /**
   * Starts the election process (5.2)
   */
  private def startElection(): Unit = {
    logger.info("starting election process")

    // To begin an election, a follower increments its current term
    val term = incrementTerm()
    // and transitions to candidate state
    status.set(ServerType.Candidate)

    // It then votes for itself
    votedFor.set(Some(self.id))

    val index = commitIndex.get()
    val requestVote = RequestVote(term, self.id, index, term)

    // sends requests for vote to all other servers
    val futures = Future.collect(peers.map { peer =>
      logger.debug(s"sending RequestVote to ${peer.address}")
      voteClient(peer.address)(requestVote)
        .rescue { case exception =>
          // returns a failed vote response when the server is unreachable
          logger.debug(s"failed sending RequestVote to ${peer.address}")
          Future { VoteResponse(term, false) }
        }
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
        logger.debug("received enough votes to become leader")
        // Once a candidate wins an election, it becomes leader.
        status.set(ServerType.Leader)

        val (lastIndex, lastTerm) = log.lastOption match {
          case Some(logEntry) => (logEntry.index, logEntry.term)
          case None => (0, 0)
        }

        // It then sends heartbeat messages to all of the other servers to establish its
        // authority and to prevent new elections.
        val heartBeat = AppendEntries(term, self.id, lastIndex, lastTerm, Seq.empty, index)
        peers.foreach { peer =>
          appendClient(peer.address)(heartBeat).onFailure { exception =>
            println(s"exception sending out initial heartbeat to ${peer.address}")
          }
        }

        // When a leader first comes to power, it initializes all nextIndex values to the index
        // just after the last one in its log
        peers.foreach(peer => peer.nextIndex = lastIndex + 1)

      } else { // If election timeout elapses: start new election
        logger.debug("did not receive enough votes to become leader, restarting election...")
        startElection()
      }
    } catch {
      case ex: TimeoutException =>
        electionFuture.set(None)
        logger.debug("timeout exception during election process, restarting election processes")
        startElection()
      case ex: FutureCancelledException =>
        electionFuture.set(None)
        logger.debug("election process cancelled by other leader")
    }
  }

  override def toString(): String =
    s"""
       |raft {
       |  current term: ${currentTerm.get()}
       |  state: ${status.get()}
       |  commit index: ${commitIndex.get()}
       |  leader id:
       |  voted for: ${votedFor.get()}
       |}
     """.stripMargin
}
