package edu.rit.csh.scaladb.raft

import com.twitter.conversions.time._
import com.twitter.finagle.Thrift
import com.twitter.finagle.filter.MaskCancelFilter
import com.twitter.finagle.service.{TimeoutFilter, RetryExceptionsFilter, RetryPolicy}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import com.typesafe.scalalogging.LazyLogging

import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference, AtomicLong}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.util.Random

case class LogEntry(term: Int, index: Int, message: String)

object ServerType extends Enumeration {
  type ServerType = Value
  val Follower, Leader, Candidate = Value
}

class RaftServer(self: String, others: Array[String]) extends LazyLogging {

  val timeoutThread = new Thread(new Runnable {
    override def run(): Unit = {
      while (true) {
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


  // When servers start up, they begin as followers. A server remains in follower state
  // as long as it receives valid RPCs from a leader or candidate
  private val status = new AtomicReference(ServerType.Follower)

  //
  // persistent stats on all servers
  //

  // latest term server has seen (initialized to 0 on first boot, increases monotonically)
  private val currentTerm = new AtomicInteger(0)
  // candidateId that received vote in current term
  private val votedFor = new AtomicReference[Option[String]](Option.empty)
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

  //
  // volatile state on leaders
  //

  // for each server, index of the next log entry to send to that server
  private val nextIndex = new ConcurrentHashMap[String, Int]()
  //for each server, index of highest log entry known to be replicated on server
  private val matchIndex = new ConcurrentHashMap[String, Int]()

  others.foreach { other =>
    nextIndex.put(other, 1 + commitIndex.get())
    matchIndex.put(other, 0)
  }

  // reference to the futures that are sending the vote requests to the other servers.
  // a reference is kept so that they can be cancled when a heartbeat comes in from a leader.
  private val electionFuture = new AtomicReference[Option[Future[Seq[VoteResponse]]]](None)

  def getCurrentTerm: Int = currentTerm.get

  def incrementTerm(): Int = currentTerm.incrementAndGet

  def setTerm(term: Int): Unit = currentTerm.set(term)

  def getVotedFor: Option[String] = votedFor.get

  def setVotedFor(id: String): Unit = votedFor.set(Some(id))

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
        electionFuture.get().foreach(_.raise(new RuntimeException("Cancelling election processes")))
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

  /**
   * Starts the election process (5.2)
   */
  @tailrec
  private def startElection(): Unit = {
    logger.info("starting election process")

    // To begin an election, a follower increments its current term
    val term = incrementTerm()
    // and transitions to candidate state
    status.set(ServerType.Candidate)

    // It then votes for itself
    votedFor.set(Some(self))

    // TODO Reset election timer

    val index = commitIndex.get()
    val otherServers = nextIndex.keys().toArray
    val requestVote = RequestVote(term, self, index, term)

    // sends requests for vote to all other servers
    val futures = Future.collect(otherServers.filter(_ != self).map { server =>
      logger.debug(s"sending RequestVote to $server")
      val fun = Thrift.newIface[RaftService.FutureIface](server).vote _
      createClient(fun)(requestVote)
    })
    electionFuture.set(Some(futures))

    val result = Await.result(futures, 100.milliseconds)
    electionFuture.set(None)
    // the number of severs that vote yes for this server
    val votesYes = 1 + result.count(_._2)
    logger.debug(s"received $votesYes yes vote(s)")

    // A candidate wins an election if it receives votes from a majority of the
    // servers in the full cluster for the same term
    if (votesYes > otherServers.length / 2) {
      // Once a candidate wins an election, it becomes leader.
      status.set(ServerType.Leader)

      // It then sends heartbeat messages to all of the other servers to establish its
      // authority and to prevent new elections.
      val heartBeat = AppendEntries(term, self, log.last.index, log.last.term, Seq.empty, index)
      otherServers.filter(_ != self).foreach { server =>
        val fun = Thrift.newIface[RaftService.FutureIface](server).append _
        createClient(fun)(heartBeat)
      }
    // If election timeout elapses: start new election
    } else {
      startElection()
    }
  }
}
