package edu.rit.csh.scaladb.raft.server

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging

import MessageConverters._

class RaftInternalServiceImpl[C <: Command, R <: Result](serverState: RaftServer[C, R])
  extends RaftService.FutureIface with LazyLogging {


  /**
   * Invoked by candidates to gather votes
   */
  override def vote(requestVote: RequestVote): Future[VoteResponse] = Future {
    logger.debug("received RequestVote RPC")
    val currentTerm = serverState.getCurrentTerm()

    // If RPC request or response contains term T > currentTerm:
    // set currentTerm = T, convert to follower
    if (requestVote.term > currentTerm) {
      serverState.setTerm(requestVote.term)
      serverState.toFollower()
    }

    if (requestVote.term < currentTerm) {
      logger.debug(s"vote failed for $requestVote")
      VoteResponse(term = currentTerm, voteGranted = false)
    } else {
      val votedFor = serverState.getVotedFor()
      val commitIndex = serverState.getCommitIndex()

      // Each server will vote for at most one candidate in a
      // given term, on a first-come-first-served basis
      if ((votedFor.contains(requestVote.candidateId) || votedFor.isEmpty) && requestVote.lastLogIndex >= commitIndex) {
        serverState.setVotedFor(requestVote.candidateId) // updates the current voted for
        logger.debug(s"vote granted for $requestVote")
        VoteResponse(term = currentTerm, voteGranted = true)
      } else {
        logger.debug(s"vote failed for $requestVote")
        VoteResponse(term = currentTerm, voteGranted = false)
      }
    }
  }

  override def append(entries: AppendEntries): Future[AppendResponse] = Future {
    logger.debug("received AppendEntries RPC")
    val currentTerm = serverState.getCurrentTerm(entries.term)
    serverState.setHeartbeat(entries.term)
    serverState.setLeaderId(entries.leaderId)

    try {
      if (entries.term < currentTerm) {
        // Reply false if term < currentTerm
        AppendResponse(term = currentTerm, success = false)
      } else {
        serverState.getLogEntry(entries.prevLogIndex) match {
          case Some(entry) if entry.term == entries.prevLogTerm =>
            serverState.appendLog(entries.entires.map(thriftToLogEntry))
            AppendResponse(term = currentTerm, success = true)
          case None =>
            serverState.appendLog(entries.entires.map(thriftToLogEntry))
            AppendResponse(term = currentTerm, success = true)
          case _ =>
            // Reply false if log does not contain an entry at prevLogIndex
            // whose term matches prevLogTerm
            AppendResponse(term = currentTerm, success = false)
        }
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Error while processing append command")
        ex.printStackTrace()
        AppendResponse(term = currentTerm, success = false)
    }
  }
}