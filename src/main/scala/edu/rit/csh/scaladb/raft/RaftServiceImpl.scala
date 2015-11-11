package edu.rit.csh.scaladb.raft

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging

class RaftServiceImpl(serverState: RaftServer) extends RaftService.FutureIface with LazyLogging {

  /**
   * Invoked by candidates to gather votes
   */
  def vote(requestVote: RequestVote): Future[VoteResponse] = Future {
    logger.debug("received RequestVote RPC")
    val currentTerm = serverState.getCurrentTerm

    // If RPC request or response contains term T > currentTerm:
    // set currentTerm = T, convert to follower
    if (requestVote.term > currentTerm) {
      serverState.setTerm(requestVote.term)
      serverState.toFollower()
    }


    if (requestVote.term < currentTerm) {
      VoteResponse(term = currentTerm, voteGranted = false)
    } else {
      val votedFor = serverState.getVotedFor
      val commitIndex = serverState.getCommitIndex

      // Each server will vote for at most one candidate in a
      // given term, on a first-come-first-served basis
      if ((votedFor.contains(requestVote.candidateId) || votedFor.isEmpty) && requestVote.lastLogIndex >= commitIndex) {
        serverState.setVotedFor(requestVote.candidateId) // updates the current voted for
        VoteResponse(term = currentTerm, voteGranted = true)
        serverState.setHeartbeat()
      } else {
        VoteResponse(term = currentTerm, voteGranted = false)
      }
    }
  }

  def append(entries: AppendEntries): Future[AppendResponse] = Future {
    val currentTerm = serverState.getCurrentTerm
    val commitIndex = serverState.getCommitIndex


    // Reply false if term < currentTerm
    if (entries.term < currentTerm) {
      AppendResponse(term = currentTerm, success = false)
    } else {
      // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
      serverState.getLogEntry(entries.prevLogIndex) match {
        case Some(entry) => if (entry.term != entries.prevLogTerm) {
          AppendResponse(term = currentTerm, success = false)
        } else {
          AppendResponse(term = currentTerm, success = true)
        }
        case None => AppendResponse(term = currentTerm, success = false)
      }
    }
  }
}