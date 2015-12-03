package edu.rit.csh.scaladb.raft.server

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging

import MessageConverters._

/**
 * This is the Thrift Finagle service that is used to communicate between the different nodes
 * in the cluster. It only implements the necessary endpoints for inter-node communication and
 * does nothing in regard to client operations
 * @param serverState the class used to get the start of the Raft server
 * @tparam C the type of commands being processed by the Raft cluster
 * @tparam R the type of the responses being returned by the Raft cluster
 */
class RaftInternalServiceImpl[C <: Command, R <: Result](serverState: RaftServer[C, R])
  extends RaftService.FutureIface with LazyLogging {


  /**
   * The endpoint called when other nodes want this node to vote for it during leader
   * election. It sees if this node can vote and returns the result
   * @param requestVote the request for this server to vote
   * @return the result of this node voting
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
      if ((votedFor.contains(requestVote.candidateId) || votedFor.isEmpty) &&
          requestVote.lastLogIndex >= commitIndex) {
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

    if (entries.term < currentTerm) { // Reply false if term < currentTerm
      AppendResponse(term = currentTerm, success = false)
    } else {
      serverState.getLogEntry(entries.prevLogIndex) match {
        case Some(entry) if entry.term == entries.prevLogTerm => appendSuccess(entries, currentTerm)
        case None => appendSuccess(entries, currentTerm)
        case _ =>
          // Reply false if log does not contain an entry at prevLogIndex
          // whose term matches prevLogTerm
          AppendResponse(term = currentTerm, success = false)
      }
    }
  }

  private def appendSuccess(entries: AppendEntries, currentTerm: Int): AppendResponse = {
    val commitIndex = serverState.getCommitIndex()
    serverState.appendLog(entries.entires.map(thriftToLogEntry))
    // If leaderCommit > commitIndex, set commitIndex =
    // min(leaderCommit, index of last new entry)
    if (entries.leaderCommit > commitIndex) {
      serverState.setCommitIndex(Math.min(commitIndex,
        entries.entires.lastOption.map(_.index).getOrElse(commitIndex)))
    }
    AppendResponse(term = currentTerm, success = true)
  }
}