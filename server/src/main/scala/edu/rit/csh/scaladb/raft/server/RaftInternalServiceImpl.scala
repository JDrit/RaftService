package edu.rit.csh.scaladb.raft.server

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging

import MessageConverters._

/**
 * This is the Thrift Finagle service that is used to communicate between the different nodes
 * in the cluster. It only implements the necessary endpoints for inter-node communication and
 * does nothing in regard to client operations
 * @param serverState the class used to get the start of the Raft server
 */
class RaftInternalServiceImpl(serverState: RaftServer) extends RaftService.FutureIface with LazyLogging {

  /**
   * The endpoint called when other nodes want this node to vote for it during leader
   * election. It sees if this node can vote and returns the result
   * @param requestVote the request for this server to vote
   * @return the result of this node voting
   */
  override def vote(requestVote: RequestVote): Future[VoteResponse] = this.synchronized(Future {
    val currentTerm = serverState.getCurrentTerm()

    // If RPC request or response contains term T > currentTerm:
    // set currentTerm = T, convert to follower
    if (requestVote.term > currentTerm) {
      serverState.setTerm(requestVote.term)
      serverState.toFollower()
    }

    if (requestVote.term < currentTerm) {
      logger.info(s"voted NO for ${requestVote.candidateId}")
      VoteResponse(term = currentTerm, voteGranted = false)
    } else {
      val votedFor = serverState.getVotedFor()
      val commitIndex = serverState.getCommitIndex()

      // Each server will vote for at most one candidate in a
      // given term, on a first-come-first-served basis
      if ((votedFor.contains(requestVote.candidateId) || votedFor.isEmpty) && requestVote.lastLogIndex >= commitIndex) {
        serverState.setVotedFor(requestVote.candidateId) // updates the current voted for
        logger.info(s"voted YES for ${requestVote.candidateId}")
        VoteResponse(term = currentTerm, voteGranted = true)
      } else {
        logger.info(s"voted NO for ${requestVote.candidateId}")
        VoteResponse(term = currentTerm, voteGranted = false)
      }
    }
  })

  override def append(entries: AppendEntries): Future[AppendResponse] = this.synchronized(Future {
    val currentTerm = serverState.getCurrentTerm(entries.term)
    val commitIndex = serverState.getCommitIndex()
    serverState.setHeartbeat(entries.term)
    serverState.setLeaderId(entries.leaderId)

    // Reply false if term < currentTerm
    if (entries.term < currentTerm) {
      AppendResponse(term = currentTerm, success = false)
    // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
    } else if (entries.prevLogIndex > -1 && !serverState.getLogEntry(entries.prevLogIndex).exists(_.term == entries.prevLogTerm)) {
      AppendResponse(term = currentTerm, success = false)
    } else {
      entries.entires.map(thriftToLogEntry).foreach(serverState.appendLog)
      // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
      if (entries.leaderCommit > commitIndex) {
        val newCommit = entries.entires.lastOption
          .map(entry => Math.min(entry.index, entries.leaderCommit))
          .getOrElse(entries.leaderCommit)
        serverState.setCommitIndex(newCommit)
      }
      AppendResponse(term = currentTerm, success = true)
    }
  })
}