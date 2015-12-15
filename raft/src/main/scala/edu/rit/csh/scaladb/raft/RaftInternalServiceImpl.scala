package edu.rit.csh.scaladb.raft

import com.twitter.logging.{Logger, Logging}
import com.twitter.util.Future
import edu.rit.csh.scaladb.raft.InternalService.FutureIface
import edu.rit.csh.scaladb.raft.MessageConverters._

/**
 * This is the Thrift Finagle service that is used to communicate between the different nodes
 * in the cluster. It only implements the necessary endpoints for inter-node communication and
 * does nothing in regard to client operations
 * @param serverState the class used to get the start of the Raft server
 */
private[raft] class RaftInternalServiceImpl(serverState: RaftServer) extends FutureIface {

  private val log = Logger.get(getClass)

  /**
   * The endpoint called when other nodes want this node to vote for it during leader
   * election. It sees if this node can vote and returns the result
   * @param requestVote the request for this server to vote
   * @return the result of this node voting
   */
  override def vote(requestVote: RequestVote): Future[VoteResponse] = Future {
   serverState.synchronized {
     val currentTerm = serverState.getCurrentTerm()

     // If RPC request or response contains term T > currentTerm:
     // set currentTerm = T, convert to follower
     if (requestVote.term > currentTerm) {
       serverState.setTerm(requestVote.term)
       serverState.toFollower()
     }

     if (requestVote.term < currentTerm) {
       log.info(s"voted NO for ${requestVote.candidateId}")
       VoteResponse(term = currentTerm, voteGranted = false)
     } else {
       val votedFor = serverState.getVotedFor()
       val commitIndex = serverState.getCommitIndex()

       // Each server will vote for at most one candidate in a
       // given term, on a first-come-first-served basis
       if ((votedFor.contains(requestVote.candidateId) || votedFor.isEmpty) && requestVote.lastLogIndex >= commitIndex) {
         serverState.setVotedFor(requestVote.candidateId) // updates the current voted for
         log.info(s"voted YES for ${requestVote.candidateId}")
         VoteResponse(term = currentTerm, voteGranted = true)
       } else {
         log.info(s"voted NO for ${requestVote.candidateId}")
         VoteResponse(term = currentTerm, voteGranted = false)
       }
     }
   }
  }

  override def append(entries: AppendEntries): Future[AppendEntriesResponse] = Future {
    serverState.synchronized {
      val currentTerm = serverState.getCurrentTerm(entries.term)
      val commitIndex = serverState.getCommitIndex()
      serverState.setHeartbeat(entries.term)
      serverState.setLeaderId(entries.leaderId)

      if (entries.entires.nonEmpty) {
        log.debug(s"got append of size ${entries.entires.size}")
      }

      // Reply false if term < currentTerm
      if (entries.term < currentTerm) {
        AppendEntriesResponse(term = currentTerm, success = false)
        // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
      } else if (entries.prevLogIndex != -1 &&
          !serverState.getLogEntry(entries.prevLogIndex).exists(_.term == entries.prevLogTerm)) {
        AppendEntriesResponse(term = currentTerm, success = false)
      } else {
        entries.entires.map(thriftToLogEntry).foreach(serverState.appendLog)
        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if (!serverState.isLeader()) {
          if (entries.leaderCommit > commitIndex) {
            val newCommit = entries.entires.lastOption
              .map(entry => Math.min(entry.index, entries.leaderCommit))
              .getOrElse(entries.leaderCommit)
            serverState.setCommitIndex(newCommit)
          }
        }
        AppendEntriesResponse(term = currentTerm, success = true)
      }
    }
  }

  override def stats(): Future[String] = Future.value(serverState.toString())

  override def changeConfig(servers: Seq[String]) = serverState.newConfiguration(servers) match {
    case SuccessResult(futures) => futures.map(_ => true)
    case NotLeaderResult(leader) => Future.value(false)
  }
}