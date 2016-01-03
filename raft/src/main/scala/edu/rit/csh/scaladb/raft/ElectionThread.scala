package edu.rit.csh.scaladb.raft

import com.twitter.util.{Future, Time, Closable}

private[raft] class ElectionThread(server: RaftServer) extends Closable {

  @volatile
  private var running = true

  private val thread = new Thread {
    override def run(): Unit = {
      while (running) {
        val sleep = server.electionTimeout
        Thread.sleep(sleep.inMilliseconds)
        if (server.status.get() != ServerType.Leader) {
          // If a follower receives no communication over a period of time
          // called the election timeout, then it assumes there is no viable
          // leader and begins an election to choose a new leader
          if (server.getLastHeartbeat > sleep) {
            server.startElection()
          }
        }
      }
    }
  }

  thread.start()

  override def close(deadline: Time): Future[Unit] = {
    running = false
    Future(thread.join())
  }
}
