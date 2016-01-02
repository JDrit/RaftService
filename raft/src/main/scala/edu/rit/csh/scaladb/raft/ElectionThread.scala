package edu.rit.csh.scaladb.raft

private[raft] class ElectionThread(server: RaftServer) extends Thread {

  override def run(): Unit = {
    while (true) {
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
