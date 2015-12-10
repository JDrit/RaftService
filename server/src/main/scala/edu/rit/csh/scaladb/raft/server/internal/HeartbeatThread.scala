package edu.rit.csh.scaladb.raft.server.internal

/**
 * Thread used to send out the heartbeat to every node in the cluster. This uses a
 * monitor to detect when it becomes the leader so it knows when it should send out
 * the messages
 * @param server
 */
private[internal] class HeartbeatThread(server: RaftServer) extends Thread {

    override def  run(): Unit = {
      while (true) {
        this.synchronized {
          while (server.status.get() != ServerType.Leader) {
            this.wait()
          }
          val (prevLogIndex, prevLogTerm) = server.getPrevInfo()
          val append = AppendEntries(server.getCurrentTerm(), server.self.address, prevLogIndex,
            prevLogTerm, Seq.empty, server.getCommitIndex())
          server.peers.values.map(_.appendClient(append))
        }
        Thread.sleep(100) // send heartbeat every 100 milliseconds
      }
    }
}
