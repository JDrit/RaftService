package edu.rit.csh.scaladb.raft


import edu.rit.csh.scaladb.raft.State.State

object ServerType extends Enumeration {
  type ServerType = Value
  val Follower, Leader, Candidate = Value
}

object State extends Enumeration {
  type State = Value
  val cOld, cOldNew = Value
}

/**
 * Server configuration for all members of the cluster
 * @param id the unique ID of the server
 * @param address the address to send requests to
 * @param nextIndex index of the next log entry to send to that server
 *                  (initialized to leader last log index + 1)
 * @param matchIndex index of highest log entry known to be replicated on server
 *                   (initialized to 0, increases monotonically)
 */
case class Peer(id: Int, address: String, var nextIndex: Int, var matchIndex: Int) {

  override def toString: String =
    s"""
       |peer {
       |  server id: $id
       |  address: $address
       |  nextIndex: $nextIndex
       |  matchIndex: $matchIndex
       |}
     """.stripMargin
}

/**
 * configuration represents the sets of peers and behaviors required to
 * mplement joint-consensus.
 * @param state determines if it is the new or old configuration
 * @param cOldPeers the list of old servers
 * @param cNewPeers the list of new servers
 */
case class RaftConfiguration(state: State, cOldPeers: Array[Peer], cNewPeers: Array[Peer]) {

  override def toString: String =
    s"""
       |configuration {
       |  state: $state
       |  old peers: [${cOldPeers.mkString(", ")}]
       |  new peers: [${cNewPeers.mkString(", ")}]
       |}
     """.stripMargin
}

case class LogEntry(term: Int, index: Int, cmd: Either[String, RaftConfiguration]) {

  override def toString: String =
    s"""
       |entry {
       |  term: $term
       |  index: $index,
       |  cmd: $cmd
       |}
     """.stripMargin
}

object RaftConfiguration {

  implicit def PeerToThrift(peer: Peer): Server = Server(peer.id, peer.address)

  implicit def RaftConfigToThrift(config: RaftConfiguration): Configuration = ???

  implicit def logEntryTothrift(entry: LogEntry): Entry = entry.cmd match {
    case Left(cmd) =>
      Entry(entry.term, entry.index, EntryType.Command, Some(cmd))
    case Right(config) =>
      Entry(entry.term, entry.index, EntryType.Configuration, configuration = Some(config))
  }

  implicit def entriesToThrift(entires: Seq[LogEntry]): Seq[Entry] = entires.map(logEntryTothrift)
}