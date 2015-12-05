package edu.rit.csh.scaladb.raft.server

import java.net.InetSocketAddress

import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.typesafe.scalalogging.LazyLogging
import edu.rit.csh.scaladb.raft.client.ClientOperations.{FinagledService => ClientService}
import edu.rit.csh.scaladb.raft.server.internal.RaftServer
import edu.rit.csh.scaladb.raft.server.util.ParserUtils
import org.apache.thrift.protocol.TBinaryProtocol.Factory

import ParserUtils._

object RaftMain extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val pArgs: Option[ArgMap] = parseArgs(Set("self", "client", "peers"), args)
    pArgs.foreach { pArgs =>

      val (id, address) = parse[(Int, InetSocketAddress)](pArgs("self"))
      val otherServers = parse[Array[(Int, InetSocketAddress)]](pArgs("peers"))
      val client = parse[InetSocketAddress](pArgs("client"))

      logger.info(s"self configuration: $id")
      logger.info(s"other configurations: ${otherServers.mkString(", ")}")

      val stateMachine = new MemoryStateMachine()
      val raftServer = RaftServer(stateMachine, id, address, otherServers)

      val clientImpl = new RaftClientServiceImpl(raftServer)
      val clientService = new ClientService(clientImpl, new Factory())

      ServerBuilder()
        .bindTo(client)
        .codec(ThriftServerFramedCodec())
        .name("Raft Client Service")
        .build(clientService)

      println("Service finished being initialized")
    }
  }
}
