package edu.rit.csh.scaladb.raft.server

import java.net.InetSocketAddress

import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.typesafe.scalalogging.LazyLogging
import edu.rit.csh.scaladb.raft.server.RaftService.{FinagledService => InternalService}
import edu.rit.csh.scaladb.raft.client.ClientOperations.{FinagledService => ClientService}
import edu.rit.csh.scaladb.raft.server.storage.MemStorage
import org.apache.thrift.protocol.TBinaryProtocol.Factory

import ParserUtils._

object RaftMain extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val pArgs: Option[ArgMap] = parseArgs(Set("self", "client", "peers"), args)
    pArgs.foreach { pArgs =>
      val config = parse[Peer](pArgs("self"))
      val otherServers = parse[Array[Peer]](pArgs("peers"))
      val client = parse[InetSocketAddress](pArgs("client"))
      val storage = new MemStorage()

      logger.info(s"self configuration: $config")
      logger.info(s"other configurations: ${otherServers.mkString("")}")

      val internalService = new InternalService(
        new RaftInternalServiceImpl(new RaftServer(config, Array(config) ++ otherServers, storage)),
        new Factory())
      val clientService = new ClientService( new RaftClientService(), new Factory())

      ServerBuilder()
        .bindTo(config.address)
        .codec(ThriftServerFramedCodec())
        .name("Raft Internal Service")
        .build(internalService)

      ServerBuilder()
        .bindTo(client)
        .codec(ThriftServerFramedCodec())
        .name("Raft Client Service")
        .build(clientService)

      println("Service finished being initialized")
    }
  }
}
