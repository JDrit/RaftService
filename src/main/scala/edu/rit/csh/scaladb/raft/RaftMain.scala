package edu.rit.csh.scaladb.raft

import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.typesafe.scalalogging.LazyLogging
import edu.rit.csh.scaladb.raft.RaftService.FinagledService
import edu.rit.csh.scaladb.raft.storage.MemStorage
import org.apache.thrift.protocol.TBinaryProtocol.Factory

import ParserUtils._

case class Arguments(serverId: Int, address: String, port: Int)

object RaftMain extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val pArgs: Option[ArgMap] = parseArgs(Set("self", "peers"), args)
    pArgs.foreach { pArgs =>
      val config = parse[Peer](pArgs("self"))
      val otherServers = parse[Array[Peer]](pArgs("peers"))
      val storage = new MemStorage()

      logger.info(s"self configuration: $config")
      logger.info(s"other configurations: ${otherServers.mkString("")}")

      val processor = new RaftServiceImpl(new RaftServer(config, Array(config) ++ otherServers, storage))
      val service = new FinagledService(processor, new Factory())

      ServerBuilder()
        .bindTo(config.address)
        .codec(ThriftServerFramedCodec())
        .name("Raft")
        .build(service)

      println("Service finished being initialized")
    }
  }
}
