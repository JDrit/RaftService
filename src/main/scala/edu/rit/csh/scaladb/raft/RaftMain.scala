package edu.rit.csh.scaladb.raft

import java.net.InetSocketAddress

import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.typesafe.scalalogging.LazyLogging
import edu.rit.csh.scaladb.raft.RaftService.FinagledService
import edu.rit.csh.scaladb.raft.storage.MemStorage
import org.apache.thrift.protocol.TBinaryProtocol.Factory

import ParserUtils._

case class Arguments(serverId: Int, address: String, port: Int)

object RaftMain extends LazyLogging {

  implicit def peerParse = new Parser[Peer] {
    def apply(str: String): Peer = {
      val splits = str.split("-")
      new Peer(splits(0).toInt, splits(1), 1, 0)
    }
  }

  def main(args: Array[String]): Unit = {

    val pArgs: Option[ArgMap] = parseArgs(Set("server-id", "address", "port", "peers"), args)
    pArgs.foreach { pArgs =>
      val serverId = parse[Int](pArgs("server-id"))
      val address = parse[String](pArgs("address"))
      val port = parse[Int](pArgs("port"))
      val otherServers = parse[Array[Peer]](pArgs("peers"))

      val config = new Peer(serverId, address, 1, 0)
      val storage = new MemStorage()

      logger.info(s"self configuration: $config")
      logger.info(s"other configurations: ${otherServers.mkString(",")}")

      val processor = new RaftServiceImpl(new RaftServer(config, Array(config) ++ otherServers, storage))
      val service = new FinagledService(processor, new Factory())

      ServerBuilder()
        .bindTo(new InetSocketAddress(port))
        .codec(ThriftServerFramedCodec())
        .name("Raft")
        .build(service)

      println("Service finished being initialized")
    }
  }
}
