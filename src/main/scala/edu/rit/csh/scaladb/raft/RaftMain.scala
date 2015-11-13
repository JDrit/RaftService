package edu.rit.csh.scaladb.raft

import java.net.InetSocketAddress

import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.typesafe.scalalogging.LazyLogging
import edu.rit.csh.scaladb.raft.RaftService.FinagledService
import edu.rit.csh.scaladb.raft.storage.MemStorage
import org.apache.thrift.protocol.TBinaryProtocol.Factory

import scala.annotation.tailrec

case class Arguments(serverId: Int, address: String, port: Int)

object RaftMain extends LazyLogging {

  type ArgMap = Map[String, String]

  @tailrec
  def nextArg(flags: Set[String], input: List[String], accum: ArgMap): ArgMap =
    if (flags.isEmpty) accum
    else input match {
      case x :: y :: xs =>
        flags.collectFirst {
          case flag if "--" + flag == x || "-" + flag == x => flag
        } match {
          case Some(flag) =>
            nextArg(flags - flag, xs, accum + (flag -> y))
          case None => logger.warn(s"unknown flag: $x") ; accum
        }
      case _ => accum
    }

  def parseArgs(flags: Set[String], args: Array[String]): ArgMap =
    nextArg(flags, args.flatMap(_.split("=")).toList, Map.empty)


  def main(args: Array[String]): Unit = {

    val pArgs: ArgMap = parseArgs(Set("server-id", "address", "port"), args)
    val serverId = pArgs.getOrElse("server-id", "1").toInt
    val address = pArgs.getOrElse("address", "localhost:5000")
    val port = pArgs.getOrElse("port", "5000").toInt

    val config = new Peer(serverId, address, 1, 0)
    val storage = new MemStorage()

    logger.info(s"starting server $serverId, binding to $address")


    val processor = new RaftServiceImpl(new RaftServer(config, Array(config), storage))
    val service = new FinagledService(processor, new Factory())

    ServerBuilder()
      .bindTo(new InetSocketAddress(port))
      .codec(ThriftServerFramedCodec())
      .name("Raft")
      .build(service)

    println("Service finished being initialized")
  }
}
