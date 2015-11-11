package edu.rit.csh.scaladb.raft

import java.net.InetSocketAddress

import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import edu.rit.csh.scaladb.raft.RaftService.FinagledService
import org.apache.thrift.protocol.TBinaryProtocol.Factory

object RaftMain {

  def main(args: Array[String]): Unit = {
    val port = 5999
    val processor = new RaftServiceImpl(new RaftServer("localhost:5999", Array.empty))
    val service = new FinagledService(processor, new Factory())

    ServerBuilder()
      .bindTo(new InetSocketAddress(port))
      .codec(ThriftServerFramedCodec())
      .name("Raft")
      .build(service)

    println("Service finished being initialized")
  }
}
