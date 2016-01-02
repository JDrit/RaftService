package edu.rit.csh.jdb.scaladb.raft.admin

import com.twitter.finagle.Thrift
import com.twitter.util.Await
import edu.rit.csh.scaladb.raft.admin.AdminService

object Admin {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("provide URL to connect to")
      System.exit(1)
    }
    val url = args(0)
    val clientServiceIface = Thrift.newIface[AdminService.FutureIface](url)
    println(Await.result(clientServiceIface.stats()))
  }
}
