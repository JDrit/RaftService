package edu.rit.csh.jdb.scaladb.raft.admin

import com.twitter.finagle.Thrift
import com.twitter.util.Await
import edu.rit.csh.scaladb.raft.admin.{Server, AdminService}

object Admin {

  def main(args: Array[String]): Unit = {
    val url = args(1)
    val clientServiceIface = Thrift.newIface[AdminService.FutureIface](url)
    if (args(0) == "stats") {
      println(Await.result(clientServiceIface.stats()))
    } else if (args(0) == "config") {
      val servers = args(2).split(",").grouped(2).map { arr =>
        val split1 = arr(0).split(":")
        val split2 = arr(1).split(":")
        Server(split1(0), split1(1).toInt, split2(0), split2(1).toInt)
      }.toSeq
      println(Await.result(clientServiceIface.changeConfig(servers)))
    }
  }
}
