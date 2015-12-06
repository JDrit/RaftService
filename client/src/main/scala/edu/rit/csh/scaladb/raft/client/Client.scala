package edu.rit.csh.scaladb.raft.client

import com.twitter.conversions.time._
import com.twitter.finagle.Thrift
import com.twitter.finagle.filter.MaskCancelFilter
import com.twitter.finagle.service.{TimeoutFilter, RetryPolicy, RetryExceptionsFilter}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Await, Future}

object Client {

  /**
   * Creates the standard client connection that is used to communicate to other Raft servers
   * @param fun the function that creates the request, which generates a Future for the response
   *            from the remote server
   * @tparam I the type of the request
   * @tparam O the type of the remote server's response
   * @return the function that sends the request to the server, returning a Future response
   */
  private def createClient[I, O](fun: I => Future[O]): I => Future[O] = {
    val retry = new RetryExceptionsFilter[I, O](RetryPolicy.tries(3), DefaultTimer.twitter)
    val timeout = new TimeoutFilter[I, O](3.seconds, DefaultTimer.twitter)
    val maskCancel = new MaskCancelFilter[I, O]()
    retry andThen timeout andThen maskCancel andThen fun
  }

  def main(args: Array[String]): Unit = {

    val address = args(0)
    val thrift = Thrift.newIface[ClientOperations.FutureIface](address)
    val clientID = "test"
    val key = "jd"

    val response = args(1) match {
      case "get" => thrift.get(GetRequest(clientID, args(2).toInt, args(3)))
      case "put" => thrift.put(PutRequest(clientID, args(2).toInt, args(3), args(4)))
      case "delete" => thrift.delete(DeleteRequest(clientID, args(2).toInt, args(3)))
      case "cas" => thrift.cas(CASRequest(clientID, args(2).toInt, args(3), args(4), args(5)))
      case "stress" =>
        println(Await.result(thrift.put(PutRequest(clientID, 0, key, "0"))))
        val futures = Future.collect((1 to 100).map { commandID =>
          try {
            thrift.append(AppendRequest(clientID, commandID, key, s"-$commandID"))
          } catch {
            case ex: AlreadySeen =>
              println(s"$commandID - ${ex.highest}")
              Future.value(AppendResponse(Some("")))
          }
        }.toList)
        Await.result(futures)
        thrift.get(GetRequest(clientID, 101, key))
    }

    try {
      val message = Await.result(response)
      println(message)
    } catch {
      case ex: NotLeader => println(ex)
      case ex: AlreadySeen => println(ex)
    }
  }
}
