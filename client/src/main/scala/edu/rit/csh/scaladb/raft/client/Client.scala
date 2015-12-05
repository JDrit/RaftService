package edu.rit.csh.scaladb.raft.client

import java.util.UUID

import com.twitter.conversions.time._
import com.twitter.finagle.Thrift
import com.twitter.finagle.filter.MaskCancelFilter
import com.twitter.finagle.service.{TimeoutFilter, RetryPolicy, RetryExceptionsFilter}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Await, Future}

import scala.util.Random

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
    val commandID = new Random().nextInt()

    val response = args(1) match {
      case "get" => thrift.get(GetRequest(clientID, commandID, args(2)))
      case "put" => thrift.put(PutRequest(clientID, commandID, args(2), args(3)))
      case "delete" => thrift.delete(DeleteRequest(clientID, commandID, args(2)))
      case "cas" => thrift.cas(CASRequest(clientID, commandID, args(2), args(3), args(4)))
    }

    try {
      val message = Await.result(response)
      println(message)
    } catch {
      case ex: NotLeader => println(ex)
    }
  }
}
