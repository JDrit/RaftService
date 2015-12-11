package edu.rit.csh.jdb.server

import java.net.InetSocketAddress

import com.twitter.app.Flag

import com.twitter.finagle.param.Stats
import com.twitter.finagle.Service
import com.twitter.finagle.Http
import com.twitter.finagle.http.{Request, Response}
import com.twitter.logging.Logging
import com.twitter.server.TwitterServer
import com.twitter.util.{Future, Await}
import edu.rit.csh.scaladb.raft._
import io.circe.{Encoder, Json}
import io.finch._
import io.circe.generic.auto._
import io.finch.circe._
import io.circe.generic.auto._
import io.circe.syntax._

object RaftMain extends TwitterServer with Logging {

  val addr: Flag[InetSocketAddress] = flag("raftAddr", new InetSocketAddress(5000), "Bind address for raft")
  val clientAddr: Flag[InetSocketAddress] = flag("client", new InetSocketAddress(6000), "Bind address for clients")
  val peersAddrs: Flag[Seq[InetSocketAddress]] = flag("peers", List.empty, "Raft peers")
  val counter = statsReceiver.counter("requests_counter")

  implicit val resultEncoder = new Encoder[Result] {
    def apply(result: Result): Json = result match {
      case GetResult(id, value) => Json.obj("id" -> id.asJson, "value" -> value.asJson)
      case PutResult(id, overridden) => Json.obj("id" -> id.asJson, "overridden" -> overridden.asJson)
      case DeleteResult(id, deleted) => Json.obj("id" -> id.asJson, "deleted" -> deleted.asJson)
      case CASResult(id, replaced) => Json.obj("id" -> id.asJson, "replaced" -> replaced.asJson)
      case AppendResult(id, value) => Json.obj("id" -> id.asJson, "value" -> value.asJson)
    }
  }

  def process(command: Command)(implicit server: RaftServer): Future[Output[Json]] = {
    counter.incr()
    server.submit(command) match {
      case NotLeaderResult(leader) =>
        Future.value(Ok(Json.obj("exception" -> Json.string("not leader"), "leader" -> Json.string(leader))))
      case SuccessResult(futures) => futures.map {
        case Left(highest) =>
          Ok(Json.obj("exception" -> Json.string("command has already been seen"), "highest" -> Json.int(highest)))
        case Right(result) =>
          Ok(result.asJson)
      }
    }
  }

  def main(): Unit = {
    val stateMachine = new MemoryStateMachine()
    implicit val raftServer = RaftServer(stateMachine, addr(), peersAddrs())

    val getReader = (param("client") :: param("id").as[Int] :: param("key")).as[Get]
    val putReader = (param("client") :: param("id").as[Int] :: param("key") :: param("value")).as[Put]
    val deleteReader = (param("client") :: param("id").as[Int] :: param("key")).as[Delete]
    val casReader = (param("client") :: param("id").as[Int] :: param("key") :: param("current") :: param("new")).as[CAS]
    val appendReader = (param("client") :: param("id").as[Int] :: param("key") :: param("value")).as[Append]

    val getEndpoint: Endpoint[Json] = get("get" ? getReader) { get: Get => process(get) }
    val putEndpoint: Endpoint[Json] = get("put" ? putReader) { put: Put => process(put) }
    val deleteEndpoint: Endpoint[Json] = get("delete" ? getReader) { get: Get => process(get) }
    val casEndpoint: Endpoint[Json] = get("cas" ? casReader) { cas: CAS => process(cas) }
    val appendEndpoint: Endpoint[Json] = get("append" ? appendReader) { append: Append => process(append) }

    val api: Service[Request, Response] = (
      getEndpoint :+: putEndpoint :+: deleteEndpoint :+: casEndpoint :+: appendEndpoint
    ).handle({
      case e: Exception => NotFound(e)
    }).toService

    val server = Http.server
      .configured(Stats(statsReceiver))
      .serve(clientAddr(), api)

    log.info("Service finished being initialized")

    onExit { server.close() }
    Await.ready(server)


  }

}
