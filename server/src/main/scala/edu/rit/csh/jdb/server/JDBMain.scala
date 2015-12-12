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
import io.finch.circe._
import io.circe.syntax._

object JDBMain extends TwitterServer with Logging {

  val addr: Flag[InetSocketAddress] = flag("raftAddr", new InetSocketAddress(5000), "Bind address for raft")
  val clientAddr: Flag[InetSocketAddress] = flag("client", new InetSocketAddress(6000), "Bind address for clients")
  val peersAddrs: Flag[Seq[InetSocketAddress]] = flag("peers", List.empty, "Raft peers")

  val getCounter = statsReceiver.scope("client").counter("requests_counter_get")
  val putCounter = statsReceiver.scope("client").counter("requests_counter_put")
  val deleteCounter = statsReceiver.scope("client").counter("requests_counter_delete")
  val casCounter = statsReceiver.scope("client").counter("requests_counter_cas")
  val appendCounter = statsReceiver.scope("client").counter("requests_counter_append")

  val getReader = (param("client") :: param("id").as[Int] :: param("key")).as[Get]
  val putReader = (param("client") :: param("id").as[Int] :: param("key") :: param("value")).as[Put]
  val deleteReader = (param("client") :: param("id").as[Int] :: param("key")).as[Delete]
  val casReader = (param("client") :: param("id").as[Int] :: param("key") :: param("current") :: param("new")).as[CAS]
  val appendReader = (param("client") :: param("id").as[Int] :: param("key") :: param("value")).as[Append]

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
    server.submit(command) match {
      case NotLeaderResult(leader) =>
        Future.value(Ok(Json.obj("exception" -> "not leader".asJson, "leader" -> leader.asJson)))
      case SuccessResult(futures) => futures.map {
        case Left(highest) =>
          Ok(Json.obj("exception" -> "command has already been seen".asJson, "highest" -> highest.asJson))
        case Right(result) =>
          Ok(result.asJson)
      }
    }
  }

  def main(): Unit = {
    implicit val raftServer = RaftServer(new MemoryStateMachine(), addr(), peersAddrs())

    val getEndpoint: Endpoint[Json] = get("get" ? getReader) { get: Get =>
      getCounter.incr()
      process(get)
    }
    val putEndpoint: Endpoint[Json] = get("put" ? putReader) { put: Put =>
      putCounter.incr()
      process(put)
    }
    val deleteEndpoint: Endpoint[Json] = get("delete" ? deleteReader) { delete: Delete =>
      deleteCounter.incr()
      process(delete)
    }
    val casEndpoint: Endpoint[Json] = get("cas" ? casReader) { cas: CAS =>
      casCounter.incr()
      process(cas)
    }
    val appendEndpoint: Endpoint[Json] = get("append" ? appendReader) { append: Append =>
      appendCounter.incr()
      process(append)
    }

    val api: Service[Request, Response] = (
      getEndpoint :+: putEndpoint :+: deleteEndpoint :+: casEndpoint :+: appendEndpoint
    ).handle({
      case e: Exception =>
        log.debug(s"exception thrown $e")
        NotFound(e)
    }).toService

    val server = Http.server
      .configured(Stats(statsReceiver))
      .serve(clientAddr(), api)

    log.info("Service finished being initialized")

    onExit { server.close() }
    Await.ready(server)
  }
}
