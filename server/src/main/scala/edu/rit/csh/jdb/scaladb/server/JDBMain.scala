package edu.rit.csh.jdb.scaladb.server

import java.lang.management.ManagementFactory
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

  val addr: Flag[InetSocketAddress] = flag("raftAddr", new InetSocketAddress("localhost", 5000), "Bind address for raft")
  val ownAddr: Flag[InetSocketAddress] = flag("client", new InetSocketAddress("localhost", 6000), "Bind address for clients")
  val raftAddrs: Flag[Seq[InetSocketAddress]] = flag("peers", List.empty, "other Raft peers")
  val serverAddrs: Flag[Seq[InetSocketAddress]] = flag("servers", List.empty, "other server addresses")

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

  def process(command: Command)(implicit server: RaftServer): Future[Output[Json]] = server.submit(command) match {
    case NotLeaderResult(leader) =>
      val addr = server.getLeader()
        .map(peer => s"http://${peer.clientAddr.getHostName}:${peer.clientAddr.getPort}")
        .getOrElse("")
      val request = command match {
        case Get(client, id, key) => Request.queryString(s"$addr/get", ("client", client), ("id", id.toString), ("key", key))
        case Put(client, id, key, value) => Request.queryString(s"$addr/put", ("client", client), ("id", id.toString), ("key", key), ("value", value))
        case Delete(client, id, key) => Request.queryString(s"$addr/delete", ("client", client), ("id", id.toString), ("key", key))
        case CAS(client, id, key, currV, newV) => Request.queryString(s"$addr/cas", ("client", client), ("id", id.toString), ("key", key), ("current", currV), ("new", newV))
        case Append(client, id, key, value) =>  Request.queryString(s"$addr/append", ("client", client), ("id", id.toString), ("key", key), ("value", value))
      }
      Future.value(TemporaryRedirect(new Exception()).withHeader(("Location", request)))
    case SuccessResult(futures) => futures.map {
      case Left(highest) => BadRequest(new Exception(
        Json.obj("message" -> "command has already been seen".asJson,
        "errorCode" -> 101.asJson, "highest" -> highest.asJson).toString()))
      case Right(result) => Ok(result.asJson)
    }
  }

  def getPid: Int = ManagementFactory.getRuntimeMXBean.getName.split("@")(0).toInt

  def main(): Unit = {
    implicit val raftServer = RaftServer(new MemoryStateMachine(), addr(), ownAddr(), raftAddrs(), serverAddrs(), Seq(this))

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
        log.debug(s"server exception thrown ${e.getStackTrace.mkString("\n")}", e)
        NotFound(e)
    }).toService

    val server = Http.server
      .configured(Stats(statsReceiver))
      .serve(ownAddr(), api)
    closeOnExit(server)
    log.info("Service finished being initialized")
    onExit { close() }
    Await.ready(server)
  }
}
