package edu.rit.csh.jdb.scaladb.server

import com.twitter.logging.Logger
import com.twitter.util.{Future, Time}
import edu.rit.csh.scaladb.raft.{StateMachine, MessageSerializer, Result, Command}

import scala.collection.mutable

case class Get(override val client: String, override val id: Int, key: String) extends Command(client, id)
case class Put(override val client: String, override val id: Int, key: String, value: String) extends Command(client, id)
case class Delete(override val client: String, override val id: Int, key: String) extends Command(client, id)
case class CAS(override val client: String, override val id: Int, key: String, current: String, newVal: String) extends Command(client, id)
case class Append(override val client: String, override val id: Int, key: String, value: String) extends Command(client, id)

case class GetResult(override val id: Int, value: Option[String]) extends Result(id)
case class PutResult(override val id: Int, overridden: Boolean) extends Result(id)
case class DeleteResult(override val id: Int, deleted: Boolean) extends Result(id)
case class CASResult(override val id: Int, replaced: Boolean) extends Result(id)
case class AppendResult(override val id: Int, newValue: String) extends Result(id)

class MemoryStateMachine extends StateMachine {
  private val storage = mutable.Map.empty[String, String]
  private val log = Logger.get(getClass)

  override def compute(cmd: Command): Result = {
    log.debug(s"computing command: $cmd")
    cmd match {
      case Get(client, id, key) => GetResult(id, storage.get(key))
      case Put(client, id, key, value) =>
        val replaced = storage.contains(key)
        storage.put(key, value)
        PutResult(id, replaced)
      case Delete(client, id, key) =>
        val exists = storage.contains(key)
        storage.remove(key)
        DeleteResult(id, exists)
      case CAS(client, id, key, current, newVal) =>
        if (storage.get(key).contains(current)) {
          storage.put(key, newVal)
          CASResult(id, true)
        } else {
          CASResult(id, false)
        }
      case Append(client, id, key, value) => storage.get(key) match {
        case Some(curValue) =>
          val newValue = curValue + value
          storage.put(key, newValue)
          AppendResult(id, newValue)
        case None =>
          storage.put(key, value)
          AppendResult(id, value)
      }
    }
  }

  val parser = new MessageSerializer[Command] {
    def serialize(command: Command): String = command match {
      case Get(client, id, key) => s"get~$client~$id~$key"
      case Put(client, id, key, value) => s"put~$client~$id~$key~$value"
      case Delete(client, id, key) => s"delete~$client~$id~$key"
      case CAS(client, id, key, current, newVal) => s"cas~$client~$id~$key~$current~$newVal"
      case Append(client, id, key, value) => s"append~$client~$id~$key~$value"
    }

    def deserialize(str: String): Command = {
      val split = str.split("~")
      split(0) match {
        case "get" => Get(split(1), split(2).toInt, split(3))
        case "put" => Put(split(1), split(2).toInt, split(3), split(4))
        case "delete" => Delete(split(1), split(2).toInt, split(3))
        case "cas" => CAS(split(1), split(2).toInt, split(3), split(4), split(5))
        case "append" => Append(split(1), split(2).toInt, split(3), split(4))
      }
    }
  }

  override def close(deadline: Time): Future[Unit] = {
    log.info("closing memory state machine")
    Future.value(Unit)
  }
}
