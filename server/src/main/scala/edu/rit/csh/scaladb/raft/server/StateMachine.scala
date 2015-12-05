package edu.rit.csh.scaladb.raft.server


import edu.rit.csh.scaladb.raft.server.internal.{MessageSerializer, Result, Command}

import scala.collection.mutable

case class Get(id: Int, key: String) extends Command(id)
case class Put(id: Int, key: String, value: String) extends Command(id)
case class Delete(id: Int, key: String) extends Command(id)
case class CAS(id: Int, key: String, current: String, newVal: String) extends Command(id)

case class GetResult(id: Int, value: Option[String]) extends Result(id)
case class PutResult(id: Int, overrided: Boolean) extends Result(id)
case class DeleteResult(id: Int, deleted: Boolean) extends Result(id)
case class CASResult(id: Int, replaced: Boolean) extends Result(id)

/**
 * Abstract state machine that runs the commands in the Raft algorithm
 */
abstract class StateMachine {

  /**
   * This is called with the command that needs to be run on the state machine. It needs to return
   * the result of the command. The commands given are guaranteed to be linearizable and that
   * there are no repeats
   * @param command the command to be run on the system
   * @return the result of the command
   */
  def applyLog(command: Command): Result


  /**
   * The state machine needs to define the message serializer for the type of
   * commands that it uses
   */
  val parser: MessageSerializer[Command]

}

class MemoryStateMachine extends StateMachine {
  private val storage = mutable.Map.empty[String, String]
  private val lock = new Object()

  override def applyLog(cmd: Command): Result = lock.synchronized(cmd match {
    case Get(id, key) => GetResult(id, storage.get(key))
    case Put(id, key, value) =>
      val replaced = storage.contains(key)
      storage.put(key, value)
      PutResult(id, replaced)
    case Delete(id, key) => DeleteResult(id, storage.remove(key).isDefined)
    case CAS(id, key, current, newVal) =>
      if (storage.get(key).contains(current)) CASResult(id, true)
      else CASResult(id, false)
  })

  val parser = new MessageSerializer[Command] {
    def serialize(command: Command): String = command match {
      case Get(id, key) => s"get:$id:$key"
      case Put(id, key, value) => s"put:$id:$key:$value"
      case Delete(id, key) => s"delete:$id:$key"
      case CAS(id, key, current, newVal) => s"cas:$id:$key:$current:$newVal"
    }

    def deserialize(str: String): Command = {
      val split = str.split(":")
      split(0) match {
        case "get" => Get(split(1).toInt, split(2))
        case "put" => Put(split(1).toInt, split(2), split(3))
        case "delete" => Delete(split(1).toInt, split(2))
        case "cas" => CAS(split(1).toInt, split(2), split(3), split(4))
      }
    }
  }


}
