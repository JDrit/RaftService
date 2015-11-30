package edu.rit.csh.scaladb.raft.server

import edu.rit.csh.scaladb.raft.server.storage.MemStorage

abstract class Operation(val id: Int) extends Command(id)
case class Get(override val id: Int, key: String) extends Operation(id)
case class Put(override val id: Int, key: String, value: String) extends Operation(id)
case class Delete(override val id: Int, key: String) extends Operation(id)
case class CAS(override val id: Int, key: String, current: String, newVal: String) extends Operation(id)

abstract class OpResult(val id: Int) extends Result(id)
case class GetResult(override val id: Int, value: Option[String]) extends OpResult(id)
case class PutResult(override val id: Int, overrided: Boolean) extends OpResult(id)
case class DeleteResult(override val id: Int, deleted: Boolean) extends OpResult(id)
case class CASResult(override val id: Int, replaced: Boolean) extends OpResult(id)

/**
 * Abstract state machine that runs the commands in the Raft algorithm
 * @tparam C the type of the command being run
 * @tparam R the type of results that are returned
 */
abstract class StateMachine[C <: Command, R <: Result] {

  /**
   * This is called with the command that needs to be run on the state machine. It needs to return
   * the result of the command. The commands given are guaranteed to be linearizable and that
   * there are no repeats
   * @param command the command to be run on the system
   * @return the result of the command
   */
  def applyLog(command: C): R

}

class MemoryStateMachine extends StateMachine[Operation, OpResult] {
  val storage = new MemStorage()

  override def applyLog(cmd: Operation): OpResult = cmd match {
    case Get(id, key) => GetResult(id, Some("value"))
    case Put(id, key, value) => PutResult(id, false)
    case Delete(id, key) => DeleteResult(id, false)
    case CAS(id, key, current, newVal) => CASResult(id, false)
  }


  object Implicits {
    implicit val parser = new MessageSerializer[Operation] {

      def serialize(command: Operation): String = command match {
        case Get(id, key) => s"get:$id:$key"
        case Put(id, key, value) => s"put:$id:$key:$value"
        case Delete(id, key) => s"delete:$id:$key"
        case CAS(id, key, current, newVal) => s"cas:$id:$key:$current:$newVal"
      }

      def deserialize(str: String): Operation = {
        val split = str.split(":")
        split(0) match {
          case "get" => Get(split(0).toInt, split(1))
          case "put" => Put(split(0).toInt, split(1), split(2))
          case "delete" => Delete(split(0).toInt, split(1))
          case "cas" => CAS(split(0).toInt, split(1), split(2), split(3))
        }
      }
    }
  }


}
