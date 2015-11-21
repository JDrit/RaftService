package edu.rit.csh.scaladb.raft.server

import edu.rit.csh.scaladb.raft.server.storage.MemStorage

abstract class Operation(val id: String) extends Command(id)
case class Get(override val id: String, key: String) extends Operation(id)
case class Put(override val id: String, key: String, value: String) extends Operation(id)
case class Delete(override val id: String, key: String) extends Operation(id)
case class CAS(override val id: String, key: String, current: String, newVal: String) extends Operation(id)

abstract class OpResult(val id: String) extends Result(id)
case class GetResult(override val id: String, value: String) extends OpResult(id)
case class PutResult(override val id: String, overrided: Boolean) extends OpResult(id)
case class DeleteResult(override val id: String, deleted: Boolean) extends OpResult(id)
case class CASResult(override val id: String, replaced: Boolean) extends OpResult(id)

/**
 * Abstract state machine that runs the commands in the Raft algorithm
 * @tparam C the type of the command being run
 * @tparam R the type of results that are returned
 */
abstract class StateMachine[C <: Command, R <: Result] {

  def applyLog(command: C): R

  def serializeCommand(command: C): String

  def serializeResult(result: R): String

  def deserializeCommand(str: String): C

  def deserializeResult(str: String): R
}

class MemoryStateMachine extends StateMachine[Operation, OpResult] {
  val storage = new MemStorage()

  override def applyLog(cmd: Operation): OpResult = cmd match {
    case Get(id, key) => GetResult(id, "value")
    case Put(id, key, value) => PutResult(id, false)
    case Delete(id, key) => DeleteResult(id, false)
    case CAS(id, key, current, newVal) => CASResult(id, false)
  }

  def serializeCommand(command: Operation) = command match {
    case Get(id, key) => s"get:$id:$key"
    case Put(id, key, value) => s"put:$id:$key:$value"
    case Delete(id, key) => s"delete:$id:$key"
    case CAS(id, key, current, newVal) => s"cas:$id:$key:$current:$newVal"
  }

  def serializeResult(result: OpResult) = ???

  def deserializeCommand(str: String): Operation = {
    val split = str.split(":")
    split(0) match {
      case "get" => Get(split(0), split(1))
      case "put" => Put(split(0), split(1), split(2))
      case "delete" => Delete(split(0), split(1))
      case "cas" => CAS(split(0), split(1), split(2), split(3))
    }
  }

  def deserializeResult(str: String): OpResult = ???
}
