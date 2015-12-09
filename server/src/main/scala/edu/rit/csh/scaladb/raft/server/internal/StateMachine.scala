package edu.rit.csh.scaladb.raft.server.internal

import com.typesafe.scalalogging.LazyLogging
import edu.rit.csh.scaladb.raft.server.internal.StateMachine.CommandResult

import scala.collection.mutable

object StateMachine {
  type CommandResult = Either[Int, Result]
}

/**
 * Abstract state machine that runs the commands in the Raft algorithm
 */
abstract class StateMachine extends LazyLogging {

  /**
   * The state machine keeps track of the highest identifier seen by each
   * client. This makes sure that it does not repeat an already seen command.
   * The starting id should be zero and should increment from there, though they do not
   * have to be consecutive increases.
   */
  private final val seen = mutable.Map.empty[String, Int]
  
  /**
   * This is called with the command that needs to be run on the state machine. It needs to return
   * the result of the command. The commands given are guaranteed to be linearizable and that
   * there are no repeats
   * @param command the command to be run on the system
   * @return the result of the command
   */
  def compute(command: Command): Result


  /**
   * The state machine needs to define the message serializer for the type of
   * commands that it uses
   */
  val parser: MessageSerializer[Command]

  /**
   * Called by the raft server to process a new command, returns the result of the
   * command or the highest seen command ID seen so far. This does not allow for the
   * same command to be run more than once
   * @param command the command to run
   * @return the result of the command if successful or the highest identifer for
   *         this client seen so far
   */
  private[internal] final def process(command: Command): CommandResult = {
    val id = seen.getOrElseUpdate(command.client, -1)
    if (id >= command.id) {
      Left(id)
    } else {
      logger.debug(s"processing command $command")
      seen.put(command.client, command.id)
      Right(compute(command))
    }
  }
}
