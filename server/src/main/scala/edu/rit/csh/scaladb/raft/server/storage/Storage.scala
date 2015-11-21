package edu.rit.csh.scaladb.raft.server.storage

/**
 * Abstract class for the storage system for Raft. Currently this works as a Key-Value
 * store with generic types. All operations need to be thread-safe to allow for concurrent
 * operations.
 * @tparam Key the type of the Key
 * @tparam Value the type of the Value
 */
abstract class Storage[Key, Value] {

  /**
   * Puts a value into the file store
   */
  def put(key: Key, value: Value): Unit

  /**
   * Gets a value from the storage system, returning an Option
   */
  def get(key: Key): Option[Value]

  /**
   * Deletes a key from the storage system, returning a boolean to say if the element
   * was actually deleted or if it never actually existed
   */
  def delete(key: Key): Boolean

  /**
   * Generates a snapshot for the storage system. This is then sent to other servers
   * to bring them up to speed.
   */
  def generateSnapshot(): Array[Byte]

  /**
   * Installs the given snapshot, possibly overriding anything currently in the system.
   * This allows for a new server to come up to speed a lot faster.
   */
  def installSnapshot(arr: Array[Byte]): Boolean
}
