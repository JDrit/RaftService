package edu.rit.csh.scaladb.raft.server.storage

class MemStorage extends Storage[String, String] {

  override def get(key: String): Option[String] = ???

  override def put(key: String, value: String): Unit = ???

  override def delete(key: String): Boolean = ???

  override def generateSnapshot(): Array[Byte] = ???

  override def installSnapshot(arr: Array[Byte]): Boolean = ???
}
