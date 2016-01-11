package edu.rit.csh.scaladb.serialization

import java.io.OutputStream

import scala.annotation.tailrec

/**
 * The output of the serialization process.
 * @tparam T the type of the final product
 * @tparam E the type of the item being incrementally added
 */
abstract class Output[T, E] extends OutputStream {
  def output: T

  def add(e: E): Unit

  def addArray(arr: Array[E]): Unit = tailRecItr(arr, 0)

  @tailrec
  private def tailRecItr(arr: Array[E], idx: Int) {
    if (idx >= arr.length)
      return
    add(arr(idx))
    tailRecItr(arr, idx + 1)
  }
}






