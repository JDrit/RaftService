package edu.rit.csh.scaladb.serialization

import java.io.InputStream

import scala.annotation.tailrec

/**
 * The input format for the serialization process
 * @tparam E the type of the item that is returned
 */
abstract class Input[E] extends InputStream {

  def next(): E

  def nextArray(arr: Array[E]): Unit = tailRecItr(arr, 0)

  @tailrec
  private def tailRecItr(arr: Array[E], idx: Int) {
    if (idx >= arr.length)
      return
    arr(idx) = next()
    tailRecItr(arr, idx + 1)
  }
}
