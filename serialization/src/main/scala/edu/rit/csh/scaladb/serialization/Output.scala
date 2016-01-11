package edu.rit.csh.scaladb.serialization

import java.io.OutputStream

import scala.annotation.tailrec

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






