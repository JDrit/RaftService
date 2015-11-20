package edu.rit.csh.scaladb.raft

import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
import scala.reflect.ClassTag

trait Parser[T] {
  def apply(input: String): T
}

object ParserUtils extends LazyLogging {

  def parse[T](str: String)(implicit parser: Parser[T]): T = parser(str)

  implicit def strParser = new Parser[String] {
    def apply(str: String) = str
  }

  implicit def intParser = new Parser[Int] {
    def apply(str: String) = str.toInt
  }

  implicit def doubleParser = new Parser[Double] {
    def apply(str: String) = str.toDouble
  }

  implicit def lstParser[T](implicit parser: Parser[T]) = new Parser[List[T]] {
    def apply(str: String) = str.split(",").map(parser.apply).toList
  }

  implicit def arrParser[T: ClassTag](implicit parser: Parser[T]) = new Parser[Array[T]] {
    def apply(str: String) = str.split(",").map(parser.apply).toArray[T]
  }

  implicit def mapParser[K, V](implicit kParser: Parser[K], vParser: Parser[V]) = new Parser[Map[K, V]] {
    def apply(str: String) = str.split(",").map { pair =>
      val split = pair.split("-")
      (kParser(split(0)), vParser(split(0)))
    }.toMap
  }

  type ArgMap = Map[String, String]

  @tailrec
  private def nextArg(flags: Set[String], input: List[String], accum: ArgMap): Option[ArgMap] =
    if (flags.isEmpty) Some(accum)
    else input match {
      case x :: y :: xs =>
        flags.collectFirst {
          case flag if "--" + flag == x || "-" + flag == x => flag
        } match {
          case Some(flag) =>
            nextArg(flags - flag, xs, accum + (flag -> y))
          case None => logger.warn(s"unknown flag: $x") ; None
        }
      case _ =>
        logger.warn(s"flags missing: $flags")
        None
    }

  def parseArgs(flags: Set[String], args: Array[String]): Option[ArgMap] =
    nextArg(flags, args.flatMap(_.split("=")).toList, Map.empty)
}
