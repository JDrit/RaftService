package edu.rit.csh.scaladb.raft.util

import java.net.InetSocketAddress

import com.typesafe.scalalogging.LazyLogging
import edu.rit.csh.scaladb.raft.Peer

import scala.annotation.tailrec
import scala.reflect.ClassTag

/**
 * Trait used to parse different datatypes. Each type (T) that wants to be parsed must have
 * an implicit Parser[T] in scope for it to be parsed. Having these as implicit values allows
 * for nested parsing
 * @tparam T
 */
trait Parser[T] {
  def apply(input: String): T
}

object ParserUtils extends LazyLogging {

  def parse[T](str: String)(implicit parser: Parser[T]): T = parser(str)

  implicit val strParser = new Parser[String] {
    def apply(str: String) = str
  }

  implicit val intParser = new Parser[Int] {
    def apply(str: String) = str.toInt
  }

  implicit val doubleParser = new Parser[Double] {
    def apply(str: String) = str.toDouble
  }

  implicit def lstParser[T](implicit parser: Parser[T]) = new Parser[List[T]] {
    def apply(str: String) = str.split(",").map(parser.apply).toList
  }

  implicit def arrParser[T: ClassTag](implicit parser: Parser[T]) = new Parser[Array[T]] {
    def apply(str: String) = str.split(",").map(parser.apply).toArray[T]
  }

  implicit def mapParser[K, V](implicit kParser: Parser[K], vParser: Parser[V]) =
    new Parser[Map[K, V]] {
      def apply(str: String) = str.split(",").map { pair =>
        val split = pair.split("-")
        (kParser(split(0)), vParser(split(0)))
      }.toMap
    }

  implicit def inetSocketAddressParse(implicit sParser: Parser[String], iParser: Parser[Int]) =
    new Parser[InetSocketAddress] {
      def apply(str: String): InetSocketAddress = {
        val splits = str.split(":")
        new InetSocketAddress(sParser(splits(0)), iParser(splits(1)))
      }
    }

  implicit def tuple2Parse[T1, T2](implicit parser1: Parser[T1], parser2: Parser[T2]) =
    new Parser[(T1, T2)] {
      def apply(str: String): (T1, T2) = {
        val splits = str.split("-")
        (parser1(splits(0)), parser2(splits(1)))
      }
    }

  type ArgMap = Map[String, String]

  @tailrec
  private def nextArg(flags: Set[String], input: List[String], accum: ArgMap): Option[ArgMap] =
    if (flags.isEmpty) Some(accum)
    else input match {
      case x :: y :: xs => flags.collectFirst {
        case flag if "--" + flag == x || "-" + flag == x => flag
      } match {
        case Some(flag) => nextArg(flags - flag, xs, accum + (flag -> y))
        case None => logger.warn(s"unknown flag: $x") ; None
      }
      case _ =>
        logger.warn(s"flags missing: $flags")
        None
    }

  /**
   * Parses out the flags that are given, This only returns a Some if and only if the only
   * flags found are the ones given as input
   * @param flags the flags to parse out
   * @param args the arguments from the command line
   * @return an option of the parsed flags
   */
  def parseArgs(flags: Set[String], args: Array[String]): Option[ArgMap] =
    nextArg(flags, args.flatMap(_.split("=")).toList, Map.empty)
}
