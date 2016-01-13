package edu.rit.csh.scaladb.serialization

import java.io.InputStream

import scala.annotation.tailrec

/**
 * The input format for the serialization process
 * @tparam E the type of the item that is returned
 */
abstract class Input[E] extends InputStream
