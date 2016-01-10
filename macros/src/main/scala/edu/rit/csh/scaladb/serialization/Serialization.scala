package edu.rit.csh.scaladb.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

object Serializer {

  def impl[T: c.WeakTypeTag](c: blackbox.Context): c.Tree = {
    import c.universe._
    val tpe = weakTypeOf[T]

    val fields = tpe.decls.collect {
      case sym: MethodSymbol if sym.isGetter => sym
    }.toList


    def write(fields: List[MethodSymbol]): c.Tree = fields match {
      case Nil => q"Unit"
      case f :: fs =>
        q"""
           Serializer.write(elem.${f.name}, buffer)
           ${write(fs)}
         """
    }

    q"""new Serializer[$tpe] {

          override def read(buffer: ByteArrayInputStream): $tpe = {
            new $tpe(..${fields.map(field => q"""Serializer.read[$field](buffer)""")})
          }

          override def write(elem: $tpe, buffer: ByteArrayOutputStream): Unit = {
            ${write(fields)}
          }
       }
     """
  }

  def materializeSerializer[T]: Serializer[T] = macro impl[T]

  def write[S](elem: S, buf: ByteArrayOutputStream)(implicit ser: Serializer[S]): Unit = ser.write(elem, buf)

  def read[S](buff: ByteArrayInputStream)(implicit ser: Serializer[S]): S = ser.read(buff)


}

abstract class Serializer[T] {

  final def read(buffer: ByteBuffer): T = {
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes, 0, bytes.length)
    read(new ByteArrayInputStream(bytes))
  }

  def read(buffer: ByteArrayInputStream): T

  def write(elem: T, buffer: ByteArrayOutputStream): Unit
}