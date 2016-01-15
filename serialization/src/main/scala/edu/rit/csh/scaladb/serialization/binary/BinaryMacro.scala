package edu.rit.csh.scaladb.serialization.binary

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
 * Generates BinarySerializers for case classes. This will have compile time failures if
 * tried to be used on other type of classes
 */
object BinaryMacro {
  def impl[T: c.WeakTypeTag](c: blackbox.Context): c.Tree = {
    import c.universe._
    val tpe = weakTypeOf[T]
    val fields = tpe.decls.collect {
      case sym: MethodSymbol if sym.isGetter => sym
    }.toList

    def write(fields: List[MethodSymbol]): c.Tree = fields match {
      case Nil => q"Unit"
      case f :: fs =>
        q"""buffer.serialize(elem.$f)
           ${write(fs)}"""
    }

    def newWrite(fields: List[MethodSymbol]): c.Tree = fields match {
      case Nil => q"newOffset"
      case f :: fs =>
        q"""newOffset = implicitly[BinarySerializer[$f]].write(elem.$f, newOffset, buffer)
            ${newWrite(fs)}"""
    }

    def size(fields: List[MethodSymbol]): c.Tree = fields match {
      case Nil => q"0"
      case f :: fs => q"implicitly[BinarySerializer[$f]].size(elem.${f.name}) + ${size(fs)}"
    }

    q"""new edu.rit.csh.scaladb.serialization.binary.DynamicSerializer[$tpe] {

          import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._
          import edu.rit.csh.scaladb.serialization.binary.{BinarySerializer, ByteArrayInput, ByteArrayOutput}

          def size(elem: $tpe) = ${size(fields)}

          override def read(buffer: ByteArrayInput): $tpe = {
            new $tpe(..${fields.map(field => q"""buffer.deserialize[$field]""")})
          }

          override def write(elem: $tpe, buffer: ByteArrayOutput): Unit = {
            ${write(fields)}
          }

          override def write(elem: $tpe, offset: Int, buffer: Array[Byte]): Int = {
            var newOffset = offset
            ${newWrite(fields)}
          }
       }
     """
  }

  implicit def materializeSerializer[T]: BinarySerializer[T] = macro impl[T]

}
