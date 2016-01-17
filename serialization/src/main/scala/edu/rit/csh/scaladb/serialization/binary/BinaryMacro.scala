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
      case f :: fs => q"implicitly[BinarySerializer[$f]].write(elem.$f, output) ; ${write(fs)}"
      case Nil => q"Unit"
    }


    def serName(field: MethodSymbol): TermName = TermName(field.name + "Ser")

    def isDefined(fields: List[MethodSymbol]): c.Tree = fields match {
      case f :: fs => q"""${serName(f)}.isDefined && ${isDefined(fs)}"""
      case Nil => q"true"
    }

    def sum(fields: List[MethodSymbol]): c.Tree = fields match {
      case f :: fs => q"""${serName(f)}.get + ${sum(fs)}"""
      case Nil => q"0"
    }

    def size(decFields: List[MethodSymbol]): c.Tree = decFields match {
      case Nil =>
        q"""if (${isDefined(fields)}) {
            Some(${sum(fields)})
           } else {
            None
           }
         """
      case f :: fs =>
        q"""val ${serName(f)} = implicitly[BinarySerializer[$f]].staticSize
          ${size(fs)}"""
    }

    def canGetSize(fields: List[MethodSymbol]): c.Tree = fields match {
      case f :: fs => q"implicitly[BinarySerializer[$f]].canGetSize && ${canGetSize(fs)}"
      case Nil => q"true"
    }

    def getSize(fields: List[MethodSymbol]): c.Tree = fields match {
      case f :: fs => q"implicitly[BinarySerializer[$f]].getSize(elem.$f) + ${getSize(fs)}"
      case Nil => q"0"
    }

    q"""new edu.rit.csh.scaladb.serialization.binary.BinarySerializer[$tpe] {

          import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._
          import edu.rit.csh.scaladb.serialization.binary.BinaryMacro._
          import edu.rit.csh.scaladb.serialization.binary.{BinarySerializer, ByteArrayInput, BinaryOutput, ByteArrayOutput}

          override val staticSize: Option[Int] = {
            ${size(fields)}
          }

          override def canGetSize = ${canGetSize(fields)}

          def getSize(elem: $tpe): Int = ${getSize(fields)}

          override def read(buffer: ByteArrayInput): $tpe = {
            new $tpe(..${fields.map(field => q"implicitly[BinarySerializer[$field]].read(buffer)")})
          }

          override def write(elem: $tpe, output: BinaryOutput): Unit = {
            ${write(fields)}
          }
       }
     """
  }

  /**
   * Macro for generating Serializer for case classes at compile-time
   * @tparam T the type to generate a serializer for
   */
  implicit def materializeSerializer[T]: BinarySerializer[T] = macro impl[T]

}
