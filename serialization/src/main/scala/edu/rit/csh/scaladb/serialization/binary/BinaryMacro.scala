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
        q"""
           buffer.serialize(elem.${f.name})
           ${write(fs)}
         """
    }

    q"""new edu.rit.csh.scaladb.serialization.binary.BinarySerializer[$tpe] {

          import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._
          import edu.rit.csh.scaladb.serialization.binary.{ByteArrayInput, ByteArrayOutput}

          override def read(buffer: ByteArrayInput): $tpe = {
            new $tpe(..${fields.map(field => q"""buffer.deserialize[$field]""")})
          }

          override def write(elem: $tpe, buffer: ByteArrayOutput): Unit = {
            ${write(fields)}
          }
       }
     """
  }

  def arrayImpl[T: c.WeakTypeTag](c: blackbox.Context)(ser: c.Tree): c.Tree = {
    import c.universe._
    val tpe = weakTypeOf[T]
    val q"(..$args) => $body" = c.enclosingMethod
    println(args)
    println(body)

    q"""new edu.rit.csh.scaladb.serialization.binary.BinarySerializer[Array[$tpe]] {

          import edu.rit.csh.scaladb.serialization.binary.DefaultBinarySerializers._
          import edu.rit.csh.scaladb.serialization.binary.{ByteArrayInput, ByteArrayOutput}

          override def read(buffer: ByteArrayInput): Array[$tpe] = {
            val len = buffer.deserialize[Int]
            val arr = new Array[$tpe](len)
            var i = 0
            while (i < len) {
              arr(i) = $ser.read(buffer)
              i+= 1
            }
            arr
          }

          override def write(elem: Array[$tpe], buffer: ByteArrayOutput): Unit = {
            buffer.serialize[Int](elem.length)
            var i = 0
            while (i < elem.length) {
              $ser.write(elem(i), buffer)
              i += 1
            }
          }
       }
     """
  }

  implicit def materializeSerializer[T]: BinarySerializer[T] = macro impl[T]

  def materializeArraySerializer[T](implicit ser: BinarySerializer[T]): BinarySerializer[Array[T]] = macro arrayImpl[T]
}
