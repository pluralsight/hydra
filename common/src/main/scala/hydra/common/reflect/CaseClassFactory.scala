package hydra.common.reflect

import scala.collection.immutable.ListMap
import scala.concurrent.duration.Duration
import scala.reflect.api.{Mirror, TypeCreator, Universe}
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{currentMirror => cm, universe => ru}


/**
  * Created by alexsilva on 6/2/16.
  */
class CaseClassFactory[T](cls: Class[T]) {

  val cl = cm.classSymbol(cls)
  val tpe = cl.toType
  val classSymbol = tpe.typeSymbol.asClass

  protected val classLoaderMirror = runtimeMirror(getClass.getClassLoader)

  require(
    tpe <:< typeOf[Product] && classSymbol.isCaseClass,
    s"CaseClassFactory only applies to case classes![$tpe],${tpe <:< typeOf[Product]},${classSymbol.isCaseClass}"
  )

  val classMirror = classLoaderMirror reflectClass classSymbol

  val constructorSymbol = tpe.decl(termNames.CONSTRUCTOR)

  val defaultConstructor =
    if (constructorSymbol.isMethod) constructorSymbol.asMethod
    else {
      val ctors = constructorSymbol.asTerm.alternatives
      ctors.map {
        _.asMethod
      }.find {
        _.isPrimaryConstructor
      }.get
    }

  val constructorMethod = classMirror reflectConstructor defaultConstructor

  val properties: ListMap[String, TypeTag[_]] = {
    val constructorSymbol = tpe.decl(termNames.CONSTRUCTOR)
    val defaultConstructor =
      if (constructorSymbol.isMethod) constructorSymbol.asMethod
      else {
        val ctors = constructorSymbol.asTerm.alternatives
        ctors.map {
          _.asMethod
        }.find {
          _.isPrimaryConstructor
        }.get
      }

    ListMap[String, TypeTag[_]]() ++ defaultConstructor.paramLists.reduceLeft(_ ++ _).map {
      sym => sym.name.toString -> tagForType(tpe.member(sym.name).asMethod.returnType)
    }

  }

  def tagForType(tpe: Type): TypeTag[_] = TypeTag(
    classLoaderMirror,
    new TypeCreator {
      def apply[U <: Universe with Singleton](m: Mirror[U]) = tpe.asInstanceOf[U#Type]
    }
  )

  /**
    * Attempts to create a new instance of the specified type by calling the
    * constructor method with the supplied arguments.
    *
    * @param args the arguments to supply to the constructor method
    */
  def buildWith(args: Seq[_]): T = {
    val nargs = fixTypes(args)
    constructorMethod(nargs: _*).asInstanceOf[T]
  }

  private def fixTypes(args: Seq[Any]) = {
    (properties zip args) map {
      case (property, arg) => {
        val name = property._2.tpe.typeSymbol.asClass.name.decodedName.toString
        name match {
          case "Duration" => Duration.create(arg.toString)
          case x => arg
        }
      }
    }
  }.toSeq

}

