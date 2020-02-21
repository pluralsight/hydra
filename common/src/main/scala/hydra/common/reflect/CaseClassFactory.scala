package hydra.common.reflect

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

  val classMirror: ClassMirror = classLoaderMirror.reflectClass(classSymbol)

  val constructorSymbol: Symbol = tpe.decl(termNames.CONSTRUCTOR)

  val defaultConstructor: MethodSymbol =
    if (constructorSymbol.isMethod) {
      constructorSymbol.asMethod
    } else {
      val ctors = constructorSymbol.asTerm.alternatives
      ctors
        .map {
          _.asMethod
        }
        .find {
          _.isPrimaryConstructor
        }
        .get
    }

  val constructorMethod = classMirror.reflectConstructor(defaultConstructor)

  lazy val contructorTypes: Map[String, TypeTag[_]] = Map(
    defaultConstructor.paramLists
      .reduceLeft(_ ++ _)
      .map(sym =>
        sym.name.toString -> tagForType(
          tpe.member(sym.name).asMethod.returnType
        )
      ): _*
  )

  def tagForType(tpe: Type): TypeTag[_] = TypeTag(
    classLoaderMirror,
    new TypeCreator {

      def apply[U <: Universe with Singleton](m: Mirror[U]) =
        tpe.asInstanceOf[U#Type]
    }
  )

  /**
    * Attempts to create a new instance of the specified type by calling the
    * constructor method with the supplied arguments.
    *
    * @param args the arguments to supply to the constructor method
    */
  def buildWith(args: Seq[_]): T = {
    require(
      contructorTypes.size == args.size,
      s"Class [$cls] has ${contructorTypes.size} parameters; arguments supplied have ${args.size}."
    )
    constructorMethod(args: _*).asInstanceOf[T]
  }
}
