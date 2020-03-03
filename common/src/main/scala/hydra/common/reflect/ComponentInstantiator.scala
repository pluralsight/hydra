package hydra.common.reflect

import java.lang.reflect.Method

import scala.util.Try

/**
  * Tries to instantiate an object in the following order:
  *
  * 1. Looks for a companion object with a given method name (default "apply") that takes a single Config arg
  * 2. If that fails, tries to instantiate it by calling its empty constructor.
  *
  * We need to add support for List[Any]
  */
object ComponentInstantiator {

  def instantiate[T](
      clazz: Class[T],
      params: List[AnyRef],
      companionMethodName: String = "apply"
  ): Try[T] = {
    Try(
      companion(clazz, companionMethodName, params.map(_.getClass))
        .map { c => c._2.invoke(c._1, params: _*) }
        .getOrElse(ReflectionUtils.instantiateClass(clazz))
        .asInstanceOf[T]
    )
  }

  /**
    * Looks for a companion object that has a method with the given name with a single argument of type Config.
    *
    * @param clazz The class to look for the companion object
    * @return
    */
  private def companion[T](
      clazz: Class[T],
      methodName: String,
      parameterTypes: List[Class[_]]
  ): Option[(T, Method)] = {
    try {
      val companion = ReflectionUtils.companionOf(clazz)
      companion.getClass.getMethods.toList.filter(m =>
        m.getName == methodName
          && areParamsAssignable(m.getParameterTypes.toList, parameterTypes)
      ) match {
        case Nil           => None
        case method :: Nil => Some(companion, method)
        case _             => None
      }
    } catch {
      case _: ClassNotFoundException => None
    }
  }

  private def areParamsAssignable(
      methodParamTypes: List[Class[_]],
      argParamTypes: List[Class[_]]
  ): Boolean = {
    methodParamTypes
      .zip(argParamTypes)
      .filter(x => x._1.isAssignableFrom(x._2))
      .size > 0
  }
}
